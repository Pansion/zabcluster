//Copyright (c) 2012, Pansion Chen <pansion dot zabcpp at gmail dot com>
//All rights reserved.
//
//Redistribution and use in source and binary forms, with or without
//modification, are permitted provided that the following conditions are met:
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//    * Neither the name of the zabcpp nor the
//      names of its contributors may be used to endorse or promote products
//      derived from this software without specific prior written permission.
//
//THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
//ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY
//DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include <limits.h>
#include "redis_protocol.h"
#include "sds.h"

#define REDIS_ERR    -1
#define REDIS_OK     0
#define REDIS_INLINE_MAX_SIZE   (1024*64) /* Max size of inline reads */

static int intlen(int i);
static size_t bulklen(size_t len);
static int redisvFormatCommand(char **target, const char *format, va_list ap);
int processMultibulkBuffer(redisClient *c);



int raw_redisCommand(char ** target, const char * format, ...) {
  va_list ap;
  int out_len = 0;
  va_start(ap, format);
  out_len = redisvFormatCommand(target, format, ap);
  va_end(ap);
  return out_len;
}

/* Calculate the number of bytes needed to represent an integer as string. */
int intlen(int i) {
  int len = 0;
  if (i < 0) {
    len++;
    i = -i;
  }
  do {
    len++;
    i /= 10;
  } while (i);
  return len;
}

/* Helper that calculates the bulk length given a certain string length. */
size_t bulklen(size_t len) {
  return 1 + intlen(len) + 2 + len + 2;
}

int redisvFormatCommand(char **target, const char *format, va_list ap) {
  const char *c = format;
  char *cmd = NULL; /* final command */
  int pos; /* position in final command */
  sds curarg, newarg; /* current argument */
  int touched = 0; /* was the current argument touched? */
  char **curargv = NULL, **newargv = NULL;
  int argc = 0;
  int totlen = 0;
  int j;

  /* Abort if there is not target to set */
  if (target == NULL )
    return -1;

  /* Build the command string accordingly to protocol */
  curarg = sdsempty();
  if (curarg == NULL )
    return -1;

  while (*c != '\0') {
    if (*c != '%' || c[1] == '\0') {
      if (*c == ' ') {
        if (touched) {
          newargv = realloc(curargv, sizeof(char*) * (argc + 1));
          if (newargv == NULL )
            goto err;
          curargv = newargv;
          curargv[argc++] = curarg;
          totlen += bulklen(sdslen(curarg));

          /* curarg is put in argv so it can be overwritten. */
          curarg = sdsempty();
          if (curarg == NULL )
            goto err;
          touched = 0;
        }
      } else {
        newarg = sdscatlen(curarg, c, 1);
        if (newarg == NULL )
          goto err;
        curarg = newarg;
        touched = 1;
      }
    } else {
      char *arg;
      size_t size;

      /* Set newarg so it can be checked even if it is not touched. */
      newarg = curarg;

      switch (c[1]) {
        case 's':
          arg = va_arg(ap,char*);
          size = strlen(arg);
          if (size > 0)
            newarg = sdscatlen(curarg, arg, size);
          break;
        case 'b':
          arg = va_arg(ap,char*);
          size = va_arg(ap,size_t);
          if (size > 0)
            newarg = sdscatlen(curarg, arg, size);
          break;
        case '%':
          newarg = sdscat(curarg, "%");
          break;
        default:
          /* Try to detect printf format */
        {
          static const char intfmts[] = "diouxX";
          char _format[16];
          const char *_p = c + 1;
          size_t _l = 0;
          va_list _cpy;

          /* Flags */
          if (*_p != '\0' && *_p == '#')
            _p++;
          if (*_p != '\0' && *_p == '0')
            _p++;
          if (*_p != '\0' && *_p == '-')
            _p++;
          if (*_p != '\0' && *_p == ' ')
            _p++;
          if (*_p != '\0' && *_p == '+')
            _p++;

          /* Field width */
          while (*_p != '\0' && isdigit(*_p))
            _p++;

          /* Precision */
          if (*_p == '.') {
            _p++;
            while (*_p != '\0' && isdigit(*_p))
              _p++;
          }

          /* Copy va_list before consuming with va_arg */
          va_copy(_cpy, ap);

          /* Integer conversion (without modifiers) */
          if (strchr(intfmts, *_p) != NULL ) {
            va_arg(ap, int);
            goto fmt_valid;
          }

          /* Double conversion (without modifiers) */
          if (strchr("eEfFgGaA", *_p) != NULL ) {
            va_arg(ap, double);
            goto fmt_valid;
          }

          /* Size: char */
          if (_p[0] == 'h' && _p[1] == 'h') {
            _p += 2;
            if (*_p != '\0' && strchr(intfmts, *_p) != NULL ) {
              va_arg(ap, int);
              /* char gets promoted to int */
              goto fmt_valid;
            }
            goto fmt_invalid;
          }

          /* Size: short */
          if (_p[0] == 'h') {
            _p += 1;
            if (*_p != '\0' && strchr(intfmts, *_p) != NULL ) {
              va_arg(ap, int);
              /* short gets promoted to int */
              goto fmt_valid;
            }
            goto fmt_invalid;
          }

          /* Size: long long */
          if (_p[0] == 'l' && _p[1] == 'l') {
            _p += 2;
            if (*_p != '\0' && strchr(intfmts, *_p) != NULL ) {
              va_arg(ap, long long);
              goto fmt_valid;
            }
            goto fmt_invalid;
          }

          /* Size: long */
          if (_p[0] == 'l') {
            _p += 1;
            if (*_p != '\0' && strchr(intfmts, *_p) != NULL ) {
              va_arg(ap, long);
              goto fmt_valid;
            }
            goto fmt_invalid;
          }

          fmt_invalid:
          va_end(_cpy);
          goto err;

          fmt_valid: _l = (_p + 1) - c;
          if (_l < sizeof(_format) - 2) {
            memcpy(_format, c, _l);
            _format[_l] = '\0';
            newarg = sdscatvprintf(curarg, _format, _cpy);

            /* Update current position (note: outer blocks
             * increment c twice so compensate here) */
            c = _p - 1;
          }

          va_end(_cpy);
          break;
        }
      }

      if (newarg == NULL )
        goto err;
      curarg = newarg;

      touched = 1;
      c++;
    }
    c++;
  }

  /* Add the last argument if needed */
  if (touched) {
    newargv = realloc(curargv, sizeof(char*) * (argc + 1));
    if (newargv == NULL )
      goto err;
    curargv = newargv;
    curargv[argc++] = curarg;
    totlen += bulklen(sdslen(curarg));
  } else {
    sdsfree(curarg);
  }

  /* Clear curarg because it was put in curargv or was free'd. */
  curarg = NULL;

  /* Add bytes needed to hold multi bulk count */
  totlen += 1 + intlen(argc) + 2;

  /* Build the command at protocol level */
  cmd = malloc(totlen + 1);
  if (cmd == NULL )
    goto err;

  pos = sprintf(cmd, "*%d\r\n", argc);
  for (j = 0; j < argc; j++) {
    pos += sprintf(cmd + pos, "$%zu\r\n", sdslen(curargv[j]));
    memcpy(cmd + pos, curargv[j], sdslen(curargv[j]));
    pos += sdslen(curargv[j]);
    sdsfree(curargv[j]);
    cmd[pos++] = '\r';
    cmd[pos++] = '\n';
  }
  assert(pos == totlen);
  cmd[pos] = '\0';

  free(curargv);
  *target = cmd;
  return totlen;

  err: while (argc--)
    sdsfree(curargv[argc]);
  free(curargv);

  if (curarg != NULL )
    sdsfree(curarg);

  /* No need to check cmd since it is the last statement that can fail,
   * but do it anyway to be as defensive as possible. */
  if (cmd != NULL )
    free(cmd);

  return -1;
}
/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int string2ll(char *s, size_t slen, long long *value) {
  char *p = s;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;

  if (plen == slen)
    return 0;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0') {
    if (value != NULL )
      *value = 0;
    return 1;
  }

  if (p[0] == '-') {
    negative = 1;
    p++;
    plen++;

    /* Abort on only a negative sign. */
    if (plen == slen)
      return 0;
  }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9') {
    v = p[0] - '0';
    p++;
    plen++;
  } else if (p[0] == '0' && slen == 1) {
    *value = 0;
    return 1;
  } else {
    return 0;
  }

  while (plen < slen && p[0] >= '0' && p[0] <= '9') {
    if (v > (ULLONG_MAX / 10)) /* Overflow. */
      return 0;
    v *= 10;

    if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
      return 0;
    v += p[0] - '0';

    p++;
    plen++;
  }

  /* Return if not all bytes were used. */
  if (plen < slen)
    return 0;

  if (negative) {
    if (v > ((unsigned long long) (-(LLONG_MIN + 1)) + 1)) /* Overflow. */
      return 0;
    if (value != NULL )
      *value = -v;
  } else {
    if (v > LLONG_MAX) /* Overflow. */
      return 0;
    if (value != NULL )
      *value = v;
  }
  return 1;
}

int processMultibulkBuffer(redisClient *c) {
  char *newline = NULL;
  int pos = 0, ok;
  long long ll;

  if (c->multibulklen == 0) {
    /* Multi bulk length cannot be read without a \r\n */
    newline = strchr(c->querybuf, '\r');
    if (newline == NULL ) {
      if (sdslen(c->querybuf) > REDIS_INLINE_MAX_SIZE) {
//                addReplyError(c,"Protocol error: too big mbulk count string");
//                setProtocolError(c,0);
        c->querybuf = sdsrange(c->querybuf, pos, -1);
      }
      return REDIS_ERR;
    }

    /* Buffer should also contain \n */
    if (newline - (c->querybuf) > ((signed) sdslen(c->querybuf) - 2))
      return REDIS_ERR;

    /* We know for sure there is a whole line since newline != NULL,
     * so go ahead and find out the multi bulk length. */
    ok = string2ll(c->querybuf + 1, newline - (c->querybuf + 1), &ll);
    if (!ok || ll > 1024 * 1024) {
//            addReplyError(c,"Protocol error: invalid multibulk length");
//            setProtocolError(c,pos);
      c->querybuf = sdsrange(c->querybuf, pos, -1);
      return REDIS_ERR;
    }

    pos = (newline - c->querybuf) + 2;
    if (ll <= 0) {
      c->querybuf = sdsrange(c->querybuf, pos, -1);
      return REDIS_OK;
    }

    c->multibulklen = ll;
  }

  while (c->multibulklen) {
    /* Read bulk length if unknown */
    if (c->bulklen == -1) {
      newline = strchr(c->querybuf + pos, '\r');
      if (newline == NULL ) {
        if (sdslen(c->querybuf) > REDIS_INLINE_MAX_SIZE) {
//          addReplyError(c, "Protocol error: too big bulk count string");
//          setProtocolError(c, 0);
          c->querybuf = sdsrange(c->querybuf, pos, -1);
        }
        break;
      }

      /* Buffer should also contain \n */
      if (newline - (c->querybuf) > ((signed) sdslen(c->querybuf) - 2))
        break;

      if (c->querybuf[pos] != '$') {
//        addReplyErrorFormat(c, "Protocol error: expected '$', got '%c'", c->querybuf[pos]);
//        setProtocolError(c, pos);
        c->querybuf = sdsrange(c->querybuf, pos, -1);
        return REDIS_ERR;
      }

      ok = string2ll(c->querybuf + pos + 1, newline - (c->querybuf + pos + 1), &ll);
      if (!ok || ll < 0 || ll > 512 * 1024 * 1024) {
//        addReplyError(c, "Protocol error: invalid bulk length");
//        setProtocolError(c, pos);
        c->querybuf = sdsrange(c->querybuf, pos, -1);
        return REDIS_ERR;
      }

      pos += newline - (c->querybuf + pos) + 2;
      c->bulklen = ll;
    }

    /* Read bulk argument */
    if (sdslen(c->querybuf) - pos < (unsigned) (c->bulklen + 2)) {
      /* Not enough data (+2 == trailing \r\n) */
      break;
    } else {
      if (sdslen(c->cmd) == 0){
        c->cmd = sdscatlen(c->cmd, c->querybuf + pos, c->bulklen);
      }
      pos += c->bulklen + 2;
      c->bulklen = -1;
      c->multibulklen--;
    }
  }

  //copy raw data
  c->rawreq = sdscatlen(c->rawreq, c->querybuf,  pos);
  /* Trim to pos */
  c->querybuf = sdsrange(c->querybuf, pos, -1);

  /* We're done when c->multibulk == 0 */
  if (c->multibulklen == 0)
    return REDIS_OK;

  /* Still not read to process the command */
  return REDIS_ERR;
}

int processInputBuffer(redisClient *c) {
  return processMultibulkBuffer(c);
}

redisClient * createClient(int fd) {
  redisClient * c = NULL;
  c = malloc(sizeof(redisClient));
  if (c != NULL) {
    c->bulklen = -1;
    c->cmd = sdsempty();
    c->fd = fd;
    c->multibulklen = 0;
    c->querybuf = sdsempty();
    c->rawreq = sdsempty();
  }
  return c;
}
void freeClient(redisClient *c) {
  sdsfree(c->querybuf);
  sdsfree(c->cmd);
  sdsfree(c->rawreq);
  free(c);
}

void resetClient(redisClient *c) {
  c->bulklen = -1;
  c->multibulklen = 0;
  c->cmd = sdsrange(c->cmd, sdslen(c->cmd), -1);
  c->rawreq = sdsrange(c->rawreq, sdslen(c->rawreq), -1);
}
