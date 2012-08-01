/*
 * test_redis_protocol.cxx
 *
 *  Created on: Jul 11, 2012
 *      Author: pchen
 */

#include <stdlib.h>
#include "redis_protocol.h"

int main(int argc, char **argv) {

  char * ret = NULL;
  int len = 0;
  len = raw_redisCommand(&ret, "SET nodeinfo test");
  printf("out put len %d, redis raw command %s\n", len, ret);
  struct redisClient *c  = createClient(1);
  int i =0;
  free(ret);
  ret = NULL;
  len = 0;
  for(i = 0; i<5; i++){
    len = raw_redisCommand(&ret, "SET nodeinfo test");
    c->querybuf = sdscatlen(c->querybuf, ret, len);
    free(ret);
    ret = NULL;
  }
  int r = 0;
  int n = 0;
  printf("raw data:%s",c->querybuf);
  while(sdslen(c->querybuf)){
    r = processInputBuffer(c);
    printf("processInputBuffer result=%d, cmd=%s:%d, rawreq=%s:%d\n",r, c->cmd, sdslen(c->cmd), c->rawreq, sdslen(c->rawreq));
    printf("remain data:%s:%d",c->querybuf,sdslen(c->querybuf));
    if (r ==0){
      resetClient(c);
      n++;
    }
  }
  printf("totol get %d cmd\n",n);
  freeClient(c);
  free(c);
  return 0;
}

