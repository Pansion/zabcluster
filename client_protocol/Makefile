CFLAGS 		= -std=c99 -g -O2 -Wall -Dlinux
LDFLAGS		= libredisprotocol.a -lrt
LIB			= libredisprotocol.a

SRCS		= sds.c redis_protocol.c
# Do not modify the following
OBJS		= $(subst .c,.o,$(SRCS))
CPPFLAGS	= -I.
DEPS 		= $(patsubst %.c,.deps/%.d,$(SRCS))
SNAME		:= $(shell uname)

PROGRAMS	=test_redis_protocol
all:$(OBJS) $(LIB) $(PROGRAMS)

%.o: %.c
	gcc $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

$(LIB): $(OBJS)
	ar rcs $(LIB) $(OBJS)
	
test_redis_protocol:test_redis_protocol.c
	gcc $(CFLAGS) $< -o $@ $(CPPFLAGS) $(LDFLAGS)

clean:
	rm -f *.o
	rm -f *.a
	rm -f $(PROGRAMS)