include ../Makefile.inc

CFLAGS 		= -g -O2 -Wall -Dlinux
LIB			= libzab.a 

SRCS		= 	peer_config.cc \
				election_cnxmgr.cc \
				election_strategy.cc \
				quorum_peer.cc  \
				leader.cc \
				follower.cc \
				quorum_server.cc \
				client_cnx_mgr.cc \
				client_handler_redis.cc \
				zabdb_redis.cc
				
# Do not modify the following
OBJS		= $(subst .cc,.o,$(SRCS))
CPPFLAGS	= -I. -I../ -I$(LEV_DIR)/include -I$(LOGCPP_DIR)/include
DEPS 		= $(patsubst %.c,.deps/%.d,$(SRCS))
SNAME		:= $(shell uname)

all:$(OBJS) $(LIB)

%.o: %.cc
	g++ $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

$(LIB): $(OBJS)
	ar rcs $(LIB) $(OBJS)
	
clean:
	rm -f *.o
	rm -f *.a
