#
# requires libevent 2.0
# requires log4cplus 1.0
#
include ../../Makefile.inc

CFLAGS 		= -g -O2 -Wall -Dlinux
LDFLAGS		=  ../../zab/libzab.a ../../base/libbase.a ../../client_protocol/libredisprotocol.a \
			$(LEV_DIR)/.libs/libevent.a $(LOGCPP_DIR)/src/.libs/liblog4cplus.a -lrt 


SRCS		= test_gthread.cxx
# Do not modify the following
OBJS		= $(subst .cxx,.o,$(SRCS))
CPPFLAGS	= -I. -I../../ -I../../zab -I../../client_protocol -I$(LEV_DIR)/include -I$(LOGCPP_DIR)/include
DEPS 		= $(patsubst %.c,.deps/%.d,$(SRCS))
SNAME		:= $(shell uname)

PROGRAMS	=test_peer_config test_el_listener test_zab test_client_db
all:$(PROGRAMS)

test_peer_config:test_peer_config.cc
	g++ $(CFLAGS) $< -o $@ $(CPPFLAGS) $(LDFLAGS)

test_el_listener:test_el_listener.cc
	g++ $(CFLAGS) $< -o $@ $(CPPFLAGS) $(LDFLAGS)

test_zab:test_zab.cc
	g++ $(CFLAGS) $< -o $@ $(CPPFLAGS) $(LDFLAGS)
	
test_client_db:test_client_db.cc
	g++ $(CFLAGS) $< -o $@ $(CPPFLAGS) $(LDFLAGS)
	
clean:
	rm -f *.o
	rm -f $(PROGRAMS)
