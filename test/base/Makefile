include ../../Makefile.inc

FLAGS 		= -g -O2 -Wall -Dlinux
LDFLAGS		= ../../base/libbase.a $(LOGCPP_DIR)/src/.libs/liblog4cplus.a -lrt


SRCS		= test_gthread.cc
# Do not modify the following
OBJS		= $(subst .cxx,.o,$(SRCS))
CPPFLAGS	= -I. -I$(LEV_DIR)/include -I$(LOGCPP_DIR)/include -I../../
DEPS 		= $(patsubst %.c,.deps/%.d,$(SRCS))
SNAME		:= $(shell uname)

PROGRAMS	=test_gthread test_log4cplus
all:$(PROGRAMS)

test_gthread:test_gthread.cc
	g++ $(CFLAGS) $< -o $@ $(CPPFLAGS) $(LDFLAGS)

test_log4cplus:test_log4cplus.cc
	g++ $(CFLAGS) $< -o $@ $(CPPFLAGS) $(LDFLAGS)
	
clean:
	rm -f *.o
	rm -f $(PROGRAMS)
