#
# requires pcre and pcre-devel RPMs on development systems
# requires pcre RPMs on deployed systems
#

LEV_DIR=/opt/open-source/libevent-2.0.15-stable
CFLAGS 		= -g -O2 -Wall -Dlinux
LDFLAGS		= ../base/libbase.a $(LEV_DIR)/build/.libs/libevent.a -lrt -llog4cplus


SRCS		= event_cpp.cxx
# Do not modify the following
OBJS		= $(subst .cxx,.o,$(SRCS))
CPPFLAGS	= -I. -I../ -I$(LEV_DIR)/include -I/usr/local/include/
DEPS 		= $(patsubst %.c,.deps/%.d,$(SRCS))
SNAME		:= $(shell uname)

PROGRAMS	=test_event
all:$(OBJS) $(LIB) $(PROGRAMS)

%.o: %.cxx
	g++ $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

test_event:test_event.cxx
	g++ $(CFLAGS) $(OBJS) $< -o $@ $(CPPFLAGS) $(LDFLAGS)

clean:
	rm -f *.o
	rm -f $(PROGRAMS)
