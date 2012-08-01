include ../Makefile.inc


CFLAGS 		= -g -O2 -Wall -Dlinux
LIB		= libbase.a

SRCS		= byte_buffer.cc condition_variable_posix.cc lock_impl_posix.cc \
			  lock.cc platform_thread_posix.cc thread.cc logging_impl.cc \
			  waitable_event_posix.cc
# Do not modify the following
OBJS		= $(subst .cc,.o,$(SRCS))
CPPFLAGS	= -I. -I$(LOGCPP_DIR)/include
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
