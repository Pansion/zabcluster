include ./Makefile.inc

ZABLIB_DIR	= base client_protocol zab
ZABLIBS 	= libbase.a libredisprotocol.a libzab.a
LDFLAGS		=  ./zab/libzab.a \
		   ./base/libbase.a \
		   ./client_protocol/libredisprotocol.a \
		   $(LEV_DIR)/.libs/libevent.a \
		   $(LOGCPP_DIR)/src/.libs/liblog4cplus.a \
		   -lrt
			
CFLAGS 		= -g -O2 -Wall -Dlinux
CPPFLAGS	= -I. -I$(LEV_DIR)/include -I$(LOGCPP_DIR)/include
SNAME		:= $(shell uname)

PROGRAMS	= zab_cluster

all:$(ZABLIBS) $(PROGRAMS)

zab_cluster:zab_cluster.cc
	g++ $(CFLAGS) $< -o $@ $(CPPFLAGS) $(LDFLAGS)
	
libbase.a:
	cd base; $(MAKE) all
	
libredisprotocol.a:
	cd client_protocol; $(MAKE) all
	
libzab.a:
	cd zab; $(MAKE) all

clean:
	rm -f *.o
	rm -f $(PROGRAMS)
	-for d in $(ZABLIB_DIR); do (cd $$d; $(MAKE) clean ); done
