# for test
include ../Makefile.inc

SUB_DIR = base zab
TESTS = test_base test_zab
all:$(TESTS)

test_base:
	cd base;$(MAKE) all

test_zab:
	cd zab;$(MAKE) all

clean:
	-for d in $(SUB_DIR); do (cd $$d; $(MAKE) clean ); done

