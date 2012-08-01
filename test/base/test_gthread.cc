/*
 * test_thread.cxx
 *
 *  Created on: Jun 14, 2012
 *      Author: pchen
 */

#include "base/thread.h"
#include "base/logging.h"

class TestThread: public Thread {
  public:
    TestThread()
        : Thread("MyTest") {
    }
    ;
  protected:
    virtual void Init() {
      sleep(2);
      INFO("Init ready, singal main thread to started up");
    }
    virtual void Run() {
      INFO("I am testing thread, started");
      while (!stopping_) {
        DEBUG("I am test thread,just ping");
        sleep(2);
      }
    }
    ;
};

int main(int argc, char **argv) {

  TestThread myTest;
  INFO("set up new thread, wait for it to start...");
  myTest.Start();
  INFO("new thread "<<myTest.thread_id()<<":"<<myTest.thread_name());
  sleep(10);
  myTest.Stop();
  return 0;
}

