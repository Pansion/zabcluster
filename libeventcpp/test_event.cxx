/*
 * test_event.cxx
 *
 *  Created on: Jun 28, 2012
 *      Author: pchen
 */

#include "event_cpp.h"
#include "base/logging.h"
#include "event2/listener.h"

#include <arpa/inet.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string>
#include <netinet/tcp.h>
using namespace std;
const int port = 9876;
static SendWorker sw;

class SendThread: public Thread {
  public:
    SendThread()
        : Thread("Send Thread") {
    }
    ;

    void SetId(int64 sid) {
      myid = sid;
    }
  protected:
    virtual void Run() {
      string addr = "127.0.0.1";
      int sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
      if (sock < 0) {
        ERROR("Could not create socket");
        return;
      }
      int flag = 1;
      setsockopt(sock, /* socket affected */
      IPPROTO_TCP, /* set option at TCP level */
      TCP_NODELAY, /* name of option */
      (char *) &flag, /* the cast is historical cruft */
      sizeof(int));
      struct sockaddr_in peerAddr;
      memset(&peerAddr, 0, sizeof(peerAddr));
      peerAddr.sin_family = AF_INET; /* Internet address family */
      peerAddr.sin_addr.s_addr = inet_addr(addr.data()); /* Server IP address */
      peerAddr.sin_port = htons(port); /* Server port */
      if (connect(sock, (struct sockaddr *) &peerAddr, sizeof(peerAddr)) < 0) {
        ERROR("could not connect to addr "<<addr<<" port "<<port<<" errno "<<errno);
        return;
      }

      sw.AddConnection(sock, myid);
      while (!stopping_) {
        ByteBuffer* m = new ByteBuffer();
        m->WriteString("test messgae");
        sw.SendMsg(myid, m);
        sleep(2);
      }
    }
    ;
  private:
    int64 myid;
};

static void accept_conn_cb(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address, int socklen,
    void *ctx) {
  INFO("get new connection fd:"<<fd);
  RecvWorker* recvw = static_cast<RecvWorker*>(ctx);
  recvw->AddPeer(fd);
}

static void accept_error_cb(struct evconnlistener *listener, void *ctx) {
  struct event_base *base = evconnlistener_get_base(listener);
  int err = EVUTIL_SOCKET_ERROR();
  fprintf(stderr, "Got an error %d (%s) on the listener. "
      "Shutting down.\n", err, evutil_socket_error_to_string(err));

  event_base_loopexit(base, NULL);
}

static void timeout_cb(evutil_socket_t fd, short event, void *arg) {
  INFO("time out and begine to break loop");
  struct event_base * b = (struct event_base*) arg;
  event_base_loopexit(b, NULL);
}

int main(int argc, char **argv) {
  INFO("begin event testing");
  struct event_base *base;
  struct evconnlistener *listener;
  struct sockaddr_in sin;

  RecvWorker recvWorker;
  recvWorker.Start();
  base = event_base_new();
  if (!base) {
    puts("Couldn't open event base");
    return 1;
  }

  /* Clear the sockaddr before using it, in case there are extra
   * platform-specific fields that can mess us up. */
  memset(&sin, 0, sizeof(sin));
  /* This is an INET address */
  sin.sin_family = AF_INET;
  /* Listen on 0.0.0.0 */
  sin.sin_addr.s_addr = htonl(0);
  /* Listen on the given port. */
  sin.sin_port = htons(port);

  listener = evconnlistener_new_bind(base, accept_conn_cb, &recvWorker, LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1,
      (struct sockaddr*) &sin, sizeof(sin));
  if (!listener) {
    perror("Couldn't create listener");
    return 1;
  }
  evconnlistener_set_error_cb(listener, accept_error_cb);

  sw.Start();
  SendThread sender[5];
  for (int i = 0; i < 5; i++) {
    sender[i].SetId(i + 1);
    sender[i].Start();
  }
  struct event * ev = evtimer_new(base, timeout_cb, base);

  struct timeval timeout = { 20, 0 };
  evtimer_add(ev, &timeout);
  event_base_dispatch(base);
  event_free(ev);
  evconnlistener_free(listener);
  INFO("event base break");
  event_base_free(base);
  for (int i = 0; i < 5; i++) {
    sender[i].Stop();
  }
  recvWorker.WakeupAndQuit();

  INFO("end event testing");
}
