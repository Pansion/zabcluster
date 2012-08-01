//Copyright (c) 2012, Pansion Chen <pansion dot zabcpp at gmail dot com>
//All rights reserved.
//
//Redistribution and use in source and binary forms, with or without
//modification, are permitted provided that the following conditions are met:
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//    * Neither the name of the zabcpp nor the
//      names of its contributors may be used to endorse or promote products
//      derived from this software without specific prior written permission.
//
//THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
//ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY
//DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


#include "client_cnx_mgr.h"
#include "base/logging.h"
#include "zab_utils.h"
#include "zab_constant.h"
#include "client_handler.h"
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/tcp.h>

namespace ZABCPP{

#define FOR_EACH_HANDLER(HandlerType, HandlerList, func)  \
  do {                                                        \
    for(set<HandlerType>::iterator iter = HandlerList.begin();iter != HandlerList.end();iter++)\
    {                                                                                         \
      HandlerType h = *iter;                                                                  \
      h->func;                                                                                \
    }                                                                                         \
  } while (0)

#define OpenScope         if(1)

  ClientCnxMgr::ClientCnxMgr(int port)
  :Thread("Client Cnx Mgr")
  ,clientPort(port)
  ,ebase(NULL)
  ,numRetries(0){
    wakeup_pipe[PIPE_OUT] = -1;
    wakeup_pipe[PIPE_IN] = -1;
  }

  ClientCnxMgr::~ClientCnxMgr() {

  }

  void ClientCnxMgr::OnLibEventNotification(evutil_socket_t fd, short what, void *arg) {
    ClientCnxMgr * c = static_cast<ClientCnxMgr*>(arg);
    c->onRecvMsg(fd);
  }

  void ClientCnxMgr::OnLibEventListenerNotify(struct evconnlistener *listener,
      evutil_socket_t fd,
      struct sockaddr *address,
      int socklen, void *ctx) {
    ClientCnxMgr * c = static_cast<ClientCnxMgr*>(ctx);
    c->onAccept(fd);
  }

  void ClientCnxMgr::OnLibEventListenerError(struct evconnlistener *listener, void *ctx) {
    int err = EVUTIL_SOCKET_ERROR();
    ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") on the listener.");
    ClientCnxMgr * c = static_cast<ClientCnxMgr*>(ctx);
    c->onListenError();
  }

  void ClientCnxMgr::OnWakeup(evutil_socket_t fd, short what, void *arg) {
    INFO("Waked up and break event loop");
    char c = 0;
    read(fd, &c, 1);
    struct event_base* b = (struct event_base*) arg;
    event_base_loopbreak(b);
  }

  void ClientCnxMgr::RegisterHanlder(ClientHandlerInterface * h) {
    AutoLock  guard(handlerLock);
    clientHandlers.insert(h);
  }

  void ClientCnxMgr::UnregisterHandler(ClientHandlerInterface * h) {
    AutoLock  guard(handlerLock);
    clientHandlers.erase(h);
  }

  void ClientCnxMgr::onRecvMsg(int sock) {
    char msg[READ_BUF_LEN];
    memset(msg, 0, READ_BUF_LEN);
    int readnum = recv(sock, msg, READ_BUF_LEN, 0);

    if (readnum == -1) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") on the ClientCnxMgr.");
      if (err == EAGAIN) {
        readnum = 0;
      } else {
        removeClient(sock);
        OpenScope {
          AutoLock guard(handlerLock);
          FOR_EACH_HANDLER(ClientHandlerInterface*, clientHandlers, HandleClientShutdown(sock));
        }
        close(sock);
      }
      return;
    } else if (readnum == 0) {
      ERROR("connection on sock "<<sock<<" was shutdown by client");
      removeClient(sock);
      OpenScope {
        AutoLock guard(handlerLock);
        FOR_EACH_HANDLER(ClientHandlerInterface*, clientHandlers, HandleClientShutdown(sock));
      }
      close(sock);
      return;
    }

    OpenScope {
      FOR_EACH_HANDLER(ClientHandlerInterface*, clientHandlers, HandleIncomingRequest(sock, msg, readnum));
    }
  }

  void ClientCnxMgr::onAccept(int sock) {
    int flag = 1;
    setsockopt(sock, /* socket affected */
                IPPROTO_TCP, /* set option at TCP level */
                TCP_NODELAY, /* name of option */
                (char *) &flag, /* the cast is historical cruft */
                sizeof(int)); /* length of option value */
    addNewClient(sock);
  }

  void ClientCnxMgr::onListenError() {
    numRetries++;
    event_base_loopbreak(ebase);
  }

  void ClientCnxMgr::Init() {
    if (ebase == NULL) {
      ebase = event_base_new();
    }

    setupPipe(wakeup_pipe);

    struct event* e = event_new(ebase, wakeup_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &ClientCnxMgr::OnWakeup, ebase);
    event_add(e, NULL);
    eventMap[wakeup_pipe[PIPE_OUT]] = e;

    numRetries = 0;
  }

  void ClientCnxMgr::Run() {
    struct sockaddr_in servSin;

    while(!stopping_ && (numRetries < MAX_LISTENER_RETIES)){
      memset(&servSin, 0, sizeof(servSin)); /* Zero out structure */
      servSin.sin_family = AF_INET; /* Internet address family */
      servSin.sin_addr.s_addr = htonl(INADDR_ANY); /* Any incoming interface */
      servSin.sin_port = htons(clientPort); /* Local port */

      struct evconnlistener *listener = evconnlistener_new_bind(ebase, &ClientCnxMgr::OnLibEventListenerNotify, this,
          LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1, (struct sockaddr*) &servSin, sizeof(servSin));

      if (!listener) {
        ERROR("Couldn't create listener");
        numRetries++;
        continue;
      }
      evconnlistener_set_error_cb(listener, &ClientCnxMgr::OnLibEventListenerError);

      event_base_dispatch(ebase);

      evconnlistener_free(listener);
      WARN("event loop break, retry times "<<numRetries);
      if (stopping_)
        return;
    }
  }

  void ClientCnxMgr::CleanUp() {
    cleanupEvents();

    cleanupPipe(wakeup_pipe);

    if (ebase != NULL) {
      event_base_free(ebase);
      ebase = NULL;
    }
  }

  void ClientCnxMgr::ShuttingDown() {
    INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
    if (wakeup_pipe[PIPE_IN] != -1) {
      char c = 0;
      write(wakeup_pipe[PIPE_IN], &c, 1);
    }
  }

  void ClientCnxMgr::addNewClient(int sock) {
    INFO("Got new client connection on sock "<<sock);
    EventMap::iterator iter = eventMap.find(sock);
    if (iter == eventMap.end()) {
      struct event * e = event_new(ebase, sock, EV_READ | EV_PERSIST, &ClientCnxMgr::OnLibEventNotification, this);
      if (e != NULL) {
        event_add(e, NULL);
        eventMap.insert(pair<int, struct event*>(sock, e));
      } else {
        ERROR("could not create event for sock "<<sock);
      }
    } else {
      WARN("client with sock "<<sock<<" already existed");
    }
  }

  void ClientCnxMgr::removeClient(int sock) {
    INFO("Try to remove client connection on sock "<<sock);
    EventMap::iterator iter = eventMap.find(sock);
    if (iter != eventMap.end()) {
      struct event *e = iter->second;
      event_del(e);
      event_free(e);
      eventMap.erase(iter);
      INFO("Removed event on sock "<<sock);
    }
  }

  void ClientCnxMgr::cleanupEvents() {
    for (EventMap::iterator iter = eventMap.begin(); iter != eventMap.end(); iter++) {
      struct event * e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
      close(iter->first);
    }
    eventMap.clear();
  }

  void ClientCnxMgr::setupPipe(int* pipeA) {
    if (pipe(pipeA)) {
      ERROR("Could not create pipe");
      return;
    }

    ZabUtil::SetNoneBlockFD((pipeA)[0]);
    ZabUtil::SetNoneBlockFD((pipeA)[1]);
  }

  void ClientCnxMgr::cleanupPipe(int* pipeA) {
    if (pipeA[PIPE_OUT] != -1) {
      close(pipeA[PIPE_OUT]);
    }

    if (pipeA[PIPE_IN] != -1) {
      close(pipeA[PIPE_IN]);
    }
  }

}

