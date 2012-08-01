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

#ifndef CLIENT_CNX_MGR_H_
#define CLIENT_CNX_MGR_H_

#include "event2/event.h"
#include "event2/listener.h"
#include "base/basictypes.h"
#include "base/thread.h"
#include "base/lock.h"
#include <map>
#include <set>

using namespace std;

namespace ZABCPP{
  class ClientHandlerInterface;
  //we will simply accept connection and read message from clients
  //once message consider as a "whole" request, it will submit to quorum server
  //the request format could be different. We could implement different clienthanlder to
  // support variety clients.
  class ClientCnxMgr:public Thread{
    public:
      ClientCnxMgr(int);
      virtual ~ClientCnxMgr();

      static void OnLibEventNotification(evutil_socket_t fd, short what, void *arg);

      static void OnLibEventListenerNotify(struct evconnlistener *listener,
          evutil_socket_t fd,
          struct sockaddr *address,
          int socklen, void *ctx);

      static void OnLibEventListenerError(struct evconnlistener *listener, void *ctx);

      static void OnWakeup(evutil_socket_t fd, short what, void *arg);

      void RegisterHanlder(ClientHandlerInterface *);
      void UnregisterHandler(ClientHandlerInterface *);
    protected:
      void onRecvMsg(int);
      void onAccept(int);
      void onListenError();
    protected:
      virtual void Init();
      virtual void Run();
      virtual void CleanUp();
      virtual void ShuttingDown();
    private:
      void addNewClient(int);
      void removeClient(int);
      void cleanupEvents();
      void setupPipe(int*);
      void cleanupPipe(int*);

    private:
      enum {
        PIPE_OUT = 0,
        PIPE_IN
      };
      int                               clientPort;
      struct event_base*                ebase;

      typedef set<ClientHandlerInterface*>  HandlerSet;
      HandlerSet                        clientHandlers;
      Lock                              handlerLock;

      typedef map<int, struct event*>   EventMap;
      EventMap                          eventMap;
      int                               wakeup_pipe[2];
      int                               numRetries;
  };
}



#endif /* CLIENT_CNX_MGR_H_ */
