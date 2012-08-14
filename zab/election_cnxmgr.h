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

#ifndef ELECTION_CNXMGR_H_
#define ELECTION_CNXMGR_H_

#include <map>
#include <list>
#include <set>
#include "base/byte_buffer.h"
#include "base/thread.h"
#include "base/lock.h"
#include "base/condition_variable.h"
#include "base/waitable_event.h"
#include "peer_config.h"
#include "circle_queue.h"
#include "event2/event.h"
#include "event2/listener.h"

using namespace std;

namespace ZABCPP{

  class ElectionMsgHandlerInterface {
    public:
      ElectionMsgHandlerInterface(){};
      virtual ~ElectionMsgHandlerInterface(){};

      //interface called in Election Cnx Mgr to handle received message
      virtual void HandleIncomingPeerMsg(int64 sid, const char * msg, int len) = 0;
      virtual void HandlePeerShutdown(int64 sid) = 0;
    private:
      DISALLOW_COPY_AND_ASSIGN(ElectionMsgHandlerInterface);
  };

  typedef CircleQueue<ByteBuffer>                 CircleByteBufferQueue;

  //merged recvworker/listener/sendworker into one thread.
  class QuorumCnxMgr : public Thread{
    public:
      QuorumCnxMgr(QuorumPeerConfig* Cfg);
      ~QuorumCnxMgr();

      void toSend(int64 sid, ByteBuffer* buf);
      void connectAll();
      void connectOne(int64 sid);

      void RegisterHandler(ElectionMsgHandlerInterface* );
      void UnregisterHandler(ElectionMsgHandlerInterface*);

      bool haveDelivered();

      QuorumPeerConfig* getPeerConfig() const {
        return peerConfig;
      }

    public:
      //interfaces to libevent
      static void OnLibEventNotification(evutil_socket_t fd, short what, void *arg);

      static void OnSendingMsgReady(evutil_socket_t fd, short what, void *arg);

      static void OnLibEventListenerNotify(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address,
          int socklen, void *ctx);

      static void OnLibEventListenerError(struct evconnlistener *listener, void *ctx);

      static void OnWakeup(evutil_socket_t fd, short what, void *arg);


    protected:
      virtual void Init();
      virtual void Run();
      virtual void CleanUp();
      virtual void ShuttingDown();

    private:
      //called when new connection come in
      void onAccept(int sock);

      //called when accept/listen error occurs
      void onError();

      //called when fd can read
      void onRecvMsg(int sock);

      //called when send queue have message
      void onMsgReady(int64 sid);

      //called when fd can write
      void onMsgCandSend(int fd);

    private:
      typedef map<int64, ByteBuffer*>                 LastMessageMap;
      typedef set<int64>                              LastMsgFlag;
      typedef map<int64, CircleByteBufferQueue*>      SendQueueMap;
      typedef map<int64, int>                         Sid2SockMap;
      typedef map<int, int64>                         Sock2SidMap;
      typedef map<int, struct event*>                 EventMap;
      typedef map<int, string*>                       SockBufMap;
      typedef set<ElectionMsgHandlerInterface*>       HanlderSet;

      enum {
        PIPE_OUT = 0,
        PIPE_IN
      };
    private:
      void initiateConnection(int sock, int64 sid);
      void addConnection(int fd);
      void removeConnection(int fd);
      void handleNewConnection(int fd);

      void addPeer(int fd, int64 sid);
      void removePeer(int fd);
      bool getLastMsgFlag(int64 sid);

      void msgReady(int64 sid);

      //lock free;
      void setLastMsg(int64 sid, ByteBuffer* m);
      ByteBuffer* getLastMsg(int64 sid);

      int64 getSid(int sock);
      int getSock(int64 sid);

      string * getSockBuf(int sock);
      void removeSockBuf(int sock);


      CircleByteBufferQueue* getQueue(int64 sid);

      void addWriteEvent(int fd);
      void removeWriteEvent(int fd);

      void setupPipe(int * pipeA);
      void cleanupPipe(int * pipeA);
      void cleanupSockBuf();
      void cleanupEvents(EventMap & eMap);
      void cleanupAllSendQueue();
      void cleanupRecvQueue();
      void cleanupLastMsg();

    private:
      QuorumPeerConfig*         peerConfig;
      struct event_base*        ebase;
      int                       wakeup_pipe[2];
      int                       msg_pipe[2];
      int                       numRetries;

      EventMap                  eventMap;
      EventMap                  wEventMap;

      HanlderSet                handlerSet;

      SockBufMap                sockBufMap;
      Sid2SockMap               sid2SockMap;
      Sock2SidMap               sock2SidMap;
      SendQueueMap              sendQueueMap;
      LastMessageMap            lastMessageSent;
      LastMsgFlag               lastMsgFlag;

      Lock                      handlerLock;
      Lock                      connectLock;
      Lock                      sendQueueLock;
      Lock                      eventLock;
      Lock                      peerLock;
      WaitableEvent             weRecv;
      DISALLOW_COPY_AND_ASSIGN(QuorumCnxMgr);
  };
}

#endif /* ELECTION_CNXMGR_H_ */
