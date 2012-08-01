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

#ifndef ELECTION_CNXMGR_HXX_
#define ELECTION_CNXMGR_HXX_

#include <map>
#include <list>
#include <vector>
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

//todo, we need to find a way to make thread stop gracefully
// in order to acheive this, we should using poll everywhere
// which enable us to use pipe to wake up a thread and die

namespace ZABCPP {

  class QuorumCnxMgr;

  static const int MAX_PEER_NUM = 1024;

  typedef map<int64, ByteBuffer*> lastMessageMap;

  class Message {
    public:
      ByteBuffer buffer;
      int64 sid;
  };

  typedef CircleQueue<ByteBuffer>  CircleByteBufferQueue;
  typedef CircleQueue<Message>     CircleMessageQueue;

  class SendWorker: public Thread {
    public:
      SendWorker();
      ~SendWorker();

      static void OnLibEventNotification(evutil_socket_t fd, short what, void *arg);
      static void OnWakeup(evutil_socket_t fd, short what, void *arg);

      bool IsAllDelivered();
      void AddPeer(int sock, int64 sid);
      void RemovePeer(int sock);

      void SendMsg(int64 sid, ByteBuffer * m) {
        getQueue(sid)->PushBack(m);
        msgReady(sid);
      }
    protected:
      virtual void Init();
      virtual void Run();
      virtual void CleanUp();
      virtual void ShuttingDown();
    private:
      void OnCanSend(int64 sid);

      void msgReady(int64 sid);

      //lock free;
      void setLastMsg(int64 sid, ByteBuffer* m);
      ByteBuffer* getLastMsg(int64 sid);
      void i_sendMsg(int sock, int64 sid, ByteBuffer* msg);
      void cleanAllLastMsg();

      //lock needed
      int64 getSid(int sock);
      int getSock(int64 sid);
      CircleByteBufferQueue* getQueue(int64 sid);
      void cleanAllQueue();

      bool setupPipe(int * pipeA);
      void cleanupPipe(int * pipeA);
      void cleanupEvents();
    private:
      enum {
        PIPE_OUT = 0,
        PIPE_IN
      };
      struct event_base* ebase;
      int wakeup_pipe[2];
      int msg_pipe[2];

      typedef map<int64, CircleByteBufferQueue*> sendQueueMap;
      typedef map<int64, int> sid2SockMap;
      typedef map<int, int64> sock2SidMap;
      typedef map<int64, ByteBuffer*> lastMessageMap;
      typedef pair<int64, ByteBuffer*> lastMsgPair;
      typedef set<int64> lastMsgFlag;
      typedef set<struct event *>     EventSet;
      sid2SockMap i_sid2SockMap;
      sock2SidMap i_sock2SidMap;
      sendQueueMap i_sendQueueMap;
      lastMessageMap i_lastMessageSent;
      lastMsgFlag i_lastMsgFlag;
      EventSet    i_eventSet;
      Lock mapLock;
      Lock sendQueueLock;
  };

  //--------------------Recv Worker----------------
  class RecvWorker: public Thread {
    public:
      RecvWorker(QuorumCnxMgr* pMgr);
      ~RecvWorker();

      static void OnLibEventNotification(evutil_socket_t fd, short what, void *arg);
      static void OnWakeup(evutil_socket_t fd, short what, void *arg);

      void AddPeer(int sock, int64 sid);
      void RemovePeer(int sock);
      bool IsConExists(int64 sid) {
        AutoLock guard(mapLock);
        return (i_conSet.find(sid) != i_conSet.end());
      }
    protected:
      virtual void Init();
      virtual void Run();
      virtual void CleanUp();
      virtual void ShuttingDown();
    private:
      void onRecvMsg(int fd);

      void processMsg(int fd, string*);

      string* getSockBuf(int fd);
      void removeSockBuf(int fd);

      void cleanupEvents();
      void cleanupSockBuf();

    private:
      enum {
        PIPE_OUT = 0,
        PIPE_IN
      };
      QuorumCnxMgr* pCnxMgr;
      struct event_base* ebase;
      int wakeup_pipe[2];

      typedef map<int, struct event*> eventMap;
      typedef pair<int, struct event*> eventMapPair;
      typedef map<int, int64> Sock2SidMap;
      typedef map<int, string*> SockBufMap;
      typedef set<int64> conSet;
      eventMap eMap;
      Sock2SidMap i_sock2SidMap;
      SockBufMap  i_sockBufMap;
      conSet i_conSet;
      Lock mapLock;
      DISALLOW_COPY_AND_ASSIGN(RecvWorker);
  };

//----------------------Listener---------------
  //todo should we use libevent for listener????
  class Listener: public Thread {
    public:
      Listener(QuorumCnxMgr* pMgr)
          : Thread("Net Listener"), pCnxMgr(pMgr), ebase(NULL), numRetries(0) {
        wakeup_pipe[PIPE_OUT] = -1;
        wakeup_pipe[PIPE_IN] = -1;
      }
      ~Listener();

      static void OnLibEventNotification(evutil_socket_t fd, short what, void *arg);

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
      void onAccept(int sock);
      void onError();
      void onRecvMsg(int sock);

      void addEvent(int sock);
      void removeEvent(int sock);

      string* getSockBuf(int sock);
      void removeSockBuf(int sock);

      void cleanupEvents();
      void cleanupSockBuf();
    private:
      enum {
        PIPE_OUT = 0,
        PIPE_IN = 1
      };
      QuorumCnxMgr* pCnxMgr;
      struct event_base* ebase;
      int numRetries;
      int wakeup_pipe[2];

      typedef map<int, struct event*>  EventMap;
      typedef map<int, string*>        SockBuf;
      EventMap                        eventMap;
      SockBuf                         sockBuf;

      DISALLOW_COPY_AND_ASSIGN(Listener);
  };

  //-------------------------QuorumCnxMgr------------------------

  class QuorumCnxMgr {
    public:
      QuorumCnxMgr(QuorumPeerConfig* Cfg);
      ~QuorumCnxMgr();

      void Start();
      void Stop();
      void toSend(int64 sid, ByteBuffer* buf);
      void connectAll();
      void connectOne(int64 sid);
      bool initiateConnection(int sock, int64 sid);
      bool receiveConnection(int sock, int64 sid);
      Message* pollRecvQueue(int64 max_time_in_ms);
      void addToRecvQueue(Message* msg);

      QuorumPeerConfig* getPeerConfig() const {
        return peerConfig;
      }

      bool haveDelivered() {
        return i_SendWorker.IsAllDelivered();
      }

      void clearRecvQueue() {
        i_recvQueue.Clear();
      }
    private:
      bool isConExists(int64 sid);
      void setupNewCon(int sock, int64 sid);
    private:
      CircleMessageQueue i_recvQueue;

      QuorumPeerConfig* peerConfig;

      Listener i_Listener;
      RecvWorker i_RecvWorker;
      SendWorker i_SendWorker;

      Lock connectLock;
      WaitableEvent     weRecv;
      DISALLOW_COPY_AND_ASSIGN(QuorumCnxMgr);
  };
}
;

#endif /* ELECTION_CNXMGR_HXX_ */
