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

#ifndef ZABDB_REDIS_H_
#define ZABDB_REDIS_H_

#include "zabdb.h"
#include "base/basictypes.h"
#include "base/thread.h"
#include "base/lock.h"
#include "event2/event.h"
#include <map>

using namespace std;

namespace ZABCPP{
  class ZabDBRedis: public ZabDBInterface,
                    public Thread{
    public:
      ZabDBRedis(int64 myid);
      virtual ~ZabDBRedis();

      virtual void SetZxid(int64 zxid);
      virtual int64 GetLastProcessedZxid();
      virtual void ProcessRequest(Request *);
      virtual void SetServerAddr(const string& addr, int port);
      virtual void Startup(){
        Start();
      }
      virtual void Shutdown(){
        Stop();
      }
      static void OnLibEventNotification(evutil_socket_t fd, short what, void *arg);

      static void OnZxidChannelNotification(evutil_socket_t fd, short what, void *arg);

      static void OnWakeup(evutil_socket_t fd, short what, void *arg);

      static void OnRequestReady(evutil_socket_t fd, short what, void *arg);
    protected:
      virtual void Init();
      virtual void Run();
      virtual void CleanUp();
      virtual void ShuttingDown();
    private:
      typedef map<int, int>   ChannelMap;
      typedef map<int, struct event*> EventMap;
      typedef list<Request*>  RequestList;

    private:
      int setupServerChannel(int clientfd);
      void removeChannelByClientFd(int clientfd);
      void removeChannelByServerFd(int serverfd);
      void removeChannelEvent(int serverfd);

      bool removeChannel(ChannelMap& firstm, int k_fd, ChannelMap& secondm, int& v_fd);

      void msgReady();

      void setupPipe(int* pipeA);
      void cleanupPipe(int* pipeA);

      void onRecvMsg(int fd);
      void onMsgReady(int fd);
      void onSendReady(int fd);

      //zxid was very important since it will impact leader election
      //we have sepical handling for zxid at startup
      //if zxid could not be loaded from redis
      //no need to go further
      void blockingGetLastZxid();
      void updateZxid(int64 zxid);
      void onRecvZxid(int fd);
      void onZxidChannelReady(int fd);
      int processZxidReply(string & );

      //have lock inside
      void pushBack(Request*);
      Request* popFront();

      void cleanupRequests(RequestList&);
      void cleanupEvents(EventMap&);

      //we found one connection to redis was down and try to handle it
      //we only care connection broken when sending requests to redis
      //since request was FIFO, if one request could not be sent to redis
      //no need to process further one otherwise data will not be consistent
      //current solution: stop thread immediately
      void handleServerDown(int fd);
    private:
      enum {
        PIPE_OUT = 0,
        PIPE_IN = 1,
      };
      struct event_base*      ebase;
      int64                   sid;
      int64                   lastProcessedZxid;


      int                     zxidChannelFd;
      struct event*           eZxid;
      string                  zxidReply;

      string                  redisServerAddr;
      int                     redisServerPort;

      int                     wakeup_pipe[2];
      int                     msg_pipe[2];
      ChannelMap              client2Server;
      ChannelMap              server2Client;


      EventMap                eventMap;
      EventMap                retryEventMap;

      RequestList             requestList;
      RequestList             retryRequestList;

      Lock                    requestLock;
  };
}

#endif /* ZABDB_REDIS_H_ */
