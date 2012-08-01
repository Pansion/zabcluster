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

#ifndef FOLLOWER_H_
#define FOLLOWER_H_

#include "quorum_packet.h"
#include "event2/event.h"
#include "quorum_server.h"
#include "client_cnx_mgr.h"
#include "client_handler.h"
#include <string>
using namespace std;

namespace ZABCPP {
  class QuorumPeer;

  typedef enum {
    //send my epoch to leader
    F_EPOCH_PROPOSE = 0,

    //wait for new epoch from leader
    F_WAITING_EPOCH,

    //send epoch ack to leader
    F_NEW_EPOCH_ACK,

    //wait for new leader announce
    F_NEW_LEADER,

    //send new leader ack
    F_NEW_LEADER_ACK,

    //wait for leader commit announce
    F_NEW_LEADER_COMMIT

  } ZabFollowerState;

  class Follower: public RequestProcessor {
    public:
      Follower(QuorumPeer* myself, ZabDBInterface * z);
      ~Follower();

      //inherit from RequestProcessor to handle request from client
      void ProcessRequest(const Request&);

      //static entry to libevent callback
      static void OnLibEventNotification(evutil_socket_t fd, short what, void *arg);

      static void OnWakeup(evutil_socket_t fd, short what, void *arg);

      //main loop when node become a follower
      void FollowLeader();

      void Shutdown();

    private:
      void onRecvMsg(int fd);

      void processMsg(int fd ,string*);

      bool findLeader(string& addr, int& port);
      int connectLeader(const string& addr, int port);
      void processQp(int fd, QuorumPacket& qp);
      void sendQp(const QuorumPacket& qp, int fd);

      void setupPipe(int*);
      void cleanupPipe(int*);
    private:
      enum {
        PIPE_OUT = 0,
        PIPE_IN
      };

      QuorumPeer* self;
      struct event_base* ebase;
      ZabFollowerState zabState;
      bool waitingForRegister;
      bool needBreak;
      string recvBuf;
      int64 epoch;
      int sock;
      int wakeup_pipe[2];

      ZabQuorumServer zabServer;
      ClientCnxMgr    clientCnxMgr;
      ClientHandlerInterface* clientHandler;
  };
}

#endif /* FOLLOWER_H_ */
