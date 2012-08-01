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

#ifndef LEADER_H_
#define LEADER_H_
#include "quorum_peer.h"
#include "quorum_packet.h"
#include "base/thread.h"
#include "event2/event.h"
#include "base/waitable_event.h"
#include "quorum_server.h"
#include "client_cnx_mgr.h"
#include "client_handler.h"
#include <map>
#include <set>
using namespace std;

namespace ZABCPP {
  class QuorumPeer;
  class Leader;

  typedef enum {
    ZAB_WAITING_EPOCH = 0,
    ZAB_NEW_EPOCH,
    ZAB_WAITING_EPCH_ACK,
    ZAB_NEW_LEADER,
    ZAB_NEW_LEADER_ACK,
    ZAB_LEADER_COMMIT,

    ZAB_QUIT
  } ZabLeaderState;

  class StateSummary {
    public:
      StateSummary()
          : currentEpoch(0), lastZxid(0) {

      }

      StateSummary(int64 epoch, int64 zxid)
          : currentEpoch(epoch), lastZxid(zxid) {

      }
      int64 currentEpoch;
      int64 lastZxid;

      inline bool IsMoreRecentThan(const StateSummary& ss) const {
        return (currentEpoch > ss.currentEpoch) || (currentEpoch == ss.currentEpoch && lastZxid > ss.lastZxid);
      }
  };

  class LearnerCnxMgr: public Thread {
    public:
      LearnerCnxMgr(Leader * ldr);
      ~LearnerCnxMgr();

      static void OnLibEventNotification(evutil_socket_t fd, short what, void *arg);

      static void OnLibEventListenerNotify(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address,
          int socklen, void *ctx);

      static void OnLibEventListenerError(struct evconnlistener *listener, void *ctx);

      static void OnWakeup(evutil_socket_t fd, short what, void *arg);

      static void OnSendingMsgReady(evutil_socket_t fd, short what, void *arg);

      void SendQp(QuorumPacket* packet);
      void PingFollowers();
      void GetSyncedFollowers(set<int64> & syncSet);
    protected:
      virtual void Init();
      virtual void Run();
      virtual void CleanUp();
      virtual void ShuttingDown();
    private:
      void onAccept(int sock);
      void onError();
      void onRecvMsg(int sock);
      void onMsgReady(int fd);

      void processMsg(int fd, string*);

      bool setupPipe(int*);
      void cleanupPipe(int*);

      //have lock inside
      void pushBack_i(QuorumPacket* packet);
      QuorumPacket* popFront_i();

      void sendToAll(QuorumPacket *p);
      int sendToOne(int fd, QuorumPacket *p);

      void msgReady();
      void processQp(int fd, QuorumPacket& qp);
      void processSendQueue();

      void addNewLearner(int sock);
      void removeLearner(int sock);
      string* getSockBuf(int sock);
      void eraseSockBuf(int sock);

      ZabLeaderState getState(int sock);
      void setState(int sock, ZabLeaderState s);
      void setSid(int sock, int64 sid);
      int64 getSid(int fd);

      void setLastTick(int fd);
    private:
      enum {
        PIPE_OUT = 0,
        PIPE_IN = 1
      };

      Leader* leader;
      struct event_base* ebase;
      int wakeup_pipe[2];
      int msg_pipe[2];

      class FollowerInfo {
        public:
          FollowerInfo()
              : sid(0), zabState(ZAB_WAITING_EPOCH), tickOfLastAck(0) {

          }
          ~FollowerInfo() {
          }
          int64 sid;
          ZabLeaderState zabState;
          int64 tickOfLastAck;
      };
      typedef map<int, FollowerInfo> Sock2FollowerMap;
      typedef map<int64, int> Sid2SockMap;
      typedef map<int, struct event*> EventMap;
      typedef map<int, string*> SockBufMap;
      typedef list<QuorumPacket*> SendQueue;

      Sock2FollowerMap i_sock2FollowerMap;
      Sid2SockMap i_sid2SockMap;
      EventMap i_eventMap;
      SockBufMap i_sockBufMap;
      SendQueue i_sendQueue;
      Lock sendQueueLock;
  };

  typedef set<int64> SidSet;

  class Proposal {
    public:
      Proposal() {

      }
      ~Proposal() {

      }
      QuorumPacket qp;
      SidSet ackSet;
  };

  class Leader: public RequestProcessor {
    public:
      Leader(QuorumPeer* myself, ZabDBInterface * z);
      ~Leader();
      void Lead();
      void Shutdown() {
        stop = true;
      }

      //inherit from RequestProcessor
      void ProcessRequest(const Request& req);

      bool GetQuorumAddr(string& addr, int& port);
      bool IsWaitingForNewEpoch() {
        return waitingForNewEpoch;
      }

      bool IsElectionFinished() {
        return electionFinished;
      }

      void NewEpochReady() {
        AutoLock guard(waitingEpochLock);
        waitingForNewEpoch = false;
      }

      void ElectionFinished() {
        AutoLock guard(electionLock);
        electionFinished = true;
      }

      int64 GetProposedEpoch() const {
        return epoch;
      }

      ZabLeaderState GetLeaderState() {
        return leaderState;
      }

      int64 GetTick() {
        return tick;
      }

      int GetSyncLimit();

      void AddConnectingFollowers(int64 sid, int64 lastAcceptedEpoch);
      void AddEpochAck(int64 sid, const StateSummary& ss);
      void AddNewLeaderAct(int64 sid);

      void ProcessAck(int64 sid, int64 zxid);

    private:
      void waitForNewEpoch();
      void waitForEpochAck();
      void waitForNewLeaderAck();

      void addProposal(Proposal *p);
    private:
      QuorumPeer* self;
      LearnerCnxMgr learnerCnxMgr;
      StateSummary lstateSummary;

      SidSet connectingFollowers;
      SidSet electingFollowers;
      SidSet newLeaderAck;

      bool waitingForNewEpoch;
      bool electionFinished;
      int64 epoch;
      Lock waitingEpochLock;
      Lock electionLock;

      WaitableEvent weNewEpoch;
      WaitableEvent weElection;
      WaitableEvent weNewLeader;
      ZabLeaderState leaderState;

      int64 tick;

      typedef map<int64, Proposal*> ProposalMap;
      ProposalMap proposalMap;
      int64 lastCommitted;
      int64 lastProposed;
      Lock proposalMapLock;

      ZabQuorumServer zabServer;
      ClientCnxMgr    clientCnxMgr;
      ClientHandlerInterface* clientHandler;

      bool stop;
  };
}

#endif /* LEADER_H_ */
