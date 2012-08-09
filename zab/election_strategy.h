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

#ifndef ELECTION_STRATEGY_HXX_
#define ELECTION_STRATEGY_HXX_

#include "base/thread.h"
#include "base/logging.h"
#include "quorum_peer.h"
#include "election_protocol.h"
#include "election_cnxmgr.h"

namespace ZABCPP {

  typedef enum {
    FASTPAXOS_UDP = 0,
    FASTPAXOS_TCP = 3
  } ElectionStrategy;

  class QuorumPeer;
  class Election {
    public:
      virtual Vote lookForLeader() = 0;
      virtual void Shutdown() = 0;
      virtual ~Election() {

      }
  };

  class ElectionStrategyFactory {
    public:
      static Election* getElectionStrategy(ElectionStrategy type, QuorumPeer* self, QuorumCnxMgr * mgr);
    private:
      ElectionStrategyFactory() {
      }
      ~ElectionStrategyFactory() {
      }
      ;
  };

  class NotificationQ {
    public:
      NotificationQ()
          : condQueue(&queueLock) {

      }
      ~NotificationQ() {

      }
      void PushBack(const Notification& n) {
        AutoLock guard(queueLock);
        bool needSignal = i_Queue.empty();
        i_Queue.push_back(n);
        if (needSignal)
          condQueue.Broadcast();
      }

      bool pollQueue(int64 ms, Notification& out_n) {
        bool ret = false;
        AutoLock guard(queueLock);
        if (i_Queue.empty()) {
          if (0 < ms) {
            condQueue.TimedWait(ms);
          } else {
            condQueue.Wait();
          }
        }
        if (!i_Queue.empty()) {
          out_n = i_Queue.front();
          i_Queue.pop_front();
          ret = true;
        }
        return ret;
      }

      void WakeUp() {
        AutoLock guard(queueLock);
        condQueue.Broadcast();
      }

      bool Empty() {
        return i_Queue.empty();
      }

      void ClearRecvQueue() {
        AutoLock guard(queueLock);
        i_Queue.clear();
      }
    private:
      list<Notification> i_Queue;
      Lock queueLock;
      ConditionVariable condQueue;
  };

  class FastPaxosElection: public Election {
    public:
      virtual Vote lookForLeader();
      virtual void Shutdown() {
        stop = true;
        i_nQueue.WakeUp();
        recvWorker.Stop();
      }

      FastPaxosElection(QuorumPeer* myself, QuorumCnxMgr * mgr)
          : logicalclock(0)
      , self(myself)
      , cnxMgr(mgr)
      , stop(false)
      , recvWorker(this, myself, mgr, &i_nQueue) {
      }

      virtual ~FastPaxosElection() {
      }

      void getVote(Vote& v) {
        //todo, need a lock here?
        v.id = proposedLeader;
        v.zxid = proposedZxid;
        v.electionEpoch = -1;
        v.peerEpoch = proposedEpoch;
        v.state = LOOKING;
      }

      int64 getLogicalclock() {
        //todo, need a lock here?
        //AutoLock guard(proposalLock);
        return logicalclock;
      }

    protected:
      typedef map<int64, Vote> VoteMap;
      void sendNotifications();
      void updateProposal(int64 leader, int64 zxid, int64 epoch);
      bool totalOrderPredicate(int64 newId, int64 newZxid, int64 newEpoch, int64 curId, int64 curZxid, int64 curEpoch);
      bool termPredicate(const VoteMap& vm, const Vote& v);
      bool checkLeader(const VoteMap& vm, int64 leader, int64 electionEpoch);
      void leaveInstance(const Vote& v);
    private:
      bool pollQueue(int64 max_t_in_ms, Notification& out_n) {
        return i_nQueue.pollQueue(max_t_in_ms, out_n);
      }

      //inside recv worker
      class RecvWorker: public Thread {
        public:
          RecvWorker(FastPaxosElection *el, QuorumPeer* peer, QuorumCnxMgr* mgr, NotificationQ* q)
              : Thread("Election Recv worker"), i_el(el), self(peer), cnxMgr(mgr), i_nQueue(q) {

          }

          ~RecvWorker() {

          }

        protected:
          virtual void Run();
          virtual void ShuttingDown(){
            INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
            cnxMgr->wakeupRecvQueue();
          }

        private:
          FastPaxosElection* i_el;
          QuorumPeer* self;
          QuorumCnxMgr* cnxMgr;
          NotificationQ* i_nQueue;
      };
    private:
      volatile int64 logicalclock; /* Election instance */
      int64 proposedLeader;
      int64 proposedZxid;
      int64 proposedEpoch;
      QuorumPeer* self;
      QuorumCnxMgr* cnxMgr;
      bool stop;
      Lock proposalLock;

      NotificationQ i_nQueue;
      RecvWorker recvWorker;
      DISALLOW_COPY_AND_ASSIGN(FastPaxosElection);
  };

}
;
#endif /* ELECTION_STRATEGY_HXX_ */

