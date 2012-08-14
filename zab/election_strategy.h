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

  //--------------------------FastPaxoElection-----------
  //The concrete election object should handle
  //1.election logic which inherit from parent Election
  //2.election message received from other peer which inherited from ElectionMsgHandlerInterface

  class FastPaxosElection: public Election,
                           public ElectionMsgHandlerInterface{
    public:
      FastPaxosElection(QuorumPeer* myself, QuorumCnxMgr * mgr);
      virtual ~FastPaxosElection();

      //interface inherited from Election
      virtual Vote lookForLeader();
      virtual void Shutdown();

      //interface called in ElectionCnxMgr to handle received message
      virtual void HandleIncomingPeerMsg(int64 sid, const char * msg, int len);
      virtual void HandlePeerShutdown(int64 sid);

    protected:
      typedef map<int64, Vote> VoteMap;
      void sendNotifications();
      void updateProposal(int64 leader, int64 zxid, int64 epoch);
      bool totalOrderPredicate(int64 newId, int64 newZxid, int64 newEpoch, int64 curId, int64 curZxid, int64 curEpoch);
      bool termPredicate(const VoteMap& vm, const Vote& v);
      bool checkLeader(const VoteMap& vm, int64 leader, int64 electionEpoch);
      void leaveInstance(const Vote& v);

      Notification * pollQueue(int64 milli_sec);

      void getVote(Vote& v);
      int64 getLogicalclock();
    protected:
      //function for handling peer message
      string* getPeerMsgBuf(int64 sid);
      void    removePeerMsgBuf(int64 sid);
      void    cleanupPeerMsgBuf();
      void    handleNotification(const Notification& n);
    private:
      //members for election
      QuorumPeer* self;
      QuorumCnxMgr* cnxMgr;
      bool stop;

      volatile int64 logicalclock; /* Election instance */
      int64 proposedLeader;
      int64 proposedZxid;
      int64 proposedEpoch;
      Lock proposalLock;

    private:
      //members for handling incoming election message from peer
      typedef CircleQueue<Notification>     CircleNotifcationQueue;
      typedef map<int64, string*>           PeerMsgBufMap;

      PeerMsgBufMap                         peerMsgBufMap;
      CircleNotifcationQueue                notificationQueue;
      WaitableEvent                         weNotification;

      DISALLOW_COPY_AND_ASSIGN(FastPaxosElection);
  };

}
;
#endif /* ELECTION_STRATEGY_HXX_ */

