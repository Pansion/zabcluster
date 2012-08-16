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


#include "election_strategy.h"
#include "base/logging.h"
#include "zab_utils.h"
#include "zab_constant.h"
#include <map>
#include <sys/time.h>

using namespace std;

namespace ZABCPP {

  static const int finalizeWait = 200;
  static const int maxNotificationInterval = 60000;

  Election* ElectionStrategyFactory::getElectionStrategy(ElectionStrategy type, QuorumPeer* self, QuorumCnxMgr * mgr) {
    Election * el = NULL;
    switch (type) {
      case FASTPAXOS_TCP:
        el = new FastPaxosElection(self, mgr);
        break;
      default:
        break;
    }
    return el;
  }
  //--------------------------FastPaxosElection-----------------------
  FastPaxosElection::FastPaxosElection(QuorumPeer* myself, QuorumCnxMgr * mgr)
  :self(myself),
   cnxMgr(mgr),
   stop(false),
   logicalclock(0),
   proposedLeader(-1),
   proposedZxid(-1),
   proposedEpoch(-1),
   notificationQueue(RECV_CAPACITY),
   weNotification(false, false) {
    cnxMgr->RegisterHandler(this);
  }

  FastPaxosElection::~FastPaxosElection() {
    cleanupPeerMsgBuf();
  }

  //interface called in ElectionCnxMgr to handle received message
  void FastPaxosElection::HandleIncomingPeerMsg(int64 sid, const char * msg, int len) {
    string * buf = getPeerMsgBuf(sid);
    buf->append(msg, len);

    while (sizeof(uint32) <= buf->length()) {
      ByteBuffer byteBuf(buf->data(), buf->length());
      int32 packetLen = 0;
      byteBuf.ReadInt32(packetLen);
      if ((0 < packetLen) && (packetLen < PACKETMAXSIZE)) {
        int totalLen = sizeof(uint32) + packetLen;
        if (totalLen <= (int) buf->length()) {
          Notification n;
          n.sid = sid;
          byteBuf.ReadInt32(n.state);
          byteBuf.ReadInt64(n.leader);
          byteBuf.ReadInt64(n.zxid);
          byteBuf.ReadInt64(n.electionEpoch);
          byteBuf.ReadInt64(n.peerEpoch);
          handleNotification(n);
          buf->erase(0, totalLen);
        } else {
          TRACE("Message was only "<<buf->length()<<" required "<<totalLen);
          return;
        }
      }
    }
  }

  void FastPaxosElection::HandlePeerShutdown(int64 sid) {
    removePeerMsgBuf(sid);
  }

  string* FastPaxosElection::getPeerMsgBuf(int64 sid) {
    string * ret = NULL;
    PeerMsgBufMap::iterator iter = peerMsgBufMap.find(sid);
    if (iter != peerMsgBufMap.end()) {
      ret = iter->second;
    } else {
      ret = new string();
      peerMsgBufMap.insert(pair<int64, string*>(sid, ret));
    }
    return ret;
  }

  void    FastPaxosElection::removePeerMsgBuf(int64 sid) {
    PeerMsgBufMap::iterator iter = peerMsgBufMap.find(sid);
    if (iter != peerMsgBufMap.end()) {
      string * b = iter->second;
      if (b != NULL) {
        delete b;
      }
      peerMsgBufMap.erase(iter);
    }
  }

  void    FastPaxosElection::cleanupPeerMsgBuf() {
    for(PeerMsgBufMap::iterator iter = peerMsgBufMap.begin();
        iter != peerMsgBufMap.end();
        iter ++) {
      string * b = iter->second;
      if (b != NULL) {
        delete b;
      }
    }
    peerMsgBufMap.clear();
  }

  void FastPaxosElection::getVote(Vote& v) {
    AutoLock guard(proposalLock);
    v.id = proposedLeader;
    v.zxid = proposedZxid;
    v.electionEpoch = -1;
    v.peerEpoch = proposedEpoch;
    v.state = LOOKING;
  }

  int64 FastPaxosElection::getLogicalclock() {
    AutoLock guard(proposalLock);
    return logicalclock;
  }

  void FastPaxosElection::handleNotification(const Notification& n) {
    DEBUG("get new notification: id "<<ZxidUtils::HexStr(n.sid)
    <<" proposed leader:"<<ZxidUtils::HexStr(n.leader)
    <<" proposed zxid:"<<ZxidUtils::HexStr(n.zxid)
    <<" peer electionEpoch:"<<ZxidUtils::HexStr(n.electionEpoch)
    <<" peerEpoch:"<<ZxidUtils::HexStr(n.peerEpoch)
    <<" peerState:"<<ZxidUtils::HexStr(n.state));

    if (self->getPeerState() == LOOKING) {
      //create new node in queue
      Notification * newNode = new Notification();
      newNode->electionEpoch = n.electionEpoch;
      newNode->leader = n.leader;
      newNode->peerEpoch = n.peerEpoch;
      newNode->sid = n.sid;
      newNode->state = n.state;
      newNode->zxid = n.zxid;
      notificationQueue.PushBack(newNode);
      weNotification.Signal();

      if ((n.state == LOOKING) && (n.electionEpoch < getLogicalclock())) {
        Vote v;
        getVote(v);
        ByteBuffer* buf = new ByteBuffer();
        buf->WriteInt32(sizeof(int32)+sizeof(int64)*4);
        buf->WriteInt32((int32) self->getPeerState());
        buf->WriteInt64(v.id);
        buf->WriteInt64(v.zxid);
        buf->WriteInt64(getLogicalclock());
        buf->WriteInt64(v.peerEpoch);
        cnxMgr->toSend(n.sid, buf);
      }
    } else {
      /*
       * If this server is not looking, but the one that sent the ack
       * is looking, then send back what it believes to be the leader.
       */
      Vote current = self->getCurrentVote();
      if (n.state == LOOKING) {
        DEBUG("Sending new notification.My id="<<ZxidUtils::HexStr(self->getId())
        <<" recipient="<<ZxidUtils::HexStr(n.sid)
        <<" zxid="<<ZxidUtils::HexStr(current.zxid)
        <<" leader="<<ZxidUtils::HexStr(current.id));

        ByteBuffer* buf = new ByteBuffer();
        buf->WriteInt32(sizeof(int32) + sizeof(int64)*4);
        buf->WriteInt32((int32) self->getPeerState());
        buf->WriteInt64(current.id);
        buf->WriteInt64(current.zxid);
        buf->WriteInt64(getLogicalclock());
        buf->WriteInt64(current.peerEpoch);
        cnxMgr->toSend(n.sid, buf);
      }
    }
  }

  void FastPaxosElection::updateProposal(int64 leader, int64 zxid, int64 epoch) {
    INFO("Updating proposal: "<<ZxidUtils::HexStr(leader)<<" (newleader) "
        <<ZxidUtils::HexStr(zxid)<<" (newzxid) "
        <<ZxidUtils::HexStr(epoch)<<" (newepoch) "
        <<ZxidUtils::HexStr(proposedLeader)<< " (oldleader) "
        <<ZxidUtils::HexStr(proposedZxid)<<" (oldzxid) "
        <<ZxidUtils::HexStr(proposedEpoch)<<" (oldepoch)");
    proposedLeader = leader;
    proposedZxid = zxid;
    proposedEpoch = epoch;
  }

  bool FastPaxosElection::totalOrderPredicate(int64 newId, int64 newZxid, int64 newEpoch, int64 curId, int64 curZxid,
      int64 curEpoch) {
    INFO("totalOrderPredicate: id:"<<ZxidUtils::HexStr(newId)
    <<", current proposed id:"<<ZxidUtils::HexStr(curId)
    <<", zxid:"<<ZxidUtils::HexStr(newZxid)
    <<", current proposed zxid:"<<ZxidUtils::HexStr(curZxid)
    <<", epoch:"<<ZxidUtils::HexStr(newEpoch)
    <<", current proposed epoch:"<<ZxidUtils::HexStr(curEpoch));
    if (self->getQuorumVerifier()->getWeight(newId) == 0) {
      return false;
    }

    return ((newEpoch > curEpoch) || ((newEpoch == curEpoch) && (newZxid > curZxid))
        || ((newZxid == curZxid) && (newId > curId)));
  }

  bool FastPaxosElection::termPredicate(const VoteMap& vm, const Vote& v) {
    set<int64> set;
    for (VoteMap::const_iterator iter = vm.begin(); iter != vm.end(); iter++) {
      if (iter->second == v) {
        set.insert(iter->first);
      }
    }
    DEBUG("termPredicate, votes for sever "<<v.id<<" were "<<set.size());
    return self->getQuorumVerifier()->containsQuorum(set);
  }

  bool FastPaxosElection::checkLeader(const VoteMap& vm, int64 leader, int64 electionEpoch) {
    bool predicate = true;

    /*
     * If everyone else thinks I'm the leader, I must be the leader.
     * The other two checks are just for the case in which I'm not the
     * leader. If I'm not the leader and I haven't received a message
     * from leader stating that it is leading, then predicate is false.
     */

    VoteMap::const_iterator iterL = vm.find(leader);
    if (leader != self->getId()) {
      if (iterL == vm.end())
        predicate = false;
      else if (iterL->second.state != LEADING)
        predicate = false;
    }

    return predicate;
  }

  void FastPaxosElection::leaveInstance(const Vote& v) {
    INFO("About to leave FLE instance: leader="<<ZxidUtils::HexStr(v.id)
    <<", zxid="<<ZxidUtils::HexStr(v.zxid)
    <<", my id="<<ZxidUtils::HexStr(self->getId())
    <<", my state="<<ZxidUtils::HexStr((int32)self->getPeerState())
    <<", my peerEpoch="<<ZxidUtils::HexStr(v.peerEpoch));
    notificationQueue.Clear();
  }

  void FastPaxosElection::sendNotifications() {
    for (QuorumServerMap::iterator iter = cnxMgr->getPeerConfig()->servers.begin();
        iter != cnxMgr->getPeerConfig()->servers.end(); iter++) {
      DEBUG("Sending Notification: proposedLeader "
          <<ZxidUtils::HexStr(proposedLeader)<<" (n.leader),"
          <<ZxidUtils::HexStr(proposedZxid)<<" (n.zxid),"
          <<ZxidUtils::HexStr(logicalclock)<<" (n.round),"
          <<ZxidUtils::HexStr(iter->first)<<" (recipient),"
          <<ZxidUtils::HexStr(self->getId())<<" (myid),"
          <<ZxidUtils::HexStr(proposedEpoch)<<" (n.peerEpoch)");
      if (iter->first != self->getId()) {
        ByteBuffer * n = new ByteBuffer();
        n->WriteInt32(sizeof(int32)+sizeof(int64)*4);
        n->WriteInt32((int32) self->getPeerState());
        n->WriteInt64(proposedLeader);
        n->WriteInt64(proposedZxid);
        n->WriteInt64(logicalclock);
        n->WriteInt64(proposedEpoch);
        cnxMgr->toSend(iter->first, n);
      } else {
        Notification * n = new Notification();
        n->state = self->getPeerState();
        n->leader = proposedLeader;
        n->zxid = proposedZxid;
        n->peerEpoch = proposedEpoch;
        n->electionEpoch = logicalclock;
        n->sid = iter->first;
        notificationQueue.PushBack(n);
      }
    }
  }

  Notification * FastPaxosElection::pollQueue(int64 milli_sec) {
    if(notificationQueue.Empty()) {
      weNotification.TimedWait(milli_sec);
    }
    return notificationQueue.PopFront();
  }

  Vote FastPaxosElection::lookForLeader() {
    VoteMap recvset;
    VoteMap outofelection;

    int notTimeout = finalizeWait;

    if(1) {
      AutoLock guard(proposalLock);
      logicalclock++;
      updateProposal(self->getId(), self->getLastZxid(), self->getCurrentEpoch());
    }

    INFO("New election. My id =  "<<self->getId() <<", proposed zxid=0x"<<ZxidUtils::HexStr(proposedZxid));
    sendNotifications();

    while ((self->getPeerState() == LOOKING) && (!stop)) {
      Notification * node = pollQueue(notTimeout * 1000);

      if (stop) {
        break;
      }

      if (node == NULL) {
        if (cnxMgr->haveDelivered()) {
          sendNotifications();
        } else {
          cnxMgr->connectAll();
        }

        /*
         * Exponential backoff
         */
        int tmpTimeOut = notTimeout * 2;
        notTimeout = (tmpTimeOut < maxNotificationInterval ? tmpTimeOut : maxNotificationInterval);
        INFO("Notification time out: "<<notTimeout);
      } else {

        //FIXME UGLY.yes. below are ugly copy actions.will refactory it later
        Notification n;
        n.electionEpoch = node->electionEpoch;
        n.leader = node->leader;
        n.peerEpoch = node->peerEpoch;
        n.sid = node->sid;
        n.state = node->state;
        n.zxid = node->zxid;
        delete node;

        //todo, need to check if sid was in quorum set
        if (n.sid) {
          switch (n.state) {
            case LOOKING: {
              // If notification > current, replace and send messages out
              if (n.electionEpoch > logicalclock) {
                logicalclock = n.electionEpoch;
                recvset.clear();
                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, self->getId(), self->getLastZxid(),
                    self->getCurrentEpoch())) {
                  updateProposal(n.leader, n.zxid, n.peerEpoch);
                } else {
                  updateProposal(self->getId(), self->getLastZxid(), self->getCurrentEpoch());
                }
                sendNotifications();
              } else if (n.electionEpoch < logicalclock) {
                DEBUG("Notification election epoch is smaller than logicalclock.n.electionEpoch="
                    <<ZxidUtils::HexStr(n.electionEpoch)<<",logicalclock="<<ZxidUtils::HexStr(logicalclock));
                break;
              } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                updateProposal(n.leader, n.zxid, n.peerEpoch);
                sendNotifications();
              }
              INFO("Adding vote: from="<<ZxidUtils::HexStr(n.sid)
              <<", proposed leader="<<ZxidUtils::HexStr(n.leader)
              <<", proposed zxid="<<ZxidUtils::HexStr(n.zxid)
              <<", proposed election epoch="<<ZxidUtils::HexStr(n.electionEpoch)
              <<", proposed peer epoch="<<ZxidUtils::HexStr(n.peerEpoch));
              Vote v(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
              recvset[n.sid] = v;

              Vote newv(proposedLeader, proposedZxid, logicalclock, proposedEpoch);
              if (termPredicate(recvset, newv)) {

                // Verify if there is any change in the proposed leader
                Notification* n;
                while ((n = pollQueue(finalizeWait * 1000)) != NULL) {
                  if (totalOrderPredicate(n->leader, n->zxid, n->peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                    notificationQueue.PushBack(n);
                    break;
                  }
                }

                /*
                 * This predicate is true once we don't read any new
                 * relevant message from the reception queue
                 */
                if (n == NULL) {
                  self->setPeerState((proposedLeader == self->getId()) ? LEADING : FOLLOWING);
                  leaveInstance(newv);
                  return newv;
                }
              }
              break;
            }
            case LEADING:
            case FOLLOWING: {
              /*
               * Consider all notifications from the same epoch
               * together.
               */
              if (n.electionEpoch == logicalclock) {
                Vote v(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                v.state = n.state;
                recvset[n.sid] = v;
                //recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                if (termPredicate(recvset, v) && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                  self->setPeerState((n.leader == self->getId()) ? LEADING : FOLLOWING);
                  leaveInstance(v);
                  return v;
                }
              }

              /**
               * Before joining an established ensemble, verify that
               * a majority are following the same leader.
               */
              Vote v(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
              v.state = n.state;
              outofelection[n.sid] = v;
              if (termPredicate(outofelection, v) && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                {
                  AutoLock guard(proposalLock);
                  logicalclock = n.electionEpoch;
                  self->setPeerState((n.leader == self->getId()) ? LEADING : FOLLOWING);
                }
                leaveInstance(v);
                return v;
              }
              break;
            }
            default: {
              ERROR("Unknown state "<<n.state);
              break;
            }
          }
        }
      }
    }
    Vote nv(-1, -1, -1, -1);
    return nv;
  }

  void FastPaxosElection::Shutdown() {
    stop = true;
    weNotification.Signal();
  }
}

