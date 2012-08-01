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

#ifndef QUORUM_PEER_H_
#define QUORUM_PEER_H_

#include "election_protocol.h"
#include "peer_config.h"
#include "quorum_verifier.h"
#include "election_cnxmgr.h"
#include "election_strategy.h"
#include "base/thread.h"
#include "base/lock.h"
#include "zabdb.h"
#include "leader.h"
#include "follower.h"
#include "quorum_server.h"

namespace ZABCPP {
  class Election;
  class Follower;
  class Leader;

  class QuorumPeer: public Thread {
    public:
      QuorumPeer(QuorumPeerConfig * cfg);
      ~QuorumPeer();

      int64 getCurrentEpoch();
      void setCurretnEpoch(int64 val);

      int64 getAcceptedEpoch();
      void setAcceptedEpoch(int64 val);

      int64 getLastZxid();

      inline int64 getId() {
        return peerConfig->serverId;
      }

      PeerState getPeerState() {
        AutoLock guard(stateLock);
        return state;
      }

      void setPeerState(PeerState s) {
        AutoLock guard(stateLock);
        state = s;
      }

      QuorumVerifier* getQuorumVerifier() const {
        return peerConfig->quorumVerifier;
      }

      Vote getCurrentVote() {
        AutoLock guard(voteLock);
        return currentVote;
      }

      void setCurrentVote(const Vote& v) {
        AutoLock guard(voteLock);
        currentVote = v;
      }

      bool GetPeerAddr(int64 sid, string & addr, int& port) {
        bool ret = false;
        QuorumServerMap::iterator iter = peerConfig->servers.find(sid);
        if (iter != peerConfig->servers.end()) {
          addr = iter->second.addr;
          port = iter->second.peerPort;
          ret = true;
        }
        return true;
      }

      inline int tickTime() const {
        return peerConfig->tickTime;
      }

      inline int syncLimit() const {
        return peerConfig->syncLimit;
      }

      inline int initLimit() const {
        return peerConfig->initLimit;
      }

      inline string getDataDir() const {
        return peerConfig->dataDir;
      }

      inline int clientPort() const {
        return peerConfig->clientPort;
      }

    protected:
      virtual void Init();
      virtual void Run();
      virtual void CleanUp();
      virtual void ShuttingDown();
    private:
      int64 ReadInt64FromFile(const string& fn);
      void WriteInt64ToFile(const string& fn, int64 val);
    private:
      int64 acceptedEpoch;
      int64 currentEpoch;
      PeerState state;

      QuorumCnxMgr            qcnxMgr;

      QuorumPeerConfig* peerConfig;
      Election* electionAlg;
      Leader* leader;
      Follower* follower;
      ZabDBInterface* zDb;

      Lock stateLock;
      Lock voteLock;
      Vote currentVote;

      int64 start_el;
      int64 end_el;
  };

}
;

#endif /* QUORUM_PEER_H_ */
