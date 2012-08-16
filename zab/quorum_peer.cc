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

#include "quorum_peer.h"
#include "zab_utils.h"
#include "base/logging.h"
#include "zabdb_redis.h"
#include <sys/time.h>
#include <fstream>

using namespace std;
namespace ZABCPP {
  static const char* CURRENT_EPOCH_FILENAME = "currentEpoch";

  static const char* ACCEPTED_EPOCH_FILENAME = "acceptedEpoch";

  int64 getTickCount(void) {
    struct timeval t;
    timerclear(&t);
    gettimeofday(&t, NULL);
    return 1000000 * t.tv_sec + t.tv_usec;
  }

  QuorumPeer::QuorumPeer(QuorumPeerConfig * cfg)
      :
          Thread("Quorum Peer"),
          acceptedEpoch(0),
          currentEpoch(0),
          state(LOOKING),
          qcnxMgr(cfg),
          peerConfig(cfg),
          electionAlg(NULL),
          leader(NULL),
          follower(NULL),
          zabDB(cfg->serverId){

  }

  QuorumPeer::~QuorumPeer() {

  }

  int64 QuorumPeer::ReadInt64FromFile(const string& fn) {
    fstream file(fn.data(), ios::in);
    if (file.fail()) {
      ERROR("Could not read file "<<fn);
      return 0;
    }
    char buf[32];
    memset(buf, 0, 32);
    file.getline(buf, 32);
    file.close();
    return strtoll(buf, NULL, 10);
  }

  void QuorumPeer::WriteInt64ToFile(const string &fn, int64 val) {
    fstream file(fn.data(), ios::out);
    if (file.fail()) {
      ERROR("Could not write file "<<fn);
      return;
    }
    char buf[32];
    int len = snprintf(buf, 32, "%lld", val);
    file.write(buf, len);
    file.flush();
    file.close();
  }

  int64 QuorumPeer::getCurrentEpoch() {
    if (currentEpoch == 0) {
      string fn = peerConfig->dataDir + "/" + CURRENT_EPOCH_FILENAME;
      currentEpoch = ReadInt64FromFile(fn);
    }
    return currentEpoch;
  }

  void QuorumPeer::setCurretnEpoch(int64 val) {
    currentEpoch = val;
    string fn = peerConfig->dataDir + "/" + CURRENT_EPOCH_FILENAME;
    WriteInt64ToFile(fn, val);
  }

  int64 QuorumPeer::getAcceptedEpoch() {
    if (acceptedEpoch == 0) {
      string fn = peerConfig->dataDir + "/" + ACCEPTED_EPOCH_FILENAME;
      acceptedEpoch = ReadInt64FromFile(fn);
    }
    return acceptedEpoch;
  }

  void QuorumPeer::setAcceptedEpoch(int64 val) {
    acceptedEpoch = val;
    string fn = peerConfig->dataDir + "/" + ACCEPTED_EPOCH_FILENAME;
    WriteInt64ToFile(fn, val);
  }

  int64 QuorumPeer::getLastZxid() {
    return zabDB.GetLastProcessedZxid();
  }

  void QuorumPeer::Init() {
    if (electionAlg == NULL) {
      electionAlg = ElectionStrategyFactory::getElectionStrategy(FASTPAXOS_TCP, this, &qcnxMgr);
    }

    zabDB.SetServerAddr("127.0.0.1", peerConfig->zabDBPort);
    zabDB.Startup();

    leader = new Leader(this, &zabDB);
    follower = new Follower(this, &zabDB);

    qcnxMgr.Start();
  }

  void QuorumPeer::Run() {
    while (!stopping_) {
      switch (getPeerState()) {
        case LOOKING: {
          INFO("Looking...");
          if (electionAlg == NULL) {
            FATAL("could not init election, quit");
            return;
          }
          start_el = getTickCount();
          setCurrentVote(electionAlg->lookForLeader());
          Vote v = getCurrentVote();
          if (v.id != -1) {
            end_el = getTickCount();
            INFO("My Id: "<<getId() <<" Leader election took "<<(end_el - start_el)/1000<<" ms"
                <<" my role:"<<getPeerState()
                <<" Leader Info:"<<v.id<<" (id) "
                <<ZxidUtils::HexStr(v.zxid)<<" (zxid) "
                <<ZxidUtils::HexStr(v.peerEpoch) <<" (peerEpoch)");
          } else {
            ERROR("could not find out a leader");
          }
          break;
        }
        case FOLLOWING: {
          follower->FollowLeader();
          INFO("Failed on following leader, looking for new one");
          setPeerState(LOOKING);
          break;
        }
        case LEADING: {
          leader->Lead();
          INFO("Failed on leading quorum, looking for new one");
          setPeerState(LOOKING);
          break;
        }
        default: {
          ERROR("unknown peer state:"<<getPeerState());
          return;
        }
      }
    }
  }

  void QuorumPeer::CleanUp() {
    INFO("Stop quorum cnx mgr");
    qcnxMgr.Stop();

    INFO("Deleting election instance");
    if (electionAlg != NULL) {
      delete electionAlg;
    }

    INFO("Deleting leader instance");
    if (leader != NULL) {
      delete leader;
      leader = NULL;
    }

    INFO("Deleting follower instance");
    if (follower != NULL) {
      delete follower;
      follower = NULL;
    }

    INFO("Shutdown ZAB db instance");
    zabDB.Shutdown();
  }

  void QuorumPeer::ShuttingDown() {
    if (electionAlg != NULL) {
      electionAlg->Shutdown();
    }

    if (leader != NULL) {
      leader->Shutdown();
    }

    if (follower != NULL) {
      follower->Shutdown();
    }
  }
}

