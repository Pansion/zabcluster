/*
 * test_lezk.cxx
 *
 *  Created on: Jun 25, 2012
 *      Author: pchen
 */

#include "quorum_peer.h"
#include "election_cnxmgr.h"
#include "election_protocol.h"
#include "election_strategy.h"
#include "base/logging.h"
#include "stdio.h"
#include <sys/time.h>
using namespace ZABCPP;
using namespace std;
int64 getTickCount(void) {
  struct timeval t;
  timerclear(&t);
  gettimeofday(&t, NULL);
  return 1000000 * t.tv_sec + t.tv_usec;
}

int main(int argc, char *argv[]) {
log_instance->setLogLevel(INFO_LOG_LEVEL);
//  log_instance->setLogLevel(WARN_LOG_LEVEL);
  string cfg = "zoo.cfg";
  if (1 < argc) {
    cfg = argv[1];
  }
  INFO("begin test leader election, using configuration file: "<<cfg);
  QuorumPeerConfig peerConfig;
  peerConfig.parseCfg(cfg);
  //QuorumCnxMgr cnxMgr(&peerConfig);
  QuorumPeer peer(&peerConfig);
  peer.Start();
  while (1) {
    //todo send information to whiteboard
    sleep(10);
  }
  // cnxMgr.Start();
//  Election*  el= ElectionStrategyFactory::getElectionStrategy(FASTPAXOS_TCP,  &peer, &cnxMgr);
//
//  int64 el_ts = getTickCount();
//  Vote v = el->lookForLeader();
//  el_ts = getTickCount() - el_ts;
//  peer.setAcceptedEpoch(v.peerEpoch);
//  peer.setCurretnEpoch(v.peerEpoch);
//  cnxMgr.Stop();
//  delete el;
//  INFO("end testing: election took:"<<el_ts/1000<<" ms for "<<peerConfig.servers.size()<<" nodes "<<"leader:"<<v.id<<" Epoch:"<<v.peerEpoch);
  return 0;
}
