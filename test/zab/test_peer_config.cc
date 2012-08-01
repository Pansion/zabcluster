/*
 * test_peer_config.cxx
 *
 *  Created on: Jun 15, 2012
 *      Author: pchen
 */

#include "peer_config.h"
#include "base/logging.h"

using namespace ZABCPP;
int main(int argc, char **argv) {

  INFO("begin test peer config");
  QuorumPeerConfig peerConfig;
  peerConfig.parseCfg("zoo.cfg");
  for (QuorumServerMap::iterator iter = peerConfig.servers.begin(); iter != peerConfig.servers.end(); iter++) {
    INFO(
        "server id:"<<iter->first<<" addr:"<<iter->second.addr<< " peerPort:"<<iter->second.peerPort<< " electionPort:"<<iter->second.electionPort<< " id:"<<iter->second.id<< " leaner type:"<<iter->second.type);
  }
  INFO("end testing");
  return 0;
}
