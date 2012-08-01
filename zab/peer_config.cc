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

#include "peer_config.h"
#include "election_strategy.h"
#include "base/logging.h"
#include <string.h>
#include <stdlib.h>
#include <fstream>

namespace ZABCPP {
  const static int MAX_CFG_LINE = 256;
  const static int DEFAULT_TICK_TIME = 3000;
  const static int DEFAULT_MAX_CLIENTS = 60;
  const static int DEFAULT_CL_PORT = 2182;
  const static int DEFAULT_EL_PORT = 4181;
  const static int DEFAULT_ZABDB_PORT = 6379;
  QuorumPeerConfig::QuorumPeerConfig()
      :
          tickTime(DEFAULT_TICK_TIME),
          maxClientCnxns(DEFAULT_MAX_CLIENTS),
          electionAlg(FASTPAXOS_TCP),
          electionPort(DEFAULT_EL_PORT),
          clientPort(DEFAULT_CL_PORT),
          zabDBPort(DEFAULT_ZABDB_PORT),
          serverId(0),
          numGroups(0),
          quorumVerifier(NULL) {

  }

  void QuorumPeerConfig::parseCfg(const string& cfg_name) {
    i_parseCfg(cfg_name);
    for (KVMap::iterator iter = i_kvMap.begin(); iter != i_kvMap.end(); iter++) {
      DEBUG("item:"<<iter->first<<"="<<iter->second);
      if (iter->first.compare("tickTime") == 0) {
        tickTime = atoi(iter->second.data());
      } else if (iter->first.compare("initLimit") == 0) {
        initLimit = atoi(iter->second.data());
      } else if (iter->first.compare("syncLimit") == 0) {
        syncLimit = atoi(iter->second.data());
      } else if (iter->first.compare("clientPort") == 0 ){
        clientPort = atoi(iter->second.data());
      } else if (iter->first.compare("zabDBPort") == 0) {
        zabDBPort = atoi(iter->second.data());
      } else if (iter->first.compare("electionAlg") == 0) {
        electionAlg = atoi(iter->second.data());
      } else if (iter->first.compare("peerType") == 0) {
        //todo add observer support
      } else if (iter->first.compare("dataDir") == 0) {
        dataDir = iter->second;
      } else if (iter->first.compare("dataLogDir") == 0) {
        dataLogDir = iter->second;
      } else if (iter->first.find("server.") != string::npos) {
        int dot = iter->first.find_first_of(".");
        long sid = atol(iter->first.substr(dot + 1).data());
        int delim_pos = -1;
        string serverStr = iter->second;
        vector<string> fields;
        while ((delim_pos = serverStr.find(':')) != -1) {
          fields.push_back(serverStr.substr(0, delim_pos));
          serverStr = serverStr.substr(delim_pos + 1);
        }
        if (!serverStr.empty()) {
          fields.push_back(serverStr);
        }
        int field_count = fields.size();
        if (field_count == 2) {
          QuorumServer serverInfo;
          serverInfo.id = sid;
          serverInfo.addr = fields[0];
          serverInfo.peerPort = atoi(fields[1].data());
          servers[sid] = serverInfo;
        } else if (field_count == 3) {
          QuorumServer serverInfo;
          serverInfo.id = sid;
          serverInfo.addr = fields[0];
          serverInfo.peerPort = atoi(fields[1].data());
          serverInfo.electionPort = atoi(fields[2].data());
          servers[sid] = serverInfo;
        } else if (field_count == 4) {
          QuorumServer serverInfo;
          serverInfo.id = sid;
          serverInfo.addr = fields[0];
          serverInfo.peerPort = atoi(fields[1].data());
          serverInfo.electionPort = atoi(fields[2].data());
          servers[sid] = serverInfo;
        } else {
          ERROR("Could parse server field:"<<serverStr);
        }
      } else {
        //todo add more configuration items parser here
        WARN("un-support configuration item:"<<iter->first<<"="<<iter->second);
      }
    }

    if (dataDir.empty()) {
      ERROR("dataDir is not set");
      return;
    }

    if (dataLogDir.empty())
      dataLogDir = dataDir;

    // read my id from file
    string myidFile = dataDir + "/myid";
    fstream file(myidFile.data(), ios::in);
    if (file.fail()) {
      ERROR("could not read myid from "<<myidFile);
      return;
    }
    char buffer[MAX_CFG_LINE];
    memset(buffer, 0, sizeof(buffer));
    file.getline(buffer, sizeof(buffer));
    serverId = strtoll(buffer, NULL, 10);
    file.close();
    DEBUG("myid "<<serverId);

    //todo, current we only support Maj do not support group
    quorumVerifier = new QuorumMaj(servers.size());
  }

  void QuorumPeerConfig::i_parseCfg(const string& cfg_name) {
    char *temp = NULL;
    char *value = NULL;
    char buffer[MAX_CFG_LINE];

    fstream file(cfg_name.data(), ios::in);
    if (file.fail()) {
      ERROR("could not open configuration file "<<cfg_name);
      return;
    }
    while (file.good()) {
      memset(buffer, 0, sizeof(buffer));
      file.getline(buffer, sizeof(buffer));
      if ((temp = strchr(buffer, '#')))
        *temp = '\0';
      if ((temp = strchr(buffer, '\n')))
        *temp = '\0';
      if ((temp = strchr(buffer, '\r')))
        *temp = '\0';
      if ((buffer[0]) && (value = strchr(buffer, '='))) {
        *value++ = '\0';
        i_kvMap[buffer] = value;
      }
    }
    file.close();
  }

  QuorumPeerConfig::~QuorumPeerConfig() {
    if (quorumVerifier != NULL) {
      delete quorumVerifier;
      quorumVerifier = NULL;
    }
  }
}
