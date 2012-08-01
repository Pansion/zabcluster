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



#include "client_handler.h"
#include "quorum_server.h"
#include "base/logging.h"
#include <set>

using namespace std;



namespace ZABCPP{

  //redis read command set
  typedef set<string> CmdSet;
  static CmdSet  RSet;

  ClientHandlerRedis::ClientHandlerRedis(){
    RSet.insert("GET");
    RSet.insert("HGETALL");
    RSet.insert("QUIT");
    RSet.insert("STRLEN");
    RSet.insert("HVALS");
    RSet.insert("CONFIG GET");
    RSet.insert("TIME");
    RSet.insert("INFO");
    RSet.insert("DEBUG OBJECT");
    RSet.insert("TTL");
    RSet.insert("KEYS");
    RSet.insert("DEBUG SEGFAULT");
    RSet.insert("TYPE");
    RSet.insert("LASTSAVE");
    RSet.insert("LLEN");
    RSet.insert("SCARD");
    RSet.insert("ZCARD");
    RSet.insert("DUMP");
    RSet.insert("EXISTS");
    RSet.insert("ZRANK");
    RSet.insert("SELECT");
    RSet.insert("MGET");
    RSet.insert("MONITOR");
    RSet.insert("ZREVRANGE");
    RSet.insert("GETBIT");
    RSet.insert("ZREVRANGEBYSCORE");
    RSet.insert("ZREVRANK");
    RSet.insert("ZSCORE");
    RSet.insert("SISMEMBER");
    RSet.insert("HEXISTS");
    RSet.insert("HGET");
  }

  ClientHandlerRedis::~ClientHandlerRedis() {
    cleanupClients();
  }

  void ClientHandlerRedis::HandleIncomingRequest(int client_fd, const char * buf, int buf_len){
    redisClient* c = getClient(client_fd);
    if (c != NULL){
      DEBUG("buf from client "<<buf<<" new buf len "<<buf_len);
      c->querybuf = sdscatlen(c->querybuf, buf, buf_len);
      while(sdslen(c->querybuf)) {
        if (processInputBuffer(c) == 0){
          Request req;
          req.clientfd = client_fd;
          req.client_type = Request::CT_REDIS;
          req.type = getCmdType(c->cmd);
          req.zxid = -1;
          req.sid = 0;
          req.request.WriteBytes(c->rawreq, sdslen(c->rawreq));
          DEBUG("submit new request len "<<req.request.Length());
          if (zabServer != NULL) {
            zabServer->SubmitRequest(req);
          }
          resetClient(c);
        } else {
          break;
        }
      }
    }
  }

  void ClientHandlerRedis::HandleClientShutdown(int client_fd) {
    ClientMap::iterator iter = clientMap.find(client_fd);
    if (iter != clientMap.end()) {
      redisClient * c = iter->second;
      if (c != NULL) {
        freeClient(c);
      }
      clientMap.erase(iter);
    }
  }

  redisClient* ClientHandlerRedis::getClient(int client_fd){
    redisClient * c = NULL;
    ClientMap::iterator iter = clientMap.find(client_fd);
    if (iter == clientMap.end()){
      c = createClient(client_fd);
      clientMap.insert(pair<int, redisClient*>(client_fd, c));
    } else {
      c = iter->second;
    }
    return c;
  }

  int  ClientHandlerRedis::getCmdType(sds cmd){
    int ret = Request::REQ_W;
    sdstoupper(cmd);
    if (RSet.find(cmd) != RSet.end()) {
      ret = Request::REQ_R;
    }
    return ret;
  }

  void ClientHandlerRedis::cleanupClients(){
    for(ClientMap::iterator iter = clientMap.begin(); iter != clientMap.end(); iter ++){
      redisClient* c = iter->second;
      if (c != NULL){
        freeClient(c);
        iter->second = NULL;
      }
    }
    clientMap.clear();
  }
}
