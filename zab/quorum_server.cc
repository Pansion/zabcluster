//Copyright (c) 2012, pansion
//All rights reserved.
//
//Redistribution and use in source and binary forms, with or without
//modification, are permitted provided that the following conditions are met:
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//    * Neither the name of the pansion nor the
//      names of its contributors may be used to endorse or promote products
//      derived from this software without specific prior written permission.
//
//THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
//ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//DISCLAIMED. IN NO EVENT SHALL pansion BE LIABLE FOR ANY
//DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


#include "quorum_server.h"
#include "base/logging.h"
#include "zab_utils.h"
namespace ZABCPP {
  ZabQuorumServer::ZabQuorumServer(int64 sid, RequestProcessor* p, ZabDBInterface *z)
      : Thread("Quorum Server")
  , requestProcessor(p)
  , zDb(z)
  , sid_(sid)
  , zxid_(0)
  , weHaswork(false, false){

  }

  ZabQuorumServer::~ZabQuorumServer() {

  }
  //This interface should be called when get requst from client
  //use reference here sine leader will change zxid inside request
  void ZabQuorumServer::SubmitRequest(const Request& req) {
    if (req.type == Request::REQ_W){
      requestProcessor->ProcessRequest(req);
    } else {
      Request * r = new Request();
      r->clientfd = req.clientfd;
      r->type = req.type;
      r->client_type = req.client_type;
      r->sid = sid_;
      r->zxid = req.zxid;
      r->request.WriteBytes(req.request.Data(), req.request.Length());
      zDb->ProcessRequest(r);
    }
  }

  //This interface should be called when follower get new proposal from leader
  void ZabQuorumServer::SubmitProposal(Request * req) {
    addProposal(req->zxid, req);
  }

  //This interface should be called when follower get commit from leader
  void ZabQuorumServer::CommitProposal(int64 zxid) {
    Request * q = getProposal(zxid);
    addCommittedProposal(q);
    weHaswork.Signal();
  }

  void ZabQuorumServer::ShuttingDown() {
    INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
    weHaswork.Signal();
  }

  void ZabQuorumServer::Init() {

  }

  void ZabQuorumServer::Run() {
    while (!stopping_) {
      Request* req = NULL;
      while ((req = getCommittedProposal()) != NULL) {
        INFO("Committed proposal zxid "<<ZxidUtils::HexStr(req->zxid)<<" originated from "<<ZxidUtils::HexStr(req->sid));
        zDb->ProcessRequest(req);
      }
      weHaswork.Wait();

      if (stopping_) {
        return;
      }
    }
  }

  void ZabQuorumServer::CleanUp() {
    clearPendingProposal();
    clearCommittedProposal();
  }

  void ZabQuorumServer::addProposal(int64 zxid, Request * req) {
    AutoLock guard(proposalLock);
    PendingProposal::iterator iter = pendingProposal.find(zxid);
    if (iter != pendingProposal.end()) {
      WARN("proposal "<<ZxidUtils::HexStr(zxid)<<" already exists");
    } else {
      pendingProposal.insert(pair<int64, Request*>(zxid, req));
    }
  }
  //get and erase proposal by zxid;
  Request* ZabQuorumServer::getProposal(int64 zxid) {
    Request * req = NULL;
    AutoLock guard(proposalLock);
    PendingProposal::iterator iter = pendingProposal.find(zxid);
    if (iter != pendingProposal.end()) {
      req = iter->second;
      pendingProposal.erase(iter);
    } else {
      WARN("proposal "<<ZxidUtils::HexStr(zxid)<<" already committed");
    }
    return req;
  }

  void ZabQuorumServer::addCommittedProposal(Request * req) {
    AutoLock guard(committedLock);
    committedProposal.push_back(req);
  }

  Request * ZabQuorumServer::getCommittedProposal() {
    Request * q = NULL;
    AutoLock guard(committedLock);
    if (!committedProposal.empty()) {
      q = committedProposal.front();
      committedProposal.pop_front();
    }
    return q;
  }

  void ZabQuorumServer::clearPendingProposal() {
    AutoLock guard(proposalLock);
    for (PendingProposal::iterator iter = pendingProposal.begin(); iter != pendingProposal.end(); iter++) {
      Request * q = iter->second;
      if (q != NULL) {
        delete q;
      }
    }
    pendingProposal.clear();
  }

  void ZabQuorumServer::clearCommittedProposal() {
    AutoLock guard(committedLock);
    for (CommittedProposal::iterator iter = committedProposal.begin(); iter != committedProposal.end(); iter++) {
      Request * q = *iter;
      if (q != NULL) {
        delete q;
      }
    }
    committedProposal.clear();
  }
}

