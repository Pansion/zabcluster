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

#ifndef QUORUM_SERVER_H_
#define QUORUM_SERVER_H_
#include "base/basictypes.h"
#include "base/thread.h"
#include "base/waitable_event.h"
#include "quorum_packet.h"
#include "zabdb.h"
#include <map>

using namespace std;
namespace ZABCPP {
  class ZabDBInterface;
  class Request {
    public:
      Request() {

      }

      ~Request() {

      }

      int32  clientfd;
      //request type now we only have Write and Read
      enum {
        REQ_R = 0,
        REQ_W
      };

      int32 type;

      //storage type
      //we can use this field to decide request content
      //e.g if it was REDIS, request should be redis client protocol
      //we need to define related request handler

      enum {
        CT_REDIS = 0,
        CT_LEVELDB
      };
      int32 client_type;

      //field should be filled by leader when process request
      int64 zxid;

      //used to decide if which server to send back response
      int64 sid;

      //request sent by client
      //its content could be different if we support different storage(redis/leveldb/bdb)
      ByteBuffer request;
  };

  //interface for leader /follower to inherit
  class RequestProcessor {
    public:
      virtual void ProcessRequest(const Request& req) = 0;
    protected:
      RequestProcessor() {
      }
      ;
      virtual ~RequestProcessor() {
      }
      ;
  };

  //NOTE: implementation difference with zookeeper
  // zookeeper have punch of processor which pass request on a chain
  // it was written in java which passing object was not a big deal
  // but in C++, pass object could be a headache.
  // if we use pointer instead, how could we handle memory deletion???
  // Actually the request buffer will be copied to leader by network
  // after leader get request, it will copy the request in quorum packet payload and
  // send it to all followers. That means we will get it back by receiving
  // one of the proposal quorum packet.
  // We only keep a list of pending proposal sent by leader
  // and we only have one thread handling quorum packet
  // that should be enough to finish zab phase 3 procedure
  // The responsibility of quorum server should be
  // 1.get request from client and submit to leader
  // 2.get proposal from leader
  // 3.commit proposal and process request in local storage db
  class ZabQuorumServer: public Thread {
    public:
      ZabQuorumServer(int64 sid, RequestProcessor*, ZabDBInterface*);

      ~ZabQuorumServer();
      //This interface should be called when get requst from client
      //use reference here sine leader will change zxid inside request
      void SubmitRequest(const Request& req);

      //This interface should be called when follower get new proposal from leader
      // we only store request part
      void SubmitProposal(Request * req);

      //This interface should be called when follower get commit from leader
      void CommitProposal(int64 zxid);

      void SetZxid(int64 zxid) {
        AutoLock guard(zxidLock);
        zxid_ = zxid;
      }

      int64 GetZxid() {
        AutoLock guard(zxidLock);
        return zxid_;
      }

      int64 GetNextZxid() {
        AutoLock guard(zxidLock);
        zxid_++;
        return zxid_;
      }

      int64 GetLastProcessedZxid() {
        AutoLock guard(zxidLock);
        return zxid_;
      }
    protected:
      virtual void Init();
      virtual void Run();
      virtual void CleanUp();
      virtual void ShuttingDown();

    private:
      void addProposal(int64 zxid, Request * qp);
      //get and erase proposal by zxid;
      Request* getProposal(int64 zxid);

      void addCommittedProposal(Request * qp);
      Request * getCommittedProposal();

      void clearPendingProposal();
      void clearCommittedProposal();

    protected:
      typedef map<int64, Request*> PendingProposal;
      typedef list<Request*> CommittedProposal;

      PendingProposal pendingProposal;
      CommittedProposal committedProposal;
      RequestProcessor* requestProcessor;
      ZabDBInterface    *zDb;
      int64 sid_;
      int64 zxid_;
      Lock zxidLock;
      Lock committedLock;
      Lock proposalLock;

      WaitableEvent weHaswork;
      DISALLOW_COPY_AND_ASSIGN(ZabQuorumServer);
  };
}

#endif /* QUORUM_SERVER_H_ */
