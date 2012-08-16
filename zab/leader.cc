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

#include "leader.h"
#include "zab_utils.h"
#include "zab_constant.h"
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
namespace ZABCPP {

  //----------------------LearnerCnxMgr----------------
  LearnerCnxMgr::LearnerCnxMgr(Leader *ldr)
      : Thread("Learner Connection Mgr"), leader(ldr), ebase(NULL) {
    wakeup_pipe[PIPE_OUT] = -1;
    wakeup_pipe[PIPE_IN] = -1;
    msg_pipe[PIPE_OUT] = -1;
    msg_pipe[PIPE_IN] = -1;
  }

  LearnerCnxMgr::~LearnerCnxMgr() {

  }

  void LearnerCnxMgr::OnLibEventNotification(evutil_socket_t fd, short what, void *arg) {
    LearnerCnxMgr* lcm = static_cast<LearnerCnxMgr*>(arg);
    if (what & EV_READ) {
      lcm->onRecvMsg(fd);
    }
  }
  void LearnerCnxMgr::OnLibEventListenerNotify(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address,
      int socklen, void *ctx) {
    LearnerCnxMgr * l = static_cast<LearnerCnxMgr*>(ctx);
    l->onAccept(fd);
  }

  void LearnerCnxMgr::OnLibEventListenerError(struct evconnlistener *listener, void *ctx) {
    int err = EVUTIL_SOCKET_ERROR();
    ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") on the listener.");
    LearnerCnxMgr * l = static_cast<LearnerCnxMgr*>(ctx);
    l->onError();
  }

  void LearnerCnxMgr::OnWakeup(evutil_socket_t fd, short what, void *arg) {
    INFO("Waked up and break event loop");
    char c = 0;
    read(fd, &c, 1);
    struct event_base* b = (struct event_base*) arg;
    event_base_loopbreak(b);
  }

  void LearnerCnxMgr::OnSendingMsgReady(evutil_socket_t fd, short what, void *arg) {
    LearnerCnxMgr* lcm = static_cast<LearnerCnxMgr*>(arg);
    lcm->onMsgReady(fd);
  }

  void LearnerCnxMgr::msgReady() {
    if (msg_pipe[PIPE_IN] != -1) {
      char c = 0;
      write(msg_pipe[PIPE_IN], &c, 1);
    }
  }

  //have lock inside
  void LearnerCnxMgr::pushBack_i(QuorumPacket* packet) {
    AutoLock guard(sendQueueLock);
    i_sendQueue.push_back(packet);
  }

  QuorumPacket* LearnerCnxMgr::popFront_i() {
    QuorumPacket * p = NULL;
    AutoLock guard(sendQueueLock);
    if (!i_sendQueue.empty()) {
      p = i_sendQueue.front();
      i_sendQueue.pop_front();
    }
    return p;
  }
  void LearnerCnxMgr::SendQp(QuorumPacket* packet) {
    pushBack_i(packet);
    msgReady();
  }

  void LearnerCnxMgr::PingFollowers() {
    QuorumPacket * p = new QuorumPacket();
    p->type = PING;
    p->zxid = leader->GetProposedEpoch();
    SendQp(p);
  }

  void LearnerCnxMgr::GetSyncedFollowers(set<int64>& syncSet) {
    if (stopping_) {
      return;
    }

    int64 leaderTick = leader->GetTick();
    int syncLimit = leader->GetSyncLimit();
    for (Sock2FollowerMap::iterator iter = i_sock2FollowerMap.begin(); iter != i_sock2FollowerMap.end(); iter++) {
      if ((leaderTick - syncLimit) <= iter->second.tickOfLastAck) {
        syncSet.insert(iter->second.sid);
      }
    }
  }

  void LearnerCnxMgr::addNewLearner(int sock) {
    INFO("Got new learner connection on sock "<<sock);
    EventMap::iterator iter = i_eventMap.find(sock);
    if (iter == i_eventMap.end()) {
      struct event * e = event_new(ebase, sock, EV_READ | EV_PERSIST, &LearnerCnxMgr::OnLibEventNotification, this);
      if (e != NULL) {
        event_add(e, NULL);
        i_eventMap.insert(pair<int, struct event*>(sock, e));
        FollowerInfo f;
        i_sock2FollowerMap.insert(pair<int, FollowerInfo>(sock, f));
      } else {
        ERROR("could not create event for sock "<<sock);
      }
    } else {
      INFO("connection with sock "<<sock<<" already existed");
    }
  }

  void LearnerCnxMgr::removeLearner(int sock) {
    INFO("Try to remove learner connection on sock "<<sock);
    EventMap::iterator iter = i_eventMap.find(sock);
    if (iter != i_eventMap.end()) {
      struct event *e = iter->second;
      event_del(e);
      event_free(e);
      i_eventMap.erase(iter);
      i_sock2FollowerMap.erase(sock);
      INFO("Removed event on sock "<<sock);
    }
  }

  string* LearnerCnxMgr::getSockBuf(int sock) {
    string* buf = NULL;
    SockBufMap::iterator iter = i_sockBufMap.find(sock);
    if (iter != i_sockBufMap.end()) {
      buf = iter->second;
    } else {
      buf = new string();
      i_sockBufMap.insert(pair<int, string*>(sock, buf));
    }
    return buf;
  }

  void LearnerCnxMgr::eraseSockBuf(int sock) {
    SockBufMap::iterator iter = i_sockBufMap.find(sock);
    if (iter != i_sockBufMap.end()) {
      string * b = iter->second;
      delete b;
      i_sockBufMap.erase(iter);
    }
  }

  ZabLeaderState LearnerCnxMgr::getState(int sock) {
    ZabLeaderState ret = ZAB_WAITING_EPOCH;
    Sock2FollowerMap::iterator iter = i_sock2FollowerMap.find(sock);
    if (iter != i_sock2FollowerMap.end()) {
      ret = iter->second.zabState;
    }
    return ret;
  }

  void LearnerCnxMgr::setState(int sock, ZabLeaderState s) {
    Sock2FollowerMap::iterator iter = i_sock2FollowerMap.find(sock);
    if (iter != i_sock2FollowerMap.end()) {
      iter->second.zabState = s;
    } else {
      ERROR("Could set state for sock "<<sock);
    }
  }

  void LearnerCnxMgr::setSid(int sock, int64 sid) {
    Sock2FollowerMap::iterator iter = i_sock2FollowerMap.find(sock);
    if (iter != i_sock2FollowerMap.end()) {
      iter->second.sid = sid;
    } else {
      ERROR("Could set state for sock "<<sock);
    }
  }

  int64 LearnerCnxMgr::getSid(int sock) {
    int64 sid = 0;
    Sock2FollowerMap::iterator iter = i_sock2FollowerMap.find(sock);
    if (iter != i_sock2FollowerMap.end()) {
      sid = iter->second.sid;
    }
    return sid;
  }

  void LearnerCnxMgr::setLastTick(int sock) {
    Sock2FollowerMap::iterator iter = i_sock2FollowerMap.find(sock);
    if (iter != i_sock2FollowerMap.end()) {
      iter->second.tickOfLastAck = leader->GetTick();
    }
  }

  int LearnerCnxMgr::sendToOne(int fd, QuorumPacket* p) {
    ByteBuffer out;
    out.WriteInt32(sizeof(p->type) + sizeof(p->zxid) + p->payload.Length());
    out.WriteInt32(p->type);
    out.WriteInt64(p->zxid);
    out.WriteBytes(p->payload.Data(), p->payload.Length());

    int sendNum = send(fd, out.Data(), out.Length(), 0);
    if (sendNum < 0) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Error on sending message "<<ZxidUtils::HexStr(p->zxid)
      <<" to follower on sock "<<fd
      <<", error " <<err<<" ("<<evutil_socket_error_to_string(err)<<")");
    }
    return sendNum;
  }

  void LearnerCnxMgr::sendToAll(QuorumPacket* p) {
    for (Sock2FollowerMap::iterator iter = i_sock2FollowerMap.begin();
        iter != i_sock2FollowerMap.end();
        iter++) {
      if (iter->second.sid != 0) {
        if (sendToOne(iter->first, p) < 0){
          ERROR("Could not send message to follower "<<iter->second.sid);
        }
      }
    }
  }

  void LearnerCnxMgr::processSendQueue() {
    QuorumPacket * p = NULL;
    while ((p = popFront_i()) != NULL) {
      sendToAll(p);
      delete p;
    }
  }

  void LearnerCnxMgr::processQp(int fd, QuorumPacket& qp) {
    //todo, we need to wait for quorum peer to sync with us
    // during the sync, we also need to queue new proposals
    //after NEWLEADER ack from quorum received then process those pending proposals
    //below switch/case logic was ugly and need re-factory later after database and zab phase3 was added in
    DEBUG("we get new quorum packet with type "<<qp.type<<" from sock "<<fd);
    switch (getState(fd)) {
      case ZAB_WAITING_EPOCH: {
        if (qp.type != FOLLOWERINFO) {
          ERROR("First packet from learner should be "<<FOLLOWERINFO<<" but we got "<<qp.type<<" close connection");
          removeLearner(fd);
          eraseSockBuf(fd);
          close(fd);
          return;
        }
        int64 learnerSid = 0;
        if (qp.payload.ReadInt64(learnerSid)) {
          i_sid2SockMap.insert(pair<int64, int>(learnerSid, fd));
          INFO("New Follower sid:"<<learnerSid);
          setSid(fd, learnerSid);

          ZabLeaderState zl = leader->GetLeaderState();
          if (zl < ZAB_NEW_EPOCH) {
            INFO("Leader was waiting for epoch, add my information");
            leader->AddConnectingFollowers(learnerSid, ZxidUtils::GetEpochFromZxid(qp.zxid));
            setState(fd, ZAB_NEW_EPOCH);
          } else {
            INFO("Leader have proposed NEW EPOCH, send LEADERINFO");
            QuorumPacket qp;
            qp.type = LEADERINFO;
            qp.zxid = ZxidUtils::MakeZxid(leader->GetProposedEpoch(), 0);
            sendToOne(fd, &qp);
            setState(fd, ZAB_WAITING_EPCH_ACK);
          }
        } else {
          ERROR("failed to get sid");
          return;
        }
        break;
      }
      case ZAB_NEW_EPOCH:
      case ZAB_WAITING_EPCH_ACK: {
        ZabLeaderState zl = leader->GetLeaderState();
        if (zl < ZAB_NEW_LEADER) {
          if (qp.type != ACKEPOCH) {
            ERROR("we supposed to get "<<ACKEPOCH<<" but actually got "<<qp.type);
            return;
          }

          int64 sid = getSid(fd);
          if (sid != 0) {
            int64 peerEpoch = 0;
            if (qp.payload.ReadInt64(peerEpoch)) {
              StateSummary ss(peerEpoch, qp.zxid);
              leader->AddEpochAck(sid, ss);
              setState(fd, ZAB_NEW_LEADER);
              return;
            }
          } else {
            ERROR("Unknown follower on sock "<<fd);
            return;
          }
        } else {
          INFO("NEW Leader announced, send NEWLEADER");
          QuorumPacket qp;
          qp.type = NEWLEADER;
          qp.zxid = ZxidUtils::MakeZxid(leader->GetProposedEpoch(), 0);
          sendToOne(fd, &qp);
          setState(fd, ZAB_NEW_LEADER_ACK);
        }
        break;
      }
      case ZAB_NEW_LEADER:
      case ZAB_NEW_LEADER_ACK: {
        ZabLeaderState zl = leader->GetLeaderState();
        if (zl < ZAB_LEADER_COMMIT) {
          if (qp.type != ACK) {
            ERROR("we supposed to get "<<ACK<<" but actually got "<<qp.type);
            return;
          }
          int64 sid = getSid(fd);
          INFO("We got NEWLEADER ACK from server "<<sid);
          if (sid != 0) {
            leader->AddNewLeaderAct(sid);
            setState(fd, ZAB_LEADER_COMMIT);
          } else {
            ERROR("Unknown follower on sock "<<fd);
            return;
          }
        } else {
          setState(fd, ZAB_LEADER_COMMIT);
          INFO("leader was already commited by quorum, send commit packet to new follower");
          QuorumPacket qp;
          qp.type = COMMIT;
          qp.zxid = ZxidUtils::MakeZxid(leader->GetProposedEpoch(), 0);
          sendToOne(fd, &qp);
        }
        break;
      }
      case ZAB_LEADER_COMMIT: {
        //update last tick number
        setLastTick(fd);
        switch (qp.type) {
          case REQUEST: {
            Request req;
            qp.payload.ReadInt32(req.clientfd);
            qp.payload.ReadInt32(req.type);
            qp.payload.ReadInt32(req.client_type);
            qp.payload.ReadInt64(req.zxid);
            qp.payload.ReadInt64(req.sid);
            req.request.WriteBytes(qp.payload.Data(), qp.payload.Length());

            INFO("We got REQUEST from server "<<req.sid);
            leader->ProcessRequest(req);
            break;
          }
          case PROPOSAL: {
            break;
          }
          case ACK: {
            int64 sid = getSid(fd);
            INFO("We got PROPOSAL "<<ZxidUtils::HexStr(qp.zxid)<<" ACK from server "<<sid);
            leader->ProcessAck(sid, qp.zxid);
            break;
          }
          case PING: {
            break;
          }
          default: {
            break;
          }
        }
        break;
      }
      default: {
        INFO("Not matched state ");
        break;
      }
    }
  }

  void LearnerCnxMgr::onAccept(int sock) {
    int flag = 1;
    setsockopt(sock, /* socket affected */
    IPPROTO_TCP, /* set option at TCP level */
    TCP_NODELAY, /* name of option */
    (char *) &flag, /* the cast is historical cruft */
    sizeof(int)); /* length of option value */
    addNewLearner(sock);
  }

  void LearnerCnxMgr::onError() {
    stopping_ = true;
    event_base_loopbreak(ebase);
  }

  void LearnerCnxMgr::processMsg(int fd, string* buf) {
    //parse and validated buffer
    while (sizeof(int32) <= buf->length()) {
      ByteBuffer b;
      b.WriteBytes(buf->data(), buf->length());
      int32 packetLen = 0;
      b.ReadInt32(packetLen);
      if ((0 < packetLen) && (packetLen < PACKETMAXSIZE)) {
        if (packetLen <= (int32)b.Length()) {
          QuorumPacket qp;
          b.ReadInt32(qp.type);
          b.ReadInt64(qp.zxid);
          int payload_len = packetLen - sizeof(qp.type) - sizeof(qp.zxid);
          if (0 < payload_len){
            qp.payload.WriteBytes(b.Data(), payload_len);
          }
          processQp(fd, qp);
          buf->erase(0, sizeof(int32) + packetLen);
        } else {
          DEBUG("Message was only "<<buf->length()<<" required "<<packetLen);
          return;
        }
      } else {
        WARN("Invalid message with packet length "<<packetLen);
      }
    }
  }

  void LearnerCnxMgr::onRecvMsg(int fd) {
    TRACE("Got new message from sock "<<fd);

    char b[READ_BUF_LEN];
    int readNum = recv(fd, b, READ_BUF_LEN, 0);
    if (readNum == 0) {
      ERROR("connection on sock "<<fd<<" was shutdown by peer, remove follower");
      removeLearner(fd);
      eraseSockBuf(fd);
      close(fd);
      return;
    } else if (readNum < 0) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") on the LearnerCnxMgr.");
      removeLearner(fd);
      eraseSockBuf(fd);
      close(fd);
      return;
    }

    string* buf = getSockBuf(fd);
    buf->append(b, readNum);
    processMsg(fd, buf);
  }

  void LearnerCnxMgr::onMsgReady(int fd) {
    DEBUG("message ready for sending");
    char c = 0;
    read(fd, &c, 1);
    processSendQueue();
    return;
  }
  bool LearnerCnxMgr::setupPipe(int * pipeA) {
    //setup wakeup channel;
    if (pipe(pipeA)) {
      ERROR("Could not create pipe");
      return false;
    }

    ZabUtil::SetNoneBlockFD((pipeA)[0]);
    ZabUtil::SetNoneBlockFD((pipeA)[1]);
    fcntl((pipeA)[0], F_SETFL, O_NOATIME);
    return true;
  }

  void LearnerCnxMgr::cleanupPipe(int * pipeA) {
    if (pipeA[PIPE_OUT] != -1) {
      close(pipeA[PIPE_OUT]);
    }

    if (pipeA[PIPE_IN] != -1) {
      close(pipeA[PIPE_IN]);
    }
  }

  void LearnerCnxMgr::Init() {
    if (ebase == NULL) {
      ebase = event_base_new();
    }

    setupPipe(wakeup_pipe);
    setupPipe(msg_pipe);

    struct event* e = event_new(ebase, wakeup_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &LearnerCnxMgr::OnWakeup, ebase);
    event_add(e, NULL);
    i_eventMap[wakeup_pipe[PIPE_OUT]] = e;

    struct event* emsg = event_new(ebase, msg_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &LearnerCnxMgr::OnSendingMsgReady, this);
    event_add(emsg, NULL);
    i_eventMap[msg_pipe[PIPE_OUT]] = emsg;
  }

  void LearnerCnxMgr::Run() {
    string addr;
    int port;
    if (!leader->GetQuorumAddr(addr, port)) {
      ERROR("could not find my quorum address, quit");
      return;
    }

    struct sockaddr_in servSin;
    memset(&servSin, 0, sizeof(servSin)); /* Zero out structure */
    servSin.sin_family = AF_INET; /* Internet address family */
    servSin.sin_addr.s_addr = inet_addr(addr.data()); /* Any incoming interface */
    servSin.sin_port = htons(port); /* Local port */

    struct evconnlistener *listener = evconnlistener_new_bind(ebase, &LearnerCnxMgr::OnLibEventListenerNotify, this,
        LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1, (struct sockaddr*) &servSin, sizeof(servSin));

    if (!listener) {
      ERROR("Couldn't create learner listener,quit");
      return;
    }
    evconnlistener_set_error_cb(listener, &LearnerCnxMgr::OnLibEventListenerError);

    while (!stopping_) {
      event_base_dispatch(ebase);
      if (stopping_) {
        WARN("Event loop exit");
        break;
      }
    }

    evconnlistener_free(listener);
  }

  void LearnerCnxMgr::CleanUp() {
    for (EventMap::iterator iter = i_eventMap.begin(); iter != i_eventMap.end(); iter++) {
      struct event * e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
      close(iter->first);
    }
    i_eventMap.clear();

    for (SockBufMap::iterator iter = i_sockBufMap.begin(); iter != i_sockBufMap.end(); iter++) {
      string* b = iter->second;
      if (b != NULL) {
        delete b;
      }
    }
    i_sockBufMap.clear();

    if (ebase != NULL) {
      event_base_free(ebase);
      ebase = NULL;
    }

    cleanupPipe(wakeup_pipe);
    cleanupPipe(msg_pipe);
    i_sock2FollowerMap.clear();
    i_sid2SockMap.clear();
  }

  void LearnerCnxMgr::ShuttingDown(){
    INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
    if (wakeup_pipe[PIPE_IN] != -1) {
      char c = 0;
      write(wakeup_pipe[PIPE_IN], &c, 1);
    }
  }
  //------------------Leader-----------------------
  Leader::Leader(QuorumPeer* myself, ZabDBInterface* z)
      :   self(myself),
          learnerCnxMgr(this),
          waitingForNewEpoch(true),
          electionFinished(false),
          epoch(0),
          weNewEpoch(false, false),
          weElection(false, false),
          weNewLeader(false, false),
          leaderState(ZAB_WAITING_EPOCH),
          tick(0),
          lastCommitted(0),
          lastProposed(0),
          zabServer(myself->getId(), this, z),
          clientCnxMgr(myself->clientPort()),
          stop(false){

  }

  Leader::~Leader() {

  }

  void Leader::Lead() {
    INFO("Leading....");

    leaderState = ZAB_WAITING_EPOCH;

    clientCnxMgr.RegisterHanlder(&clientHandler);
    clientHandler.SetZabServer(&zabServer);

    while ((!stop) && (leaderState != ZAB_QUIT)) {
      switch (leaderState) {
        case ZAB_WAITING_EPOCH: {
          lstateSummary.currentEpoch = self->getCurrentEpoch();
          lstateSummary.lastZxid = self->getLastZxid();
          connectingFollowers.clear();

          //add my self before accept connection from followers
          AddConnectingFollowers(self->getId(), self->getAcceptedEpoch());

          //begin to accpet connection from followers
          learnerCnxMgr.Start();

          waitForNewEpoch();

          if (waitingForNewEpoch) {
            ERROR("Timeout while waiting for epoch from quorum, quit");
            leaderState = ZAB_QUIT;
          } else {
            INFO("we get new epoch "<<epoch<<" from quorum, move state to ZAB_NEW_EPOCH");
            leaderState = ZAB_NEW_EPOCH;
          }
          break;
        }
        case ZAB_NEW_EPOCH: {
          electingFollowers.clear();
          AddEpochAck(self->getId(), lstateSummary);

          QuorumPacket * qp = new QuorumPacket();
          qp->type = LEADERINFO;
          qp->zxid = ZxidUtils::MakeZxid(epoch, 0);
          //todo add version as zookeeper did?
          learnerCnxMgr.SendQp(qp);
          //send LEADERINFO packet;
          INFO("send new epoch "<<epoch<<" to quorum, move state to ZAB_WAITING_EPCH_ACK");
          leaderState = ZAB_WAITING_EPCH_ACK;
          break;
        }

        case ZAB_WAITING_EPCH_ACK: {
          INFO("Waiting for epch ack from quorum");
          waitForEpochAck();
          if (!electionFinished) {
            ERROR("Timeout while waiting for epoch ACK from quorum, quit");
            leaderState = ZAB_QUIT;
          } else {
            INFO("We get enough EPOCHACK, move state to ZAB_NEW_LEADER");
            //todo need to make it persist in storage
            zabServer.SetZxid(ZxidUtils::MakeZxid(epoch, 0));
            leaderState = ZAB_NEW_LEADER;
            self->setCurretnEpoch(epoch);
          }
          break;
        }
        case ZAB_NEW_LEADER: {
          newLeaderAck.clear();
          newLeaderAck.insert(self->getId());

          INFO("Broadcast NEW_LEADER to quorum");
          QuorumPacket * qp = new QuorumPacket();
          qp->type = NEWLEADER;
          qp->zxid = ZxidUtils::MakeZxid(epoch, 0);
          learnerCnxMgr.SendQp(qp);
          INFO("send NEWLEADER proposal to quorum, move state to ZAB_NEW_LEADER_ACK");
          leaderState = ZAB_NEW_LEADER_ACK;
          break;
        }
        case ZAB_NEW_LEADER_ACK: {
          INFO("Waiting for NEWLEADER ACK from quorum");
          waitForNewLeaderAck();

          if (!self->getQuorumVerifier()->containsQuorum(newLeaderAck)) {
            ERROR("could not get NEWLEADER ACK from quorum, quit");
            leaderState = ZAB_QUIT;
          } else {
            INFO("we got enough ACK from quorom, move state to ZAB_LEADER_COMMIT");
            leaderState = ZAB_LEADER_COMMIT;
          }
          break;
        }
        case ZAB_LEADER_COMMIT: {
          INFO("New Leader commit, leading...");
          QuorumPacket * qp = new QuorumPacket();
          qp->type = COMMIT;
          qp->zxid = ZxidUtils::MakeZxid(epoch, 0);
          learnerCnxMgr.SendQp(qp);

          zabServer.Start();
          clientCnxMgr.Start();

          set<int64> syncSet;
          bool tickSkip = true;
          while (!stop) {
            usleep(self->tickTime() / 2 * 1000);

            if (stop) {
              break;
            }
            if (!tickSkip) {
              tick++;
            }
            syncSet.clear();
            syncSet.insert(self->getId());
            learnerCnxMgr.GetSyncedFollowers(syncSet);
            learnerCnxMgr.PingFollowers();

            if (!tickSkip && !self->getQuorumVerifier()->containsQuorum(syncSet)) {
              ERROR("Only have "<<syncSet.size()<<" synced followers, did not reach quorum, quit");
              leaderState = ZAB_QUIT;
              break;
            }
            tickSkip = !tickSkip;
          }
          break;
        }
        default: {
          ERROR("Unknown state "<<leaderState);
          leaderState = ZAB_QUIT;
          break;
        }
      }
    }

    clientCnxMgr.Stop();
    zabServer.Stop();
    learnerCnxMgr.Stop();
  }

  void Leader::ProcessRequest(const Request& req) {
    int64 newZxid = zabServer.GetNextZxid();

    //create a new request buffer and submit to zabserver;
    Request * r = new Request();
    int64 sid = req.sid;
    if (sid == 0) {
      sid = self->getId();
    }

    r->sid = sid;
    r->clientfd = req.clientfd;
    r->type = req.type;
    r->client_type = req.client_type;
    r->zxid = newZxid;
    r->request.WriteBytes(req.request.Data(), req.request.Length());
    zabServer.SubmitProposal(r);

    Proposal* p = new Proposal();
    p->qp.zxid = newZxid;
    p->qp.type = PROPOSAL;
    p->qp.payload.WriteInt32(req.clientfd);
    p->qp.payload.WriteInt32(req.type);
    p->qp.payload.WriteInt32(req.client_type);
    p->qp.payload.WriteInt64(newZxid);
    p->qp.payload.WriteInt64(sid);
    p->qp.payload.WriteBytes(req.request.Data(), req.request.Length());
    p->ackSet.insert(self->getId());
    addProposal(p);

    QuorumPacket* q = new QuorumPacket();
    q->type = PROPOSAL;
    q->zxid = newZxid;
    q->payload.WriteBytes(p->qp.payload.Data(), p->qp.payload.Length());
    //send proposal to followers
    learnerCnxMgr.SendQp(q);
  }

  bool Leader::GetQuorumAddr(string & addr, int & port) {
    return self->GetPeerAddr(self->getId(), addr, port);
  }

  int Leader::GetSyncLimit() {
    return self->syncLimit();
  }

  void Leader::AddConnectingFollowers(int64 sid, int64 lastAcceptedEpoch) {
    if (!waitingForNewEpoch) {
      return;
    }

    connectingFollowers.insert(sid);
    if (lastAcceptedEpoch >= epoch) {
      epoch = lastAcceptedEpoch + 1;
    }

    TRACE("we get "<<connectingFollowers.size()<<" followers");
    if (self->getQuorumVerifier()->containsQuorum(connectingFollowers)) {
      INFO("we have majority of followers, wake up and move to next state");
      self->setAcceptedEpoch(epoch);
      waitingForNewEpoch = false;
      weNewEpoch.Signal();
    }
  }

  void Leader::AddEpochAck(int64 sid, const StateSummary& ss) {
    if (electionFinished) {
      return;
    }
    if ((ss.currentEpoch != -1) && ss.IsMoreRecentThan(lstateSummary)) {
      ERROR("Follower "<<sid<<" is ahead of the leader, quit");
      INFO("Leader summary: current Epoch "<<ZxidUtils::HexStr(lstateSummary.currentEpoch)
      <<" zxid "<<ZxidUtils::HexStr(lstateSummary.lastZxid)
      <<", Follower summary: current Epoch "<<ZxidUtils::HexStr(ss.currentEpoch)
      <<" zxid "<<ZxidUtils::HexStr(ss.lastZxid));
      weElection.Signal();
      return;
    }
    electingFollowers.insert(sid);
    TRACE("we get "<<electingFollowers.size()<<" ACK from followers");
    if (self->getQuorumVerifier()->containsQuorum(electingFollowers)) {
      electionFinished = true;
      weElection.Signal();
    }
  }

  void Leader::AddNewLeaderAct(int64 sid) {
    newLeaderAck.insert(sid);

    if (self->getQuorumVerifier()->containsQuorum(newLeaderAck)) {
      weNewLeader.Signal();
    }
  }

  void Leader::ProcessAck(int64 sid, int64 zxid) {
    AutoLock guard(proposalMapLock);
    if (proposalMap.empty()) {
      INFO("no proposal waitting for ACK");
      return;
    }

    if (zxid <= lastCommitted) {
      INFO("proposal "<<ZxidUtils::HexStr(zxid)
      <<" was already commited, lastCommitted zxid "<<ZxidUtils::HexStr(lastCommitted));
      return;
    }

    ProposalMap::iterator iter = proposalMap.find(zxid);
    if (iter != proposalMap.end()) {
      iter->second->ackSet.insert(sid);
      if (self->getQuorumVerifier()->containsQuorum(iter->second->ackSet)) {
        INFO("commit proposal: "<<ZxidUtils::HexStr(zxid));
        lastCommitted = zxid;

        QuorumPacket * qp = new QuorumPacket;
        qp->type = COMMIT;
        qp->zxid = zxid;
        learnerCnxMgr.SendQp(qp);

        zabServer.CommitProposal(zxid);

        Proposal * p = iter->second;
        delete p;
        proposalMap.erase(iter);
      }
      DEBUG("got "<<iter->second->ackSet.size()<<" ACK from quorum for proposal: "<<ZxidUtils::HexStr(zxid));
    } else {
      WARN("could find proposal: "<<ZxidUtils::HexStr(zxid));
    }
  }

  void Leader::addProposal(Proposal *p) {
    AutoLock guard(proposalMapLock);
    ProposalMap::iterator iter = proposalMap.find(p->qp.zxid);
    if (iter == proposalMap.end()) {
      INFO("Added new proposal:"<<ZxidUtils::HexStr(p->qp.zxid));
      proposalMap.insert(pair<int64, Proposal*>(p->qp.zxid, p));
      lastProposed = p->qp.zxid;
    } else {
      WARN("proposal:"<<ZxidUtils::HexStr(p->qp.zxid)<<" already exists");
      delete p;
    }
  }

  void Leader::waitForNewEpoch() {
    weNewEpoch.TimedWait(self->tickTime() * self->syncLimit() * 1000);
  }

  void Leader::waitForEpochAck() {
    weElection.TimedWait(self->tickTime() * self->syncLimit() * 1000);
  }

  void Leader::waitForNewLeaderAck() {
    weNewLeader.TimedWait(self->tickTime() * self->initLimit() * 1000);
  }
}

