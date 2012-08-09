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

#include "follower.h"
#include "quorum_peer.h"
#include "election_protocol.h"
#include "quorum_packet.h"
#include "zab_utils.h"
#include "zab_constant.h"
#include "event2/event.h"
#include "base/logging.h"
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>

namespace ZABCPP {
  Follower::Follower(QuorumPeer* myself, ZabDBInterface * z)
      :
          self(myself),
          ebase(NULL),
          zabState(F_EPOCH_PROPOSE),
          waitingForRegister(false),
          needBreak(false),
          epoch(0),
          sock(-1),
          zabServer(myself->getId(), this, z),
          clientCnxMgr(myself->clientPort()),
          clientHandler(NULL){
    wakeup_pipe[PIPE_OUT] = -1;
    wakeup_pipe[PIPE_IN] = -1;
  }

  Follower::~Follower() {
    if (clientHandler != NULL) {
      delete clientHandler;
      clientHandler = NULL;
    }
  }

  bool Follower::findLeader(string& addr, int& port) {
    Vote v = self->getCurrentVote();
    return self->GetPeerAddr(v.id, addr, port);
  }

  int Follower::connectLeader(const string& addr, int port) {
    int retry = 0;
    int sock = -1;

    while (retry < MAX_CONNECT_LEADER_RETRY) {
      DEBUG("Try to connect leader "<<addr<<":"<<port);
      sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
      if (sock < 0) {
        ERROR("Could not create socket");
        retry++;
        sleep(1);
        continue;
      }
      int flag = 1;
      setsockopt(sock, /* socket affected */
      IPPROTO_TCP, /* set option at TCP level */
      TCP_NODELAY, /* name of option */
      (char *) &flag, /* the cast is historical cruft */
      sizeof(int));
      struct sockaddr_in peerAddr;
      memset(&peerAddr, 0, sizeof(peerAddr));
      peerAddr.sin_family = AF_INET; /* Internet address family */
      peerAddr.sin_addr.s_addr = inet_addr(addr.data()); /* Server IP address */
      peerAddr.sin_port = htons(port); /* Server port */
      if (connect(sock, (struct sockaddr *) &peerAddr, sizeof(peerAddr)) < 0) {
        ERROR("could not connect to leader addr "<<addr<<" port "<<port<<" errno "<<errno);
        retry++;
        close(sock);
        sleep(1);
        continue;
      }
      break;
    }
    return sock;
  }

  void Follower::processMsg(int fd, string* buf) {
    //get packet length first
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
        ERROR("Invalid message which packet length was "<<packetLen);
        //todo, since we get a corrupt message should we just close the connection and quit???
      }
    }
  }

  void Follower::onRecvMsg(int fd) {
    TRACE("we get new message from "<<fd);

    char buf[READ_BUF_LEN];
    memset(buf, 0, READ_BUF_LEN);
    int readNum = recv(fd, buf, READ_BUF_LEN, 0);
    if (readNum <= 0) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") on the Follower.");
      needBreak = true;
      close(fd);
      event_base_loopbreak(ebase);
      return;
    }
    recvBuf.append(buf, readNum);
    processMsg(fd, &recvBuf);
  }

  void Follower::sendQp(const QuorumPacket& qp, int fd) {
    ByteBuffer out;
    out.WriteInt32(sizeof(qp.type) + sizeof(qp.zxid) + qp.payload.Length());
    out.WriteInt32(qp.type);
    out.WriteInt64(qp.zxid);
    out.WriteBytes(qp.payload.Data(), qp.payload.Length());

    int sendNum = send(fd, out.Data(), out.Length(), 0);
    if (sendNum < 0) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") when sending message.");
    }
  }

  void Follower::setupPipe(int* pipeA) {
    if (pipe(pipeA)) {
      ERROR("Could not create pipe");
      return;
    }

    ZabUtil::SetNoneBlockFD((pipeA)[0]);
    ZabUtil::SetNoneBlockFD((pipeA)[1]);
  }

  void Follower::cleanupPipe(int* pipeA) {
    if (pipeA[PIPE_OUT] != -1) {
      close(pipeA[PIPE_OUT]);
    }

    if (pipeA[PIPE_IN] != -1) {
      close(pipeA[PIPE_IN]);
    }
  }

  void Follower::processQp(int fd, QuorumPacket& qp) {
    DEBUG("Process new quorum packet type "<<qp.type<<" zxid "<<ZxidUtils::HexStr(qp.zxid));

    switch (zabState) {
      case F_WAITING_EPOCH: {
        if (qp.type == LEADERINFO) {
          // we are connected to a 1.0 server so accept the new epoch and read the next packet
          int64 newEpoch = ZxidUtils::GetEpochFromZxid(qp.zxid);
          int64 peerEpoch = self->getAcceptedEpoch();
          if (newEpoch > self->getAcceptedEpoch()) {
            self->setAcceptedEpoch(newEpoch);
            epoch = newEpoch;
          } else if (newEpoch == peerEpoch) {
            peerEpoch = -1;
          } else if (newEpoch < self->getAcceptedEpoch()) {
            ERROR("Proposed leader epoch "<< ZxidUtils::ZxidToString(newEpoch)
            <<" is less than our accepted epoch "<<ZxidUtils::ZxidToString(self->getAcceptedEpoch()));
            needBreak = true;
            close(fd);
            event_base_loopbreak(ebase);
            return;
          }
          zabState = F_NEW_EPOCH_ACK;
          QuorumPacket ack;
          ack.type = ACKEPOCH;
          ack.zxid = self->getLastZxid();
          ack.payload.WriteInt64(peerEpoch);
          INFO("send ACKEPOCH to leader: zxid "<<ZxidUtils::HexStr(ack.zxid)
          <<" my Epoch "<<peerEpoch<<" move state to F_NEW_EPOCH_ACK");
          sendQp(ack, fd);
        } else {
          ERROR("Invalid packet when waiting for NEW EPOCH, expected type " <<LEADERINFO<<" but we got "<<qp.type<<" ,quit");
          close(fd);
          event_base_loopbreak(ebase);
          return;
        }
        break;
      }
      case F_NEW_EPOCH_ACK: {
        if (qp.type == NEWLEADER) {
          INFO("Get NEWLEADER packet, send NEW LEADER ACK and move to F_NEW_LEADER_ACK");
          QuorumPacket ack;
          ack.type = ACK;
          ack.zxid = self->getLastZxid();
          self->setCurretnEpoch(epoch);
          sendQp(ack, fd);

          if (!zabServer.IsRunning())
            zabServer.Start();
          clientCnxMgr.Start();
          zabState = F_NEW_LEADER_ACK;
        } else {
//          ERROR("Invalid packet when waiting for NEW LEADER, expected type " <<NEWLEADER<<" but we got "<<qp.type<<" ,quit");
//          close(fd);
//          event_base_loopbreak(ebase);
//          return;
        }
        break;
      }
      case F_NEW_LEADER_ACK: {
        if (qp.type == COMMIT) {
          INFO("Get NEWLEADER COMMIT packet, move state to F_NEW_LEADER_COMMIT");
          zabState = F_NEW_LEADER_COMMIT;
        } else {
//          ERROR("Invalid packet when waiting for NEW LEADER COMMIT, expected type "
//              <<COMMIT<<" but we got "<<qp.type<<" ,quit");
//          close(fd);
//          event_base_loopbreak(ebase);
//          return;
        }
        break;
      }
      case F_NEW_LEADER_COMMIT: {
        //we enter stable state and begin propose/commit
        switch (qp.type) {
          case PING: {
            QuorumPacket p;
            p.type = PING;
            p.zxid = self->getLastZxid();
            sendQp(p, fd);
//
//            Request req;
//            req.type = 0;
//            req.client_type = 0;
//            req.sid = self->getId();
//            req.zxid = -1;
//            zabServer.SubmitRequest(req);
            break;
          }
          case PROPOSAL: {
            //    INFO("Get new PROPOSAL packet, zxid "<<ZxidUtils::HexStr(qp.zxid));
            Request * req = new Request();
            qp.payload.ReadInt32(req->clientfd);
            qp.payload.ReadInt32(req->type);
            qp.payload.ReadInt32(req->client_type);
            qp.payload.ReadInt64(req->zxid);
            qp.payload.ReadInt64(req->sid);
            req->request.WriteBytes(qp.payload.Data(), qp.payload.Length());
            DEBUG("Get new PROPOSAL "<<ZxidUtils::HexStr(qp.zxid)
            <<" originated from server "<<ZxidUtils::HexStr(req->sid)
            <<" request data "<<req->request.Data()
            <<" request length "<<req->request.Length());
            zabServer.SubmitProposal(req);

            QuorumPacket ack;
            ack.type = ACK;
            ack.zxid = qp.zxid;
            sendQp(ack, fd);
            break;
          }
          case COMMIT: {
            zabServer.CommitProposal(qp.zxid);
            break;
          }
          case UPTODATE: {
            break;
          }
          case REVALIDATE: {
            break;
          }
          case SYNC: {
            break;
          }
        }
        break;
      }
      default: {
        break;
      }
    }
  }

  void Follower::OnLibEventNotification(evutil_socket_t fd, short what, void *arg) {
    Follower* f = static_cast<Follower*>(arg);
    if (what & EV_READ) {
      f->onRecvMsg(fd);
    }
  }

  void Follower::OnWakeup(evutil_socket_t fd, short what, void *arg) {
    INFO("Waked up and break event loop");
    char c = 0;
    read(fd, &c, 1);
    struct event_base* b = (struct event_base*) arg;
    event_base_loopbreak(b);
  }

  void Follower::ProcessRequest(const Request& req) {
    QuorumPacket qp;
    int64 myid = self->getId();

    qp.type = REQUEST;
    qp.zxid = -1;
    qp.payload.WriteInt32(req.clientfd);
    qp.payload.WriteInt32(req.type);
    qp.payload.WriteInt32(req.client_type);
    qp.payload.WriteInt64(req.zxid);
    qp.payload.WriteInt64(myid);
    qp.payload.WriteBytes(req.request.Data(), req.request.Length());
    sendQp(qp, sock);
  }

  void Follower::FollowLeader() {
    INFO("Following leader....");
    string leaderAddr;
    int leaderPort;
    sock = -1;
    if (findLeader(leaderAddr, leaderPort)) {
      sock = connectLeader(leaderAddr, leaderPort);
      if (sock < 0) {
        ERROR("Could not connect to leader");
        return;
      }
      INFO("connected to leader on sock "<<sock);
      ZabUtil::SetNoneBlockFD(sock);

      if (ebase != NULL) {
        event_base_free(ebase);
      }
      ebase = event_base_new();
      //setup wakeup channel;
      setupPipe(wakeup_pipe);
      struct event* eWakeup = event_new(ebase, wakeup_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &Follower::OnWakeup, ebase);
      event_add(eWakeup, NULL);

      struct event* eRead = event_new(ebase, sock, EV_READ | EV_PERSIST, &Follower::OnLibEventNotification, this);
      event_add(eRead, NULL);

      if (clientHandler == NULL) {
        clientHandler = new ClientHandlerRedis();
        clientHandler->SetZabServer(&zabServer);
        clientCnxMgr.RegisterHanlder(clientHandler);
      }
      //send register info to leader;
      QuorumPacket qp;
      qp.type = FOLLOWERINFO;
      qp.zxid = ZxidUtils::MakeZxid(self->getAcceptedEpoch(), 0);
      qp.payload.WriteInt64(self->getId());
      ByteBuffer out;
      out.WriteInt32(sizeof(qp.type) + sizeof(qp.zxid) + qp.payload.Length());
      out.WriteInt32(qp.type);
      out.WriteInt64(qp.zxid);
      out.WriteBytes(qp.payload.Data(), qp.payload.Length());

      int sendNum = send(sock, out.Data(), out.Length(), 0);
      if (sendNum < 0) {
        int err = EVUTIL_SOCKET_ERROR();
        ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") when sending message.");
        event_del(eWakeup);
        event_free(eWakeup);
        cleanupPipe(wakeup_pipe);

        event_del(eRead);
        event_free(eRead);
        close(sock);

        event_base_free(ebase);
        ebase = NULL;
        zabServer.Stop();
        return;
      }

      zabState = F_WAITING_EPOCH;
      //now we just waiting for read packet;
      recvBuf.clear();
      needBreak = false;
      while (!needBreak) {
        event_base_dispatch(ebase);
        if (needBreak) {
          ERROR("Error on following leader, quit");
          zabServer.Stop();
          clientCnxMgr.Stop();
          break;
        }
      }

      event_del(eWakeup);
      event_free(eWakeup);
      cleanupPipe(wakeup_pipe);

      close(sock);
      event_del(eRead);
      event_free(eRead);
      event_base_free(ebase);
      ebase = NULL;
      sock = -1;
    } else {
      ERROR("Could not find leader info");
    }
  }

  void Follower::Shutdown() {
    needBreak = true;
    if (wakeup_pipe[PIPE_IN] != -1) {
      char c = 0;
      write(wakeup_pipe[PIPE_IN], &c, 1);
    }
  }
}

