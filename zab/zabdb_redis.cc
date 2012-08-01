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


#include "zabdb_redis.h"
#include "base/logging.h"
#include "client_protocol/redis_protocol.h"
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
  ZabDBRedis::ZabDBRedis(int64 myid)
  :Thread("ZabDB Redis")
   ,ebase(NULL)
   ,sid(myid)
   ,lastProcessedZxid(-1)
   ,zxidChannelFd(-1)
   ,eZxid(NULL)
   ,redisServerAddr("127.0.0.1")
   ,redisServerPort(6379){
    wakeup_pipe[PIPE_OUT] = -1;
    wakeup_pipe[PIPE_IN] = -1;
    msg_pipe[PIPE_OUT] = -1;
    msg_pipe[PIPE_IN] = -1;
  }

  ZabDBRedis::~ZabDBRedis() {

  }

  void ZabDBRedis::SetZxid(int64 zxid) {

  }

  int64 ZabDBRedis::GetLastProcessedZxid() {
    return lastProcessedZxid;
  }

  void ZabDBRedis::ProcessRequest(Request * req) {
    pushBack(req);
    msgReady();
  }

  void ZabDBRedis::SetServerAddr(const string& addr, int port) {
    redisServerAddr = addr;
    redisServerPort = port;
  }

  void ZabDBRedis::OnZxidChannelNotification(evutil_socket_t fd, short what, void *arg) {
    ZabDBRedis * z = static_cast<ZabDBRedis*>(arg);
    if (what & EV_WRITE) {
      z->onZxidChannelReady(fd);
    } else if (what & EV_READ) {
      z->onRecvZxid(fd);
    }
  }

  void ZabDBRedis::OnLibEventNotification(evutil_socket_t fd, short what, void *arg) {
    ZabDBRedis* z = static_cast<ZabDBRedis*>(arg);
    if (what & EV_READ) {
      z->onRecvMsg(fd);
    } else if (what & EV_WRITE) {
      z->onSendReady(fd);
    }
  }

  void ZabDBRedis::OnWakeup(evutil_socket_t fd, short what, void *arg) {
    INFO("Waked up and break event loop");
    char c = 0;
    read(fd, &c, 1);
    struct event_base* b = (struct event_base*) arg;
    event_base_loopbreak(b);
  }

  void ZabDBRedis::OnRequestReady(evutil_socket_t fd, short what, void *arg) {
    ZabDBRedis* z = static_cast<ZabDBRedis*>(arg);
    if (what & EV_READ) {
      char c = 0;
      read(fd, &c, 1);
      z->onMsgReady(fd);
    }
  }

  void ZabDBRedis::Init() {
    if (ebase == NULL) {
      ebase = event_base_new();
    }

    setupPipe(wakeup_pipe);
    setupPipe(msg_pipe);

    struct event* e = event_new(ebase, wakeup_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &ZabDBRedis::OnWakeup, ebase);
    event_add(e, NULL);
    eventMap[wakeup_pipe[PIPE_OUT]] = e;

    struct event* emsg = event_new(ebase, msg_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &ZabDBRedis::OnRequestReady, this);
    event_add(emsg, NULL);
    eventMap[msg_pipe[PIPE_OUT]] = emsg;

    INFO("getting last zxid from redis");
    blockingGetLastZxid();
  }

  void ZabDBRedis::Run() {
    while(!stopping_) {
      event_base_dispatch(ebase);
      if (stopping_) {
        return;
      }
    }
  }

  void ZabDBRedis::CleanUp() {
    cleanupEvents(eventMap);
    cleanupEvents(retryEventMap);
    cleanupRequests(requestList);
    cleanupRequests(retryRequestList);

    if (eZxid != NULL) {
      event_del(eZxid);
      event_free(eZxid);
      eZxid = NULL;
    }

    if (ebase != NULL) {
      event_base_free(ebase);
      ebase = NULL;
    }

    cleanupPipe(wakeup_pipe);
    cleanupPipe(msg_pipe);

    client2Server.clear();
    server2Client.clear();
  }

  void ZabDBRedis::ShuttingDown() {
    INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
    if (wakeup_pipe[PIPE_IN] != -1) {
      char c = 0;
      write(wakeup_pipe[PIPE_IN], &c, 1);
    }
  }

  bool ZabDBRedis::removeChannel(ChannelMap& firstm, int k_fd, ChannelMap& secondm, int& v_fd) {
    bool ret = false;
    ChannelMap::iterator iter = firstm.find(k_fd);
    if(iter != firstm.end()) {
      v_fd = iter->second;
      secondm.erase(iter->second);
      firstm.erase(iter);
      ret = true;
    }
    return ret;
  }

  void ZabDBRedis::removeChannelEvent(int serverfd) {
    EventMap::iterator iter = eventMap.find(serverfd);
    if (iter != eventMap.end()) {
      struct event * e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
      close(iter->first);
      eventMap.erase(iter);
    }
  }

  void ZabDBRedis::removeChannelByClientFd(int clientfd) {
    int serverfd = 0;
    if (removeChannel(client2Server, clientfd, server2Client, serverfd)) {
      INFO("removed channel client "<<clientfd<<"---"<<serverfd<<" server");
      removeChannelEvent(serverfd);
    }
    close(clientfd);
  }

  void ZabDBRedis::removeChannelByServerFd(int serverfd) {
    int clientfd = 0;
    if (removeChannel(server2Client, serverfd, client2Server, clientfd)) {
      INFO("removed channel client "<<clientfd<<"---"<<serverfd<<" server");
      close(clientfd);
    }
    removeChannelEvent(serverfd);
  }

  void ZabDBRedis::msgReady() {
    if (msg_pipe[PIPE_IN] != -1) {
      char c = 0;
      write(msg_pipe[PIPE_IN], &c, 1);
    }
  }


  void ZabDBRedis::setupPipe(int* pipeA) {
    if (pipe(pipeA)) {
      ERROR("Could not create pipe");
      return;
    }

    ZabUtil::SetNoneBlockFD((pipeA)[0]);
    ZabUtil::SetNoneBlockFD((pipeA)[1]);
    fcntl((pipeA)[0], F_SETFL, O_NOATIME);
  }

  void ZabDBRedis::cleanupPipe(int* pipeA) {
    if (pipeA[PIPE_OUT] != -1) {
      close(pipeA[PIPE_OUT]);
    }

    if (pipeA[PIPE_IN] != -1) {
      close(pipeA[PIPE_IN]);
    }
  }

  void ZabDBRedis::onRecvMsg(int fd) {
    DEBUG("get response from redis on channel "<<fd);

    char buf[READ_BUF_LEN];
    memset(buf, 0, READ_BUF_LEN);
    int readnum = recv(fd, buf, READ_BUF_LEN, 0);
    if (readnum == -1) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") on the ZabDBRedis.");
      if (err == EAGAIN) {
        readnum = 0;
      } else {
        // server channel was broken
        removeChannelByServerFd(fd);
      }
      return;
    } else if (readnum == 0) {
      ERROR("server channel "<<fd<<" was shutdown");
      // server channel was broken
      removeChannelByServerFd(fd);
      return;
    }

    ChannelMap::iterator iter = server2Client.find(fd);
    if (iter != server2Client.end()) {
      int sendnum = send(iter->second, buf, readnum, 0);
      if (sendnum < 0) {
        int err = EVUTIL_SOCKET_ERROR();
        ERROR("Error on sending response to client on sock "<<iter->second
           <<", error " <<err
           <<" ("<<evutil_socket_error_to_string(err)<<")");

        //although client connection was down, the request should be handled already by redis
        //just remove channel and return
        removeChannelByServerFd(fd);
      }
    } else {
      WARN("No client available for server channel on "<<fd);
      removeChannelByServerFd(fd);
    }
  }

  void ZabDBRedis::onMsgReady(int fd) {
    Request * r = NULL;
    if (!retryRequestList.empty()) {
      WARN("there was pending requests which was not sent to redis, will not process any new one");
      return;
    }

    while ((r = popFront()) != NULL) {
      //request was originated from my client then I need to send back response
      int serverfd = -1;

      if (r->sid == sid) {
        ChannelMap::iterator iter = client2Server.find(r->clientfd);
        if (iter != client2Server.end()) {
          serverfd = iter->second;
        } else {
          //set up a new server channel for client
          serverfd = setupServerChannel(r->clientfd);
        }
      } else {
        //request was from other peer, just send to redis and did not handle response
        serverfd = zxidChannelFd;
      }

      if (serverfd != -1) {
        int nwrite = send(serverfd, r->request.Data(), r->request.Length(), 0);
        //get error when sending request
        if (nwrite < 0 ){
          int err = EVUTIL_SOCKET_ERROR();
          ERROR("Error on sending request "<<ZxidUtils::HexStr(r->zxid)
             <<" on sock "<<serverfd<<", error " <<err
             <<" ("<<evutil_socket_error_to_string(err)<<")");

          if (err == EAGAIN) {
            retryRequestList.push_back(r);
            //create EV_WRITE event and let kernel tell us when could I send message
            struct event * e = event_new(ebase, serverfd, EV_WRITE|EV_PERSIST, &ZabDBRedis::OnLibEventNotification, this);
            event_add(e, NULL);
            retryEventMap[serverfd] = e;
            return;
          }
          //todo, we find one connection to redis was down
          //in this case, we should not handle any new request otherwise
          //data will not consistent. We have to decide
          //1.should we just shutdown entire server
          //2.should we just retry connecting redis and repost request
          //The safest way was to shutdown server
          handleServerDown(serverfd);
        } else if (nwrite < (int)r->request.Length()) {
          //EGAIN or only partial packet was sent to redis
          //register EV_WRITE and try to re-send request;
          DEBUG("Only sent "<<nwrite<<" ,required to send "<<r->request.Length()<<" add to retry list");
          r->request.Shift(nwrite);
          retryRequestList.push_back(r);
          struct event * e = event_new(ebase, serverfd, EV_WRITE|EV_PERSIST, &ZabDBRedis::OnLibEventNotification, this);
          event_add(e, NULL);
          retryEventMap[serverfd] = e;
          return;
        } else if (nwrite == (int)r->request.Length()) {
          //we have send all packet to redis
          //update zxid then
          updateZxid(r->zxid);

          DEBUG("commit request "<<ZxidUtils::HexStr(r->zxid)
                <<" to redis on channel "<<serverfd
                <<" request len "<<r->request.Length()
                <<" data:"<<r->request.Data());
          delete r;
        }
      }  else {
        ERROR("No server channel available for client "<<r->clientfd);
        return;
      }
    }
  }

  void ZabDBRedis::onSendReady(int fd) {
    if (!retryRequestList.empty()){
      Request * r = retryRequestList.front();
      int nwrite = send(fd, r->request.Data(), r->request.Length(), 0);
      if (nwrite < 0) {
        int err = EVUTIL_SOCKET_ERROR();
        ERROR("Error on sending request "<<ZxidUtils::HexStr(r->zxid)
           <<" on sock "<<fd<<", error " <<err
           <<" ("<<evutil_socket_error_to_string(err)<<")");
        //todo, we find one connection to redis was down
        //in this case, we should not handle any new request otherwise
        //data will not consistent. We have to decide
        //1.should we just shutdown entire server
        //2.should we just retry connecting redis and repost request
        //The safest way was to shutdown server
        handleServerDown(fd);
      } else if (nwrite < (int)r->request.Length()) {
        r->request.Shift(nwrite);
        return;
      } else {
        INFO("re-send request "<<ZxidUtils::HexStr(r->zxid)<<" to redis successfully");

        //update zxid to redis server
        updateZxid(r->zxid);
        retryRequestList.pop_front();
        //finally we have send all data to redis
        EventMap::iterator iter = retryEventMap.find(fd);
        if (iter != retryEventMap.end()) {
          struct event *e = iter->second;
          if (e != NULL) {
            event_del(e);
            event_free(e);
          }
          retryEventMap.erase(iter);
        }
        delete r;
      }
    }
  }

  void ZabDBRedis::blockingGetLastZxid() {
      zxidChannelFd = setupServerChannel(-1);
      while (zxidChannelFd == -1 ) {
        sleep(2);
        zxidChannelFd = setupServerChannel(-1);
      }
      eZxid = event_new(ebase, zxidChannelFd, EV_WRITE|EV_PERSIST, &ZabDBRedis::OnZxidChannelNotification, this);
      event_add(eZxid, NULL);
      event_base_dispatch(ebase);

      INFO("last processed zxid was "<<ZxidUtils::HexStr(lastProcessedZxid));
  }

  void ZabDBRedis::updateZxid(int64 zxid) {
    if (zxid != -1) {
      lastProcessedZxid = zxid;
      char * set_zxid = NULL;
      int len = raw_redisCommand(&set_zxid,"set zxid %lld", zxid);
      int nwrites = send(zxidChannelFd, set_zxid, len, 0);
      free(set_zxid);
      if (nwrites < 0 ) {
        int err = EVUTIL_SOCKET_ERROR();

        if(err != EAGAIN) {
          ERROR("failed to update zxid "<<ZxidUtils::HexStr(zxid)
                <<" server channel on sock "<<zxidChannelFd<<" was broken, error "
                <<err<<" ("<<evutil_socket_error_to_string(err)<<")");
          handleServerDown(zxidChannelFd);
        }
      }
    }
  }

  int ZabDBRedis::processZxidReply(string & buf) {
    if (buf.at(0) != '$') {
      INFO("Reply type "<<buf.at(0)<<" was not supported");
      buf.clear();
      return -1;
    }

    string delim("\r\n");
    int newline = buf.find(delim);
    if (newline != (int)string::npos) {
      int zxidLen = strtol(buf.substr(1, newline - 1).data(), NULL, 10);
      if (zxidLen < 0 ) {
        lastProcessedZxid = 0;
      } else  if (newline+2+zxidLen <= (int)buf.length()) {
        lastProcessedZxid = strtoll(buf.substr(newline+2, zxidLen).data(), NULL, 10);
      }
    }

    return lastProcessedZxid;
  }

  void ZabDBRedis::onRecvZxid(int fd) {
    char buf[READ_BUF_LEN];
    memset(buf, 0, READ_BUF_LEN);
    int readnum = recv(fd, buf, READ_BUF_LEN, 0);
    if (readnum == -1) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") on the ZabDBRedis.");
      if (err == EAGAIN) {
        readnum = 0;
      } else {
        // server channel was broken
        handleServerDown(fd);
      }
      return;
    } else if (readnum == 0) {
      ERROR("server channel "<<fd<<" was shutdown");
      // server channel was broken
      handleServerDown(fd);
      return;
    }

    //todo parse redis reply
    zxidReply.append(buf, readnum);
    if (processZxidReply(zxidReply) < 0) {
      return;
    }
    //delete event if we get zxid from redis
    if (eZxid != NULL) {
      event_del(eZxid);
      event_free(eZxid);
    }
    event_base_loopbreak(ebase);
  }

  void ZabDBRedis::onZxidChannelReady(int fd) {
    char * set_zxid = NULL;
    int len = raw_redisCommand(&set_zxid,"get zxid");
    int nwrites = send(fd, set_zxid, len, 0);
    free(set_zxid);
    if (nwrites < 0 ) {
      int err = EVUTIL_SOCKET_ERROR();

      if(err != EAGAIN) {
        ERROR("Could not get zxid on sock "<<fd<<", error "
              <<err<<" ("<<evutil_socket_error_to_string(err)<<")");
        handleServerDown(fd);
      }
    } else {
      if (eZxid != NULL) {
        event_del(eZxid);
        event_free(eZxid);
      }
      zxidReply.clear();
      eZxid = event_new(ebase, fd, EV_READ|EV_PERSIST,  &ZabDBRedis::OnZxidChannelNotification, this);
      event_add(eZxid, NULL);
    }
  }

  int ZabDBRedis::setupServerChannel(int clientfd) {
    int serverfd = -1;
    serverfd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (serverfd < 0) {
      ERROR("Could not create socket");
      return -1;
    }
    int flag = 1;
    setsockopt(serverfd, /* socket affected */
    IPPROTO_TCP, /* set option at TCP level */
    TCP_NODELAY, /* name of option */
    (char *) &flag, /* the cast is historical cruft */
    sizeof(int));

    struct sockaddr_in peerAddr;
    memset(&peerAddr, 0, sizeof(peerAddr));
    peerAddr.sin_family = AF_INET; /* Internet address family */
    peerAddr.sin_addr.s_addr = inet_addr(redisServerAddr.data()); /* Server IP address */
    peerAddr.sin_port = htons(redisServerPort); /* Server port */

    if (connect(serverfd, (struct sockaddr *) &peerAddr, sizeof(peerAddr)) < 0) {
      ERROR("could not connect to redis server "<<redisServerAddr<<" port "<<redisServerPort<<" errno "<<errno);
      close(serverfd);
      serverfd = -1;
    } else {
      INFO("Server Channel setup successfully on "<<serverfd<<" for client "<<clientfd);
      ZabUtil::SetNoneBlockFD(serverfd);
      //if it was zxid channel, no need to add handle response
      if (clientfd != -1) {
        struct event * e = event_new(ebase, serverfd, EV_READ | EV_PERSIST, &ZabDBRedis::OnLibEventNotification, this);
        event_add(e, NULL);
        eventMap[serverfd] = e;
        client2Server[clientfd] = serverfd;
        server2Client[serverfd] = clientfd;
      }
    }
    return serverfd;
  }

  //have lock inside
  void ZabDBRedis::pushBack(Request* r) {
    AutoLock guard(requestLock);
    requestList.push_back(r);
  }

  Request* ZabDBRedis::popFront() {
    Request * r = NULL;
    AutoLock guard(requestLock);
    if (!requestList.empty()) {
      r = requestList.front();
      requestList.pop_front();
    }
    return r;
  }

  void ZabDBRedis::cleanupRequests(RequestList& reqList) {
    AutoLock guard(requestLock);
    for(RequestList::iterator iter = reqList.begin();
        iter != reqList.end();
        iter ++) {
      Request * r = *iter;
      if (r != NULL) {
        delete r;
      }
    }
    reqList.clear();
  }

  void ZabDBRedis::cleanupEvents(EventMap& eMap) {
    for (EventMap::iterator iter = eMap.begin(); iter != eMap.end(); iter++) {
      struct event * e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
      close(iter->first);
    }
    eMap.clear();
  }

  void ZabDBRedis::handleServerDown(int fd) {
    ERROR("Server connection "<<fd<<" was broken, quit");
    StopSoon();
  }
}
