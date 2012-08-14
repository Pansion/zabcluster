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


#include "election_cnxmgr.h"
#include "base/logging.h"
#include "election_protocol.h"
#include "zab_constant.h"
#include "zab_utils.h"
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <string.h>
namespace ZABCPP {

#define FOR_EACH_HANDLER(HandlerType, HandlerList, func)  \
  do {                                                        \
    for(set<HandlerType>::iterator iter = HandlerList.begin();iter != HandlerList.end();iter++)\
    {                                                                                         \
      HandlerType h = *iter;                                                                  \
      h->func;                                                                                \
    }                                                                                         \
  } while (0)

#define OpenScope         if(1)

  QuorumCnxMgr::QuorumCnxMgr(QuorumPeerConfig* Cfg)
      :Thread("Quorum Cnx Mgr")
  , peerConfig(Cfg)
  , ebase(NULL)
  , numRetries(0)
  , weRecv(false, false){
    wakeup_pipe[PIPE_OUT] = -1;
    wakeup_pipe[PIPE_IN] = -1;
    msg_pipe[PIPE_OUT] = -1;
    msg_pipe[PIPE_IN] = -1;
  }

  QuorumCnxMgr::~QuorumCnxMgr() {

  }

  void QuorumCnxMgr::RegisterHandler(ElectionMsgHandlerInterface* h) {
    AutoLock guard(handlerLock);
    handlerSet.insert(h);
  }

  void QuorumCnxMgr::UnregisterHandler(ElectionMsgHandlerInterface* h) {
    AutoLock guard(handlerLock);
    handlerSet.erase(h);
  }

  void QuorumCnxMgr::toSend(int64 sid, ByteBuffer* buf) {
      getQueue(sid)->PushBack(buf);
      msgReady(sid);
  }

  void QuorumCnxMgr::connectOne(int64 sid) {
    INFO("connecting to server "<<sid);
    if (getSock(sid) != -1) {
      DEBUG("There is a connection already for server "<<sid);
      return;
    }

    AutoLock guard(connectLock);
    if (sid != peerConfig->serverId) {
      QuorumServerMap::iterator iter = peerConfig->servers.find(sid);
      if (iter != peerConfig->servers.end()) {
        string addr = iter->second.addr;
        int port = iter->second.electionPort;
        DEBUG("open channel to server "<<sid);
        int sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (sock < 0) {
          ERROR("Could not create socket");
          return;
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
          ERROR("could not connect to server "<<sid<<" addr "<<addr<<" port "<<port<<" errno "<<errno);
          return;
        }
        DEBUG("connected to server "<<sid);
        initiateConnection(sock, sid);
      } else {
        ERROR("invalid sid "<<sid);
        return;
      }
    }
  }

  void QuorumCnxMgr::connectAll() {
    for (QuorumServerMap::iterator iter = peerConfig->servers.begin(); iter != peerConfig->servers.end(); iter++) {
      connectOne(iter->second.id);
    }
  }

  void QuorumCnxMgr::initiateConnection(int sock, int64 sid) {
    //immediately write our sid to remote;
    int64 nsid = (int64) HostToNetwork64(peerConfig->serverId);
    int sendNum = send(sock, &nsid, sizeof(nsid), 0);
    if (sendNum < 0) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") when sending message.");
      close(sock);
      return;
    }

    if (sid > peerConfig->serverId) {
      INFO("I Have smaller server identifier, so dropping the " <<"connection: ("<<sid << ", "<<peerConfig->serverId <<")");
      close(sock);
      return;
    } else {
      // NOTE:should add peer first to avoid race-condition on find new peer
      addPeer(sock, sid);
      addConnection(sock);
    }
    return;
  }


  bool QuorumCnxMgr::haveDelivered() {
    bool ret = true;
    AutoLock guard(sendQueueLock);
    for(SendQueueMap::iterator iter = sendQueueMap.begin();
        iter != sendQueueMap.end();
        iter ++) {
      if (!iter->second->Empty()) {
        ret = false;
        break;
      }
    }
    return ret;
  }

  //interfaces to libevent
  void QuorumCnxMgr::OnLibEventNotification(evutil_socket_t fd, short what, void *arg) {
    QuorumCnxMgr * q = static_cast<QuorumCnxMgr*>(arg);
    if (what & EV_READ) {
      q->onRecvMsg(fd);
    } else if (what & EV_WRITE) {
      q->onMsgCandSend(fd);
    }
  }

  void QuorumCnxMgr::OnSendingMsgReady(evutil_socket_t fd, short what, void *arg) {
    QuorumCnxMgr * q = static_cast<QuorumCnxMgr*>(arg);
    if (what & EV_READ) {
      int64 sid;
      read(fd, &sid, sizeof(sid));
      DEBUG("Message ready for sending, sid "<<sid);
      q->onMsgReady(sid);
    }
  }

  void QuorumCnxMgr::OnLibEventListenerNotify(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address,
      int socklen, void *ctx) {
    QuorumCnxMgr * q = static_cast<QuorumCnxMgr*>(ctx);
    q->onAccept(fd);
  }

  void QuorumCnxMgr::OnLibEventListenerError(struct evconnlistener *listener, void *ctx) {
    QuorumCnxMgr * q = static_cast<QuorumCnxMgr*>(ctx);
    q->onError();
  }

  void QuorumCnxMgr::OnWakeup(evutil_socket_t fd, short what, void *arg) {
    INFO("Waked up and break event loop");
    char c = 0;
    read(fd, &c, 1);
    struct event_base* b = (struct event_base*) arg;
    event_base_loopbreak(b);
  }

  void QuorumCnxMgr::Init() {
    if (ebase == NULL) {
      ebase = event_base_new();
    }

    setupPipe(wakeup_pipe);
    setupPipe(msg_pipe);

    struct event* e = event_new(ebase, wakeup_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &QuorumCnxMgr::OnWakeup, ebase);
    event_add(e, NULL);
    eventMap[wakeup_pipe[PIPE_OUT]] = e;

    struct event* emsg = event_new(ebase, msg_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &QuorumCnxMgr::OnSendingMsgReady, this);
    event_add(emsg, NULL);
    eventMap[msg_pipe[PIPE_OUT]] = emsg;

    numRetries = 0;
  }

  void QuorumCnxMgr::Run() {
    struct sockaddr_in servSin;

    int64 myId = peerConfig->serverId;
    const QuorumServer info = peerConfig->servers[myId];

    INFO("Listener "<<myId<< " on port "<<info.electionPort<<" started");

    while (!stopping_ && (numRetries < MAX_LISTENER_RETIES)) {
      memset(&servSin, 0, sizeof(servSin)); /* Zero out structure */
      servSin.sin_family = AF_INET; /* Internet address family */
      servSin.sin_addr.s_addr = htonl(INADDR_ANY); /* Any incoming interface */
      servSin.sin_port = htons(info.electionPort); /* Local port */

      struct evconnlistener *listener = evconnlistener_new_bind(ebase, &QuorumCnxMgr::OnLibEventListenerNotify, this,
          LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1, (struct sockaddr*) &servSin, sizeof(servSin));

      if (!listener) {
        ERROR("Couldn't create listener");
        numRetries++;
        continue;
      }

      evconnlistener_set_error_cb(listener, &QuorumCnxMgr::OnLibEventListenerError);
      event_base_dispatch(ebase);
      evconnlistener_free(listener);
      listener = NULL;
      WARN("event loop break, retry times "<<numRetries);
      if (stopping_)
        return;
    }
    INFO("Listener "<<myId<< " on port "<<info.electionPort<<" quit, retry times "<<numRetries);
  }

  void QuorumCnxMgr::CleanUp() {

    cleanupEvents(eventMap);
    cleanupEvents(wEventMap);

    cleanupPipe(wakeup_pipe);
    cleanupPipe(msg_pipe);

    if (ebase != NULL) {
      event_base_free(ebase);
      ebase = NULL;
    }

    cleanupSockBuf();
    cleanupAllSendQueue();
    cleanupLastMsg();

    OpenScope {
      handlerSet.clear();
    }
    numRetries = 0;
  }

  void QuorumCnxMgr::ShuttingDown() {
    INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
    if (wakeup_pipe[PIPE_IN] != -1) {
      char c = 0;
      write(wakeup_pipe[PIPE_IN], &c, 1);
    }
  }

  //called when new connection come in
  void QuorumCnxMgr::onAccept(int sock) {
    int flag = 1;
    setsockopt(sock, /* socket affected */
    IPPROTO_TCP, /* set option at TCP level */
    TCP_NODELAY, /* name of option */
    (char *) &flag, /* the cast is historical cruft */
    sizeof(int)); /* length of option value */

    ZabUtil::SetNoneBlockFD(sock);

    addConnection(sock);
  }

  //called when accept/listen error occurs
  void QuorumCnxMgr::onError() {
    ERROR("Get error when listen for connection, retry "<<numRetries);
    numRetries++;
    event_base_loopbreak(ebase);
  }

  //called when fd can read
  void QuorumCnxMgr::onRecvMsg(int sock) {
    int64 sid = getSid(sock);
    if (sid == 0) {
      //new connection, we need to find out peer id first
      handleNewConnection(sock);
      return;
    }

    DEBUG("Get message from server "<<sid<<" on sock "<<sock);
    char msg[READ_BUF_LEN];
    memset(msg, 0, READ_BUF_LEN);
    int readnum = recv(sock, msg, READ_BUF_LEN, 0);
    DEBUG("Get message "<<readnum<<" from server "<<sid<<" on sock "<<sock);

    if ((readnum <= 0)) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") on sock "<<sock);
      close(sock);
      removeConnection(sock);
      removeSockBuf(sock);
      removePeer(sock);
      OpenScope {
        FOR_EACH_HANDLER(ElectionMsgHandlerInterface*, handlerSet, HandlePeerShutdown(sid));
      }
      return;
    }

    OpenScope {
      FOR_EACH_HANDLER(ElectionMsgHandlerInterface*, handlerSet, HandleIncomingPeerMsg(sid, msg, readnum));
    }
  }

  //called when send queue have message
  void QuorumCnxMgr::onMsgReady(int64 sid) {
    int fd = getSock(sid);
    if (fd == -1) {
      DEBUG("could not find connection for server "<<sid);
      return;
    }
    //create a write event
    addWriteEvent(fd);
  }

  //called when fd can write
  void QuorumCnxMgr::onMsgCandSend(int fd) {
    int64 sid = getSid(fd);
    if (sid != 0 ) {
      DEBUG("Ready to send message to peer "<<sid<<" on sock "<<fd);
      if (getLastMsgFlag(sid)) {
        ByteBuffer * lmsg = getLastMsg(sid);
        if (lmsg != NULL) {
          INFO("Try to send last message to server "<<sid<<" on sock "<<fd);
          if (send(fd, lmsg->Data(), lmsg->Length(), 0) < 0) {
            int err = EVUTIL_SOCKET_ERROR();
            ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") when sending message.");
            setLastMsg(sid, lmsg);
            removeConnection(fd);
            removeWriteEvent(fd);
            removePeer(fd);
            close(fd);
          } else {
            delete lmsg;
          }
        }
      }

      CircleByteBufferQueue* q = getQueue(sid);
      ByteBuffer* m = NULL;

      while ((m = q->Front()) != NULL) {
        int sendNum = send(fd, m->Data(), m->Length(), 0);
        if (sendNum < 0) {
          int err = EVUTIL_SOCKET_ERROR();
          ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") when sending message.");

          removeConnection(fd);
          removeWriteEvent(fd);
          removePeer(fd);
          close(fd);
          return;
        }else  if (sendNum < (int) m->Length()) {
          WARN("try to send "<<m->Length()<<" but actually sent "<<sendNum<<" on sock "<<fd);
          m->Shift(sendNum);
          return;
        }
        DEBUG("Successfully sent out "<<m->Length());
        //data was sent successfully,set last message and pop front node
        q->PopFront();
        setLastMsg(sid, m);
      }

      if (q->Empty()) {
        //we have send out all the message, remove retry event
        DEBUG("All message delivered, remove write event");
        removeWriteEvent(fd);
      }
    } else {
      ERROR("Could not find server on connection "<<fd);
      removeWriteEvent(fd);
    }
  }

  void QuorumCnxMgr::addConnection(int fd) {
    AutoLock guard(eventLock);
    EventMap::iterator iter = eventMap.find(fd);
    if (iter == eventMap.end()) {
      ZabUtil::SetNoneBlockFD(fd);
      struct event* e = event_new(ebase, fd, EV_READ|EV_PERSIST, &QuorumCnxMgr::OnLibEventNotification, this);
      event_add(e, NULL);
      eventMap.insert(pair<int, struct event*>(fd, e));
    } else {
      WARN("Event already existed for sock "<<fd);
    }
  }

  void QuorumCnxMgr::removeConnection(int fd) {
    AutoLock guard(eventLock);
    EventMap::iterator iter = eventMap.find(fd);
    if (iter != eventMap.end()) {
      struct event * e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
      eventMap.erase(iter);
    }
  }

  void QuorumCnxMgr::handleNewConnection(int sock) {
    INFO("We got new connection on "<<sock);
    char msg[sizeof(int64)];
    memset(msg, 0, sizeof(int64));
    string * buf = getSockBuf(sock);

    //only read max to sizeof(int64) to get sid
    int readnum = recv(sock, msg, sizeof(int64) - buf->length(), 0);
    if (readnum <= 0 ) {
      ERROR("failed to receive message from peer on "<<sock);
      removeConnection(sock);
      removeSockBuf(sock);
      close(sock);
      return;
    }
    buf->append(msg, readnum);

    if (buf->length() == sizeof(int64)) {
      int64 remoteId = 0;
      ByteBuffer b;
      b.WriteBytes(buf->data(), buf->length());
      b.ReadInt64(remoteId);

      if (remoteId < peerConfig->serverId) {
        INFO("Create new connection to server: "<<remoteId<<", drop current one on "<<sock);
        removeConnection(sock);
        removeSockBuf(sock);
        close(sock);
        connectOne(remoteId);
      } else {
        addPeer(sock, remoteId);
        buf->clear();
      }
    }
  }

  void QuorumCnxMgr::addPeer(int fd, int64 sid) {
    INFO("Add new peer "<<sid<<" on sock "<<fd);
    AutoLock guard(peerLock);
    sock2SidMap[fd] = sid;
    sid2SockMap[sid] = fd;
    lastMsgFlag.insert(sid);
    msgReady(sid);
  }

  void QuorumCnxMgr::removePeer(int fd) {
    INFO("Try to Remove peer "<<" on sock "<<fd);
    AutoLock guard(peerLock);
    Sock2SidMap::iterator iter = sock2SidMap.find(fd);
    if (iter != sock2SidMap.end()) {
      INFO("Removed peer "<<iter->second<<" on sock "<<fd);
      sid2SockMap.erase(iter->second);
      lastMsgFlag.erase(iter->second);
      sock2SidMap.erase(iter);
    }
  }

  bool QuorumCnxMgr::getLastMsgFlag(int64 sid) {
    bool ret = false;
    AutoLock guard(peerLock);
    LastMsgFlag::iterator iter = lastMsgFlag.find(sid);
    if (iter != lastMsgFlag.end()) {
      ret = true;
      lastMsgFlag.erase(iter);
    }
    return ret;
  }

  void QuorumCnxMgr::msgReady(int64 sid) {
    if (msg_pipe[PIPE_IN] != -1) {
      int64 s = sid;
      write(msg_pipe[PIPE_IN], &s, sizeof(s));
    }
  }
  void QuorumCnxMgr::setLastMsg(int64 sid, ByteBuffer* m) {
    ByteBuffer * b = NULL;
    LastMessageMap::iterator iter = lastMessageSent.find(sid);
    if (iter != lastMessageSent.end()) {
      b = iter->second;
      if (b != NULL) {
        delete b;
      }
      iter->second = m;
    } else {
      lastMessageSent.insert(pair<int64, ByteBuffer*>(sid, m));
    }
  }

  ByteBuffer* QuorumCnxMgr::getLastMsg(int64 sid) {
    ByteBuffer * b = NULL;
    LastMessageMap::iterator iter = lastMessageSent.find(sid);
    if (iter != lastMessageSent.end()) {
      b = iter->second;
      iter->second = NULL;
    }
    return b;
  }

  int64 QuorumCnxMgr::getSid(int sock) {
    int64 sid = 0;
    AutoLock guard(peerLock);
    Sock2SidMap::iterator iter = sock2SidMap.find(sock);
    if (iter != sock2SidMap.end()) {
      sid = iter->second;
    }
    return sid;
  }

  int QuorumCnxMgr::getSock(int64 sid) {
    int fd = -1;
    AutoLock guard(peerLock);
    Sid2SockMap::iterator iter = sid2SockMap.find(sid);
    if (iter != sid2SockMap.end()) {
      fd = iter->second;
    }
    return fd;
  }

  string * QuorumCnxMgr::getSockBuf(int sock) {
    string * ret = NULL;
    SockBufMap::iterator iter = sockBufMap.find(sock);
    if (iter != sockBufMap.end()) {
      ret = iter->second;
    } else {
      ret = new string();
      sockBufMap.insert(pair<int, string*>(sock, ret));
    }
    return ret;
  }

  void QuorumCnxMgr::removeSockBuf(int sock) {
    SockBufMap::iterator iter = sockBufMap.find(sock);
    if (iter != sockBufMap.end()) {
      string * s = iter->second;
      if (s != NULL) {
        delete s;
      }
      sockBufMap.erase(iter);
    }
  }

  CircleByteBufferQueue* QuorumCnxMgr::getQueue(int64 sid) {
    CircleByteBufferQueue * q = NULL;
    AutoLock guard(sendQueueLock);
    SendQueueMap::iterator iterQ = sendQueueMap.find(sid);
    if (iterQ != sendQueueMap.end()) {
      q = iterQ->second;
    } else {
      INFO("no send queue for server "<<sid<<" create one");
      q = new CircleByteBufferQueue(SEND_CAPACITY);
      sendQueueMap[sid] = q;
    }
    return q;
  }

  void QuorumCnxMgr::addWriteEvent(int fd) {
    EventMap::iterator iter = wEventMap.find(fd);
    if (iter == wEventMap.end()) {
      struct event *e = event_new(ebase, fd, EV_WRITE|EV_PERSIST, &QuorumCnxMgr::OnLibEventNotification, this);
      event_add(e, NULL);
      wEventMap.insert(pair<int, struct event*>(fd, e));
    }else {
      WARN("Try-event already exists for connection "<<fd);
    }
  }

  void QuorumCnxMgr::removeWriteEvent(int fd) {
    EventMap::iterator iter = wEventMap.find(fd);
    if (iter != wEventMap.end()) {
      struct event * e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
      wEventMap.erase(iter);
    }
  }

  void QuorumCnxMgr::setupPipe(int * pipeA) {
    //setup wakeup channel;
    if (pipe(pipeA)) {
      ERROR("Could not create pipe");
      return;
    }

    ZabUtil::SetNoneBlockFD((pipeA)[0]);
    ZabUtil::SetNoneBlockFD((pipeA)[1]);
    fcntl((pipeA)[0], F_SETFL, O_NOATIME);
    return;
  }

  void QuorumCnxMgr::cleanupPipe(int * pipeA) {
    if (pipeA[PIPE_OUT] != -1) {
      close(pipeA[PIPE_OUT]);
    }

    if (pipeA[PIPE_IN] != -1) {
      close(pipeA[PIPE_IN]);
    }
  }

  void QuorumCnxMgr::cleanupSockBuf() {
    for(SockBufMap::iterator iter = sockBufMap.begin();
        iter != sockBufMap.end();
        iter ++) {
      string * b = iter->second;
      if (b != NULL) {
        delete b;
      }
    }
    sockBufMap.clear();
  }

  void QuorumCnxMgr::cleanupEvents(EventMap & eMap) {
    AutoLock guard(eventLock);
    for(EventMap::iterator iter = eMap.end();
        iter != eMap.end();
        iter ++) {
      struct event * e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
    }
    eMap.clear();
  }

  void QuorumCnxMgr::cleanupAllSendQueue() {
    AutoLock guard(sendQueueLock);
    for(SendQueueMap::iterator iter = sendQueueMap.begin();
        iter != sendQueueMap.end();
        iter ++) {
      CircleByteBufferQueue * q = iter->second;
      if (q != NULL) {
        delete q;
      }
    }
    sendQueueMap.clear();
  }

  void QuorumCnxMgr::cleanupLastMsg() {
    for (LastMessageMap::iterator iter = lastMessageSent.begin();
        iter != lastMessageSent.end();
        iter++) {
      ByteBuffer * m = iter->second;
      if (m != NULL) {
        delete m;
      }
    }
    lastMessageSent.clear();
  }

}

