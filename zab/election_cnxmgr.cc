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


  //------------------------Send Worker
  SendWorker::SendWorker()
      : Thread("Event send worker"), ebase(NULL) {
    wakeup_pipe[PIPE_OUT] = -1;
    wakeup_pipe[PIPE_IN] = -1;
    msg_pipe[PIPE_OUT] = -1;
    msg_pipe[PIPE_IN] = -1;
  }

  SendWorker::~SendWorker() {
    if (ebase != NULL) {
      event_base_free(ebase);
    }
  }

  void SendWorker::OnLibEventNotification(evutil_socket_t fd, short what, void *arg) {
    SendWorker* sw = static_cast<SendWorker*>(arg);
    if (what & EV_READ) {
      int64 sid;
      read(fd, &sid, sizeof(sid));
      DEBUG("Message ready for sending, sid "<<sid);
      sw->OnCanSend(sid);
    }
  }
  void SendWorker::OnWakeup(evutil_socket_t fd, short what, void *arg) {
    INFO("Waked up and break event loop");
    char c = 0;
    read(fd, &c, 1);
    struct event_base* b = (struct event_base*) arg;
    event_base_loopbreak(b);
  }

  bool SendWorker::IsAllDelivered() {
    bool ret = true;
    AutoLock guard(sendQueueLock);
    for (sendQueueMap::iterator iter = i_sendQueueMap.begin(); iter != i_sendQueueMap.end(); iter++) {
      if (!iter->second->Empty()) {
        ret = false;
        break;
      }
    }
    return ret;
  }

  void SendWorker::AddPeer(int fd, int64 sid) {
    INFO("SendWorker::Try to add new connection fd "<<fd<<" sid "<<sid);
    AutoLock guard(mapLock);
    ZabUtil::SetNoneBlockFD(fd);
    sid2SockMap::iterator iter = i_sid2SockMap.find(sid);
    if (iter == i_sid2SockMap.end()) {
      i_sid2SockMap.insert(pair<int64, int>(sid, fd));
      i_lastMsgFlag.insert(sid);
      i_sock2SidMap[fd] = sid;
      INFO("SendWorker::Added new connection fd "<<fd<<" sid "<<sid);
      msgReady(sid);
    }
  }

  void SendWorker::RemovePeer(int fd) {
    INFO("SendWorker::Try to remove connection on fd "<<fd);
    AutoLock guard(mapLock);
    sock2SidMap::iterator iter = i_sock2SidMap.find(fd);
    if (iter != i_sock2SidMap.end()) {
      INFO("SendWorker::Removed  connection fd "<<fd<<" for server "<<iter->second);
      i_sid2SockMap.erase(iter->second);
      i_sock2SidMap.erase(iter);
    }
  }

  void SendWorker::Init() {
    if (ebase == NULL){
      ebase = event_base_new();
    }

    setupPipe(wakeup_pipe);
    struct event* e_wakeup = event_new(ebase, wakeup_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &SendWorker::OnWakeup, ebase);
    event_add(e_wakeup, NULL);
    i_eventSet.insert(e_wakeup);

    setupPipe(msg_pipe);
    struct event* e_msg = event_new(ebase, msg_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &SendWorker::OnLibEventNotification, this);
    event_add(e_msg, NULL);
    i_eventSet.insert(e_msg);
  }

  void SendWorker::Run() {
    INFO("Send Worker Started");

    while (!stopping_) {
      event_base_dispatch(ebase);
      INFO("SendWorker::event loop exit");
      if (stopping_)
        return;
    }
  }

  void SendWorker::CleanUp() {
    cleanupEvents();
    if (ebase != NULL) {
      event_base_free(ebase);
    }
    cleanAllLastMsg();
    cleanAllQueue();
    cleanupPipe(wakeup_pipe);
    cleanupPipe(msg_pipe);
  }

  void SendWorker::ShuttingDown(){
    INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
    if (wakeup_pipe[PIPE_IN] != -1) {
      char c = 0;
      write(wakeup_pipe[PIPE_IN], &c, 1);
    }
  }

  void SendWorker::msgReady(int64 sid) {
    //todo should I add a lock here
    if (msg_pipe[PIPE_IN] != -1) {
      int64 s = sid;
      write(msg_pipe[PIPE_IN], &s, sizeof(s));
    }
  }

  void SendWorker::OnCanSend(int64 sid) {
    int fd = getSock(sid);
    if (fd == -1) {
      DEBUG("could not find connection for server "<<sid);
      return;
    }

    bool lFlag = false;
    {
      AutoLock guard(mapLock);
      lastMsgFlag::iterator iter = i_lastMsgFlag.find(sid);
      if (iter != i_lastMsgFlag.end()) {
        lFlag = true;
        i_lastMsgFlag.erase(iter);
      }
    }

    if (lFlag) {
      ByteBuffer * lmsg = getLastMsg(sid);
      if (lmsg != NULL) {
        INFO("Try to send last message to server "<<sid);
        i_sendMsg(fd, sid, lmsg);
        delete lmsg;
      }
    }

    CircleByteBufferQueue* q = getQueue(sid);
    ByteBuffer* m = NULL;
    while ((m = q->PopFront()) != NULL) {
      i_sendMsg(fd, sid, m);
      setLastMsg(sid, m);
    }
  }

  //lock free;
  void SendWorker::setLastMsg(int64 sid, ByteBuffer* msg) {
    ByteBuffer * b = NULL;
    lastMessageMap::iterator iter = i_lastMessageSent.find(sid);
    if (iter != i_lastMessageSent.end()) {
      b = iter->second;
      delete b;
      iter->second = msg;
    } else {
      i_lastMessageSent.insert(lastMsgPair(sid, msg));
    }
  }

  ByteBuffer* SendWorker::getLastMsg(int64 sid) {
    ByteBuffer * b = NULL;
    lastMessageMap::iterator iter = i_lastMessageSent.find(sid);
    if (iter != i_lastMessageSent.end()) {
      b = iter->second;
      iter->second = NULL;
    }
    return b;
  }

  void SendWorker::i_sendMsg(int sock, int64 sid, ByteBuffer* msg) {
    TRACE("SendWorker:try to send "<<msg->Length()<<" to sever "<<sid<<" on sock "<<sock);
    int32 packetLen = HostToNetwork32(msg->Length());
    int sendNum = send(sock, &packetLen, sizeof(packetLen), 0);
    if (sendNum < 0) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") when sending message.");
      RemovePeer(sock);
      close(sock);
      return;
    }

    if (sendNum != sizeof(packetLen)) {
      ERROR("try to send "<<sizeof(packetLen)<<" but actually sent "<<sendNum<<" on sock "<<sock);
      return;
    }
    TRACE("SendWorker::sent "<<sendNum<<" to server "<<sid<<" on sock "<<sock);
    //send remain message
    sendNum = send(sock, msg->Data(), msg->Length(), 0);
    if (sendNum < 0) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") when sending message.");
      RemovePeer(sock);
      close(sock);
      return;
    }

    if (sendNum != (int) msg->Length()) {
      ERROR("try to send "<<msg->Length()<<" but actually sent "<<sendNum<<" on sock "<<sock);
    }
    TRACE("SendWorker::sent "<<sendNum<<" to server "<<sid<<" on sock "<<sock);
  }

  void SendWorker::cleanAllLastMsg() {
    for (lastMessageMap::iterator iter = i_lastMessageSent.begin(); iter != i_lastMessageSent.end(); iter++) {
      ByteBuffer * m = iter->second;
      if (m != NULL) {
        delete m;
        iter->second = NULL;
      }
    }
    i_lastMessageSent.clear();
  }

  //lock needed
  void SendWorker::cleanAllQueue() {
    AutoLock guard(sendQueueLock);
    for (sendQueueMap::iterator iter = i_sendQueueMap.begin(); iter != i_sendQueueMap.end(); iter++) {
      CircleByteBufferQueue * q = iter->second;
      if (q != NULL) {
        delete q;
        iter->second = NULL;
      }
    }
    i_sendQueueMap.clear();
  }

  CircleByteBufferQueue* SendWorker::getQueue(int64 sid) {
    CircleByteBufferQueue * q = NULL;
    AutoLock guard(sendQueueLock);
    sendQueueMap::iterator iterQ = i_sendQueueMap.find(sid);
    if (iterQ != i_sendQueueMap.end()) {
      q = iterQ->second;
    } else {
      INFO("no send queue for server "<<sid<<" create one");
      q = new CircleByteBufferQueue(SEND_CAPACITY);
      i_sendQueueMap[sid] = q;
    }
    return q;
  }

  int SendWorker::getSock(int64 sid) {
    AutoLock guard(mapLock);
    int ret = -1;
    sid2SockMap::iterator iter = i_sid2SockMap.find(sid);
    if (iter != i_sid2SockMap.end()) {
      ret = iter->second;
    }
    return ret;
  }

  int64 SendWorker::getSid(int sock) {
    int64 ret = 0;
    AutoLock guard(mapLock);
    sock2SidMap::iterator iter = i_sock2SidMap.find(sock);
    if (iter != i_sock2SidMap.end()) {
      ret = iter->second;
    }
    return ret;
  }

  bool SendWorker::setupPipe(int * pipeA) {
    //setup wakeup channel;
    if (pipe(pipeA) != 0) {
      ERROR("Could not create pipe");
      return false;
    }

    ZabUtil::SetNoneBlockFD((pipeA)[0]);
    ZabUtil::SetNoneBlockFD((pipeA)[1]);
    fcntl((pipeA)[0], F_SETFL, O_NOATIME);
    return true;
  }

  void SendWorker::cleanupPipe(int * pipeA) {
    if (pipeA[PIPE_OUT] != -1) {
      close(pipeA[PIPE_OUT]);
    }

    if (pipeA[PIPE_IN] != -1) {
      close(pipeA[PIPE_IN]);
    }
  }

  void SendWorker::cleanupEvents() {
    for(EventSet::iterator iter = i_eventSet.begin();
        iter != i_eventSet.end();
        iter ++) {
      struct event* e = *iter;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
    }
    i_eventSet.clear();
  }
  //-------------------------Recv Worker
  RecvWorker::RecvWorker(QuorumCnxMgr* pMgr)
      : Thread("Event Recv Worker"), pCnxMgr(pMgr), ebase(NULL) {
    wakeup_pipe[PIPE_OUT] = -1;
    wakeup_pipe[PIPE_IN] = -1;
  }

  RecvWorker::~RecvWorker() {

  }


  void RecvWorker::AddPeer(int fd, int64 sid) {
    INFO("RecvWorker::Try to add new connection fd "<<fd<<" sid "<<sid);
    AutoLock guard(mapLock);
    ZabUtil::SetNoneBlockFD(fd);
    eventMap::iterator iter = eMap.find(fd);
    if (iter == eMap.end()) {
      struct event* e = event_new(ebase, fd, EV_READ | EV_PERSIST, &RecvWorker::OnLibEventNotification, this);
      if (e != NULL) {
        eMap.insert(iter, eventMapPair(fd, e));
        event_add(e, NULL);
        INFO("RecvWorker::Added new connection fd "<<fd<<" sid "<<sid);
      }
    }
    i_sock2SidMap[fd] = sid;
    i_conSet.insert(sid);
  }

  void RecvWorker::RemovePeer(int fd) {
    INFO("RecvWorker::Try to remove connection on sock "<<fd);
    AutoLock guard(mapLock);
    eventMap::iterator iter = eMap.find(fd);
    if (iter != eMap.end()) {
      struct event* e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
        INFO("RecvWorker::Removed event on sock "<<fd);
      }
      eMap.erase(iter);
    }
    Sock2SidMap::iterator iterS = i_sock2SidMap.find(fd);
    if (iterS != i_sock2SidMap.end()) {
      i_conSet.erase(iterS->second);
      i_sock2SidMap.erase(iterS);
      INFO("RecvWorker::Removed connection on sock "<<fd);
    }
  }

  void RecvWorker::Init() {
    if (ebase == NULL) {
      ebase = event_base_new();
    }

    //setup wakeup channel;
    if (pipe(wakeup_pipe) != 0) {
      ERROR("Could not create wake up pipe, quit");
      return;
    }

    ZabUtil::SetNoneBlockFD(wakeup_pipe[0]);
    ZabUtil::SetNoneBlockFD(wakeup_pipe[1]);
    struct event* e = event_new(ebase, wakeup_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &RecvWorker::OnWakeup, ebase);
    event_add(e, NULL);
    eMap[wakeup_pipe[PIPE_OUT]] = e;
  }

  void RecvWorker::Run() {
    while (!stopping_) {
      event_base_dispatch(ebase);
      INFO("event loop exit");
      if (stopping_)
        return;
    }
  }

  void RecvWorker::CleanUp() {
    cleanupEvents();
    cleanupSockBuf();

    if (ebase != NULL) {
      event_base_free(ebase);
    }

    if (wakeup_pipe[PIPE_OUT] != -1) {
      close(wakeup_pipe[PIPE_OUT]);
    }

    if (wakeup_pipe[PIPE_IN] != -1) {
      close(wakeup_pipe[PIPE_IN]);
    }
  }

  void RecvWorker::ShuttingDown() {
    INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
    if (wakeup_pipe[PIPE_IN] != -1) {
      char c = 0;
      write(wakeup_pipe[PIPE_IN], &c, 1);
    }
  }

  string* RecvWorker::getSockBuf(int fd) {
    string * buf = NULL;
    SockBufMap::iterator iter = i_sockBufMap.find(fd);
    if (iter != i_sockBufMap.end()) {
      buf = iter->second;
    } else {
      buf = new string();
      i_sockBufMap.insert(pair<int, string*>(fd, buf));
    }
    return buf;
  }

  void RecvWorker::removeSockBuf(int fd) {
    SockBufMap::iterator iter = i_sockBufMap.find(fd);
    if (iter != i_sockBufMap.end()) {
      string * b = iter->second;
      if (b != NULL) {
        delete b;
      }
      i_sockBufMap.erase(iter);
    }
  }

  void RecvWorker::cleanupEvents() {
    AutoLock guard(mapLock);
    for (eventMap::iterator iter = eMap.begin(); iter != eMap.end(); iter++) {
      struct event* e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
      close(iter->first);
      iter->second = NULL;
    }
    eMap.clear();
    i_sock2SidMap.clear();
  }

  void RecvWorker::cleanupSockBuf() {
    for(SockBufMap::iterator iter = i_sockBufMap.begin();
        iter != i_sockBufMap.end();
        iter ++) {
      string * b = iter->second;
      if (b != NULL) {
        delete b;
      }
      iter->second = NULL;
    }
    i_sockBufMap.clear();
  }

  void RecvWorker::processMsg(int fd, string* buf) {
    //parse and validated buffer
    while (sizeof(uint32) <= buf->length()) {
      ByteBuffer byteBuf(buf->data(), buf->length());
      int32 packetLen;
      byteBuf.ReadInt32(packetLen);
      if ((0 < packetLen) && (packetLen < PACKETMAXSIZE)) {
        int totalLen = sizeof(uint32) + packetLen;
        if (totalLen <= (int) buf->length()) {
          Message* newMsg = new Message();
          newMsg->sid = i_sock2SidMap[fd];
          newMsg->buffer.WriteString(buf->substr(sizeof(uint32), packetLen));
          buf->erase(0, totalLen);
          pCnxMgr->addToRecvQueue(newMsg);
        } else {
          TRACE("Message was only "<<buf->length()<<" required "<<totalLen);
          return;
        }
      }
    }
  }

  void RecvWorker::onRecvMsg(int sock) {
    TRACE("Recv message from "<<sock<<" sid "<<i_sock2SidMap[sock]);

    char msg[READ_BUF_LEN];
    memset(msg, 0, READ_BUF_LEN);
    int readnum = recv(sock, msg, READ_BUF_LEN, 0);

    if ((readnum == 0)) {
      ERROR("connection on sock "<<sock<<" was shutdown by peer");
      close(sock);
      RemovePeer(sock);
      removeSockBuf(sock);
      return;
    } else if (readnum < 0) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") on the RecvWorker.");
      close(sock);
      RemovePeer(sock);
      removeSockBuf(sock);
      return;
    }
    string * buf = getSockBuf(sock);
    buf->append(msg, readnum);
    processMsg(sock, buf);
  }

  void RecvWorker::OnWakeup(evutil_socket_t fd, short what, void *arg) {
    INFO("Waked up and break event loop");
    char c = 0;
    read(fd, &c, 1);
    struct event_base* b = (struct event_base*) arg;
    event_base_loopbreak(b);
  }

  void RecvWorker::OnLibEventNotification(evutil_socket_t fd, short int what, void * arg) {
    RecvWorker* self = static_cast<RecvWorker*>(arg);
    if (what & EV_READ) {
      self->onRecvMsg(fd);
    }
  }

  //--------------------------Listener
  Listener::~Listener() {

  }

  void Listener::ShuttingDown(){
    INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
    if (wakeup_pipe[PIPE_IN] != -1) {
      char buf = 0;
      write(wakeup_pipe[PIPE_IN], &buf, 1);
    }
  }

  void Listener::OnLibEventNotification(evutil_socket_t fd, short what, void *arg) {
    Listener * l = static_cast<Listener*>(arg);
    if (what & EV_READ) {
      l->onRecvMsg(fd);
    }
  }

  void Listener::OnLibEventListenerNotify(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address,
      int socklen, void *ctx) {
    Listener * l = static_cast<Listener*>(ctx);
    l->onAccept(fd);
  }

  void Listener::OnLibEventListenerError(struct evconnlistener *listener, void *ctx) {
    int err = EVUTIL_SOCKET_ERROR();
    ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") on the listener.");
    Listener * l = static_cast<Listener*>(ctx);
    l->onError();
  }

  void Listener::OnWakeup(evutil_socket_t fd, short what, void *arg) {
    INFO("Waked up and break event loop");
    char c = 0;
    read(fd, &c, 1);
    struct event_base* b = (struct event_base*) arg;
    event_base_loopbreak(b);
  }

  void Listener::onError() {
    numRetries++;
    event_base_loopbreak(ebase);
  }

  void Listener::onAccept(int sock) {
    int flag = 1;
    setsockopt(sock, /* socket affected */
    IPPROTO_TCP, /* set option at TCP level */
    TCP_NODELAY, /* name of option */
    (char *) &flag, /* the cast is historical cruft */
    sizeof(int)); /* length of option value */

    ZabUtil::SetNoneBlockFD(sock);
    addEvent(sock);
  }

  void Listener::onRecvMsg(int sock) {
    char msg[sizeof(int64)];
    memset(msg, 0, sizeof(int64));
    string * buf = getSockBuf(sock);

    //only read max to sizeof(int64) to get sid
    //all other data should be read and handle by recvworker;
    int readnum = recv(sock, msg, sizeof(int64) - buf->length(), 0);
    if (readnum <= 0 ) {
      ERROR("failed to receive message from peer on "<<sock);
      removeEvent(sock);
      removeSockBuf(sock);
      close(sock);
      return;
    }

    buf->append(msg, readnum);
    if (buf->length() == sizeof(int64)) {
      //get enough data and remove event immediately
      removeEvent(sock);

      int64 remoteId = 0;
      ByteBuffer b;
      b.WriteBytes(buf->data(), buf->length());
      b.ReadInt64(remoteId);
      pCnxMgr->receiveConnection(sock, remoteId);

      //remove sock buf
      removeSockBuf(sock);
    }
  }

  void Listener::addEvent(int sock) {
    EventMap::iterator iter = eventMap.find(sock);
    if (iter != eventMap.end()) {
      struct event * e = iter->second;
      if (e != NULL){
        event_del(e);
        event_free(e);
      }
      e = event_new(ebase, sock, EV_READ|EV_PERSIST, &Listener::OnLibEventNotification, this);
      event_add(e, NULL);
      iter->second = e;
    } else {
      struct event * ne = event_new(ebase, sock, EV_READ|EV_PERSIST, &Listener::OnLibEventNotification, this);
      event_add(ne, NULL);
      eventMap.insert(pair<int, struct event *>(sock, ne));
    }
  }

  void Listener::removeEvent(int sock) {
    EventMap::iterator iter = eventMap.find(sock);
    if (iter != eventMap.end()) {
      struct event * e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
      eventMap.erase(iter);
    }
  }

  string* Listener::getSockBuf(int sock) {
    string * ret = NULL;
    SockBuf::iterator iter = sockBuf.find(sock);
    if (iter != sockBuf.end()) {
      ret = iter->second;
    } else {
      ret = new string();
      sockBuf.insert(pair<int, string*>(sock, ret));
    }
    return ret;
  }

  void Listener::removeSockBuf(int sock) {
    SockBuf::iterator iter = sockBuf.find(sock);
    if (iter != sockBuf.end()) {
      string * s = iter->second;
      if (s != NULL) {
        delete s;
      }
      sockBuf.erase(iter);
    }
  }

  void Listener::cleanupEvents() {
    for (EventMap::iterator iter = eventMap.begin();
        iter != eventMap.end();
        iter++) {
      struct event * e = iter->second;
      if (e != NULL) {
        event_del(e);
        event_free(e);
      }
    }
    eventMap.clear();
  }

  void Listener::cleanupSockBuf() {
    for (SockBuf::iterator iter = sockBuf.begin();
        iter != sockBuf.end();
        iter ++) {
      string * s = iter->second;
      if (s != NULL) {
        delete s;
      }
    }
    sockBuf.clear();
  }

  void Listener::Init() {
    if (ebase == NULL) {
      ebase = event_base_new();
    }
    numRetries = 0;
  }

  void Listener::Run() {
    struct sockaddr_in servSin;

    int64 myId = pCnxMgr->getPeerConfig()->serverId;
    const QuorumServer info = pCnxMgr->getPeerConfig()->servers[myId];

    INFO("Listener "<<myId<< " on port "<<info.electionPort<<" started");

    //setup poll here
    if (pipe(wakeup_pipe)) {
      ERROR("Could not create wake up pipe, quit");
      return;
    }

    ZabUtil::SetNoneBlockFD(wakeup_pipe[0]);
    ZabUtil::SetNoneBlockFD(wakeup_pipe[1]);

    struct event* e = event_new(ebase, wakeup_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &Listener::OnWakeup, ebase);
    event_add(e, NULL);

    while (!stopping_ && (numRetries < MAX_LISTENER_RETIES)) {
      memset(&servSin, 0, sizeof(servSin)); /* Zero out structure */
      servSin.sin_family = AF_INET; /* Internet address family */
      servSin.sin_addr.s_addr = htonl(INADDR_ANY); /* Any incoming interface */
      servSin.sin_port = htons(info.electionPort); /* Local port */

      struct evconnlistener *listener = evconnlistener_new_bind(ebase, &Listener::OnLibEventListenerNotify, this,
          LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1, (struct sockaddr*) &servSin, sizeof(servSin));

      if (!listener) {
        ERROR("Couldn't create listener");
        numRetries++;
        continue;
      }

      evconnlistener_set_error_cb(listener, &Listener::OnLibEventListenerError);
      event_base_dispatch(ebase);
      evconnlistener_free(listener);
      listener = NULL;
      WARN("event loop break, retry times "<<numRetries);
      if (stopping_)
        return;
    }
    INFO("Listener "<<myId<< " on port "<<info.electionPort<<" quit, retry times "<<numRetries);
  }

  void Listener::CleanUp() {
    cleanupEvents();
    cleanupSockBuf();

    if (wakeup_pipe[PIPE_OUT] != -1) {
      close(wakeup_pipe[PIPE_OUT]);
    }

    if (wakeup_pipe[PIPE_IN] != -1) {
      close(wakeup_pipe[PIPE_IN]);
    }

    if (ebase != NULL) {
      event_base_free(ebase);
    }
  }

  //---------------------------QuorumCnxMgr
  QuorumCnxMgr::QuorumCnxMgr(QuorumPeerConfig* Cfg)
      : i_recvQueue(RECV_CAPACITY)
  , peerConfig(Cfg)
  , i_Listener(this)
  , i_RecvWorker(this)
  , weRecv(false, false){

  }

  QuorumCnxMgr::~QuorumCnxMgr() {
    Stop();
  }

  bool QuorumCnxMgr::isConExists(int64 sid) {
    return i_RecvWorker.IsConExists(sid);
  }

  void QuorumCnxMgr::setupNewCon(int sock, int64 sid) {
    INFO("setup send/recieve worker for server "<<sid<<" on "<<sock);
    ZabUtil::SetNoneBlockFD(sock);

    i_SendWorker.AddPeer(sock, sid);
    i_RecvWorker.AddPeer(sock, sid);
  }

  void QuorumCnxMgr::Start() {
    i_SendWorker.Start();
    i_Listener.Start();
    i_RecvWorker.Start();
  }

  void QuorumCnxMgr::Stop() {
    i_Listener.Stop();
    i_SendWorker.Stop();
    i_RecvWorker.Stop();
  }

  void QuorumCnxMgr::toSend(int64 sid, ByteBuffer* buf) {
    //todo, need a lock here??
    //AutoLock guard(sendLock);
    if (sid == peerConfig->serverId) {
      Message * msg = new Message();
      msg->buffer.WriteBytes(buf->Data(), buf->Length());
      msg->sid = sid;
      delete buf;
      addToRecvQueue(msg);
    } else {
      i_SendWorker.SendMsg(sid, buf);
    }
  }

  void QuorumCnxMgr::connectOne(int64 sid) {
    INFO("connecting to server "<<sid);
    AutoLock guard(connectLock);
    if (isConExists(sid)) {
      DEBUG("There is a connection already for server "<<sid);
      return;
    }

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
    } else {

    }
  }

  void QuorumCnxMgr::connectAll() {
    for (QuorumServerMap::iterator iter = peerConfig->servers.begin(); iter != peerConfig->servers.end(); iter++) {
      connectOne(iter->second.id);
    }
  }

  bool QuorumCnxMgr::initiateConnection(int sock, int64 sid) {
    bool ret = true;
    //immediately write our sid to remote;
    int64 nsid = (int64) HostToNetwork64(peerConfig->serverId);
    int sendNum = send(sock, &nsid, sizeof(nsid), 0);
    if (sendNum < 0) {
      int err = EVUTIL_SOCKET_ERROR();
      ERROR("Got an error "<<err<<" ("<<evutil_socket_error_to_string(err)<<") when sending message.");
      close(sock);
      return false;
    }

    if (sendNum != sizeof(nsid)) {
      INFO("try to sent "<<sizeof(nsid)<<" but actually sent "<<sendNum);
    }

    if (sid > peerConfig->serverId) {
      INFO("I Have smaller server identifier, so dropping the " <<"connection: ("<<sid << ", "<<peerConfig->serverId <<")");
      close(sock);
      ret = false;
    } else {
      // create sender and recv worker
      setupNewCon(sock, sid);
    }
    return ret;
  }

  bool QuorumCnxMgr::receiveConnection(int sock, int64 sid) {
    bool ret = true;
    DEBUG("received new connection from server "<<sid);
    if (sid < peerConfig->serverId) {
      /*
       * This replica might still believe that the connection to sid is
       * up, so we have to shut down the workers before trying to open a
       * new connection.
       */
      i_SendWorker.RemovePeer(sock);
      i_RecvWorker.RemovePeer(sock);
      /*
       * Now we start a new connection
       */
      INFO("Create new connection to server: "<<sid<<", drop current one on "<<sock);
      close(sock);
      connectOne(sid);
    } else {
      setupNewCon(sock, sid);
    }
    return ret;
  }

  Message* QuorumCnxMgr::pollRecvQueue(int64 max_time_in_ms) {
    Message * msg = i_recvQueue.PopFront();
    if (msg == NULL) {
      if (0 < max_time_in_ms) {
        weRecv.TimedWait(max_time_in_ms);
      } else {
        weRecv.Wait();
      }
      msg = i_recvQueue.PopFront();
    }
    return msg;
  }

  void QuorumCnxMgr::addToRecvQueue(Message* msg) {
    i_recvQueue.PushBack(msg);
    weRecv.Signal();
  }
}
;
