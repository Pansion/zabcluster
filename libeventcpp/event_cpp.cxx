/*
 * event_cpp.cxx
 *
 *  Created on: Jun 28, 2012
 *      Author: pchen
 */

#include "event_cpp.h"
#include "base/basictypes.h"
#include "base/logging.h"
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
static int set_noneblocking_fd(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    flags = 0;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
RecvWorker::RecvWorker()
    : Thread("Event Recv Worker"), ebase(NULL) {
  wakeup_pipe[PIPE_OUT] = -1;
  wakeup_pipe[PIPE_IN] = -1;
  ebase = event_base_new();
}

RecvWorker::~RecvWorker() {
  if (ebase != NULL) {
    event_base_free(ebase);
  }
}

void RecvWorker::WakeupAndQuit() {
  INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
  StopSoon();
  if (wakeup_pipe[PIPE_IN] != -1) {
    char c = 0;
    write(wakeup_pipe[PIPE_IN], &c, 1);
  }
  Stop();
}

void RecvWorker::AddPeer(int fd) {
  AutoLock guard(mapLock);
  set_noneblocking_fd(fd);
  eventMap::iterator iter = eMap.find(fd);
  if (iter == eMap.end()) {
    struct event* e = event_new(ebase, fd, EV_READ | EV_PERSIST, &RecvWorker::OnLibEventNotifiction, this);
    if (e != NULL) {
      eMap.insert(iter, eventMapPair(fd, e));
      event_add(e, NULL);
    }
  }
}

void RecvWorker::RemovePeer(int fd) {
  AutoLock guard(mapLock);
  eventMap::iterator iter = eMap.find(fd);
  if (iter != eMap.end()) {
    struct event* e = iter->second;
    if (e != NULL) {
      event_del(e);
      event_free(e);
    }
    eMap.erase(iter);
  }
}

void RecvWorker::Init() {

}

void RecvWorker::Run() {
  //setup wakeup channel;
  if (pipe(wakeup_pipe)) {
    ERROR("Could not create wake up pipe, quit");
    return;
  }

  set_noneblocking_fd(wakeup_pipe[0]);
  set_noneblocking_fd(wakeup_pipe[1]);
  struct event* e = event_new(ebase, wakeup_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &RecvWorker::OnWakeup, ebase);
  if (e != NULL) {
    event_add(e, NULL);
  } else {
    ERROR("Could not add wakeup channel, quit");
    return;
  }

  while (!stopping_) {
    event_base_dispatch(ebase);
    INFO("event loop exit");
    if (stopping_)
      return;
  }
  event_del(e);
  event_free(e);
}

void RecvWorker::Cleanup() {
  AutoLock guard(mapLock);
  for (eventMap::iterator iter = eMap.begin(); iter != eMap.end(); iter++) {
    struct event* e = iter->second;
    if (e != NULL) {
      event_del(e);
      event_free(e);
    }
    iter->second = NULL;
  }
  eMap.clear();

  if (wakeup_pipe[PIPE_OUT] != -1) {
    close(wakeup_pipe[PIPE_OUT]);
  }

  if (wakeup_pipe[PIPE_IN] != -1) {
    close(wakeup_pipe[PIPE_IN]);
  }
}

void RecvWorker::OnRecvMsg(int fd) {
  INFO("recv message from fd "<<fd);
  char buf[1024];
  int readNum = recv(fd, buf, 1024, 0);
  INFO("recv totally "<<readNum);
}

void RecvWorker::OnWakeup(evutil_socket_t fd, short what, void *arg) {
  INFO("Waked up and break event loop");
  char c = 0;
  read(fd, &c, 1);
  struct event_base* b = (struct event_base*) arg;
  event_base_loopbreak(b);
}

void RecvWorker::OnLibEventNotifiction(evutil_socket_t fd, short int what, void * arg) {
  RecvWorker* self = static_cast<RecvWorker*>(arg);
  if (what & EV_READ) {
    self->OnRecvMsg(fd);
  }
}

//---------------------sender worker
SendWorker::SendWorker()
    : Thread("Event send worker"), ebase(NULL) {
  wakeup_pipe[PIPE_OUT] = -1;
  wakeup_pipe[PIPE_IN] = -1;
  ebase = event_base_new();
}

SendWorker::~SendWorker() {
  if (ebase != NULL) {
    event_base_free(ebase);
  }
}

void SendWorker::OnLibEventNotifiction(evutil_socket_t fd, short what, void *arg) {
  SendWorker* sw = static_cast<SendWorker*>(arg);
  if (what & EV_WRITE) {
    sw->OnCanSend(fd);
  }
}
void SendWorker::OnWakeup(evutil_socket_t fd, short what, void *arg) {
  INFO("Waked up and break event loop");
  char c = 0;
  read(fd, &c, 1);
  struct event_base* b = (struct event_base*) arg;
  event_base_loopbreak(b);
}

void SendWorker::WakeupAndQuit() {
  INFO("Task:"<<thread_name()<<" id:"<<thread_id()<<", Wake up and quit");
  StopSoon();
  if (wakeup_pipe[PIPE_IN] != -1) {
    char c = 0;
    write(wakeup_pipe[PIPE_IN], &c, 1);
  }
  Stop();
}
bool SendWorker::IsAllDelivered() {

}
void SendWorker::AddConnection(int fd, int64 sid) {
  INFO("add new connection fd "<<fd<<" sid "<<sid);
  AutoLock guard(mapLock);
  set_noneblocking_fd(fd);
  eventMap::iterator iter = eMap.find(fd);
  if (iter == eMap.end()) {
    struct event* e = event_new(ebase, fd, EV_WRITE | EV_PERSIST, &SendWorker::OnLibEventNotifiction, this);
    if (e != NULL) {
      eMap.insert(iter, eventMapPair(fd, e));
      event_add(e, NULL);
      i_sock2SidMap[fd] = sid;
      i_lastMsgFlag.insert(sid);
    }
  }
}

void SendWorker::RemoveConnection(int fd) {
  AutoLock guard(mapLock);
  eventMap::iterator iter = eMap.find(fd);
  if (iter != eMap.end()) {
    struct event* e = iter->second;
    if (e != NULL) {
      event_del(e);
      event_free(e);
    }
    eMap.erase(iter);
    i_sock2SidMap.erase(fd);
  }
}

void SendWorker::Init() {
}

void SendWorker::Run() {
  //setup wakeup channel;
  if (pipe(wakeup_pipe)) {
    ERROR("Could not create wake up pipe, quit");
    return;
  }

  set_noneblocking_fd(wakeup_pipe[0]);
  set_noneblocking_fd(wakeup_pipe[1]);
  struct event* e = event_new(ebase, wakeup_pipe[PIPE_OUT], EV_READ | EV_PERSIST, &SendWorker::OnWakeup, ebase);
  if (e != NULL) {
    event_add(e, NULL);
  } else {
    ERROR("Could not add wakeup channel, quit");
    return;
  }
  while (!stopping_) {
    event_base_dispatch(ebase);
    INFO("event loop exit");
    if (stopping_)
      return;
  }
  event_del(e);
  event_free(e);
}

void SendWorker::Cleanup() {
  AutoLock guard(mapLock);
  for (eventMap::iterator iter = eMap.begin(); iter != eMap.end(); iter++) {
    struct event* e = iter->second;
    if (e != NULL) {
      event_del(e);
      event_free(e);
    }
    iter->second = NULL;
  }
  eMap.clear();

  if (wakeup_pipe[PIPE_OUT] != -1) {
    close(wakeup_pipe[PIPE_OUT]);
  }

  if (wakeup_pipe[PIPE_IN] != -1) {
    close(wakeup_pipe[PIPE_IN]);
  }
}

void SendWorker::OnCanSend(int fd) {
  int64 sid = getSid(fd);
  if (!sid) {
    ERROR("could not find server by sock "<<fd);
    //todo, should I remove this sock from map?
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

  CircleQueue* q = getQueue(sid);
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
  int32 packetLen = HostToNetwork32(msg->Length());
  int sendNum = send(sock, &packetLen, sizeof(packetLen), 0);
  if (sendNum != sizeof(packetLen)) {
    ERROR("try to send "<<sizeof(packetLen)<<" but actually sent "<<sendNum);
    return;
  }

  //send remain message
  sendNum = send(sock, msg->Data(), msg->Length(), 0);
  if (sendNum != (int) msg->Length()) {
    ERROR("try to send "<<msg->Length()<<" but actually sent "<<sendNum);
  }
}
//lock needed
CircleQueue* SendWorker::getQueue(int64 sid) {
  CircleQueue * q = NULL;
  AutoLock guard(sendQueueLock);
  sendQueueMap::iterator iterQ = i_sendQueueMap.find(sid);
  if (iterQ != i_sendQueueMap.end()) {
    q = iterQ->second;
  } else {
    INFO("no send queue for server "<<sid<<" create one");
    q = new CircleQueue(SEND_CAPACITY);
    i_sendQueueMap[sid] = q;
  }
  return q;
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

