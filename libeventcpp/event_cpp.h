/*
 * event_cpp.h
 *
 *  Created on: Jun 28, 2012
 *      Author: pchen
 */

#ifndef EVENT_CPP_H_
#define EVENT_CPP_H_

#include "event2/event.h"
#include "base/thread.h"
#include "base/lock.h"
#include "base/byte_buffer.h"
#include "base/logging.h"
#include <map>
#include <set>

using namespace std;
static const int SEND_CAPACITY = 1;
class RecvWorker: public Thread {
  public:
    RecvWorker();
    ~RecvWorker();

    static void OnLibEventNotifiction(evutil_socket_t fd, short what, void *arg);
    static void OnWakeup(evutil_socket_t fd, short what, void *arg);

    void AddPeer(int sock);
    void RemovePeer(int sock);
    void WakeupAndQuit();
  protected:
    virtual void Init();
    virtual void Run();
    virtual void Cleanup();
  private:
    void OnRecvMsg(int fd);
  private:
    enum {
      PIPE_OUT = 0,
      PIPE_IN
    };
    struct event_base* ebase;
    int wakeup_pipe[2];

    typedef map<int, struct event*> eventMap;
    typedef pair<int, struct event*> eventMapPair;
    eventMap eMap;
    Lock mapLock;
};

class CircleQueue {
  public:
    CircleQueue(int capacity)
        : i_Queue(NULL), start(0), len(0), capacity(capacity) {
      if (0 < capacity)
        i_Queue = new ByteBuffer*[capacity];
    }

    ~CircleQueue() {
      if (i_Queue != NULL) {
        ByteBuffer * temp = NULL;
        while ((temp = i_popFront()) != NULL) {
          delete temp;
        }
        delete[] i_Queue;
      }
    }

    void PushBack(ByteBuffer * node) {
      AutoLock guard(queueLock);
      i_pushBack(node);
    }
    ByteBuffer * PopFront() {
      AutoLock guard(queueLock);
      return i_popFront();
    }
    bool Empty() {
      return (len == 0);
    }
  private:
    void i_pushBack(ByteBuffer * node) {
      if (node == NULL) {
        ERROR("not a valid node");
        return;
      }

      if (len < capacity) {
        (i_Queue)[(start + len) % capacity] = node;
        len++;
      } else {
        INFO("reach capacity, remove old one then add new one");
        ByteBuffer *tmp = (i_Queue)[start];
        delete tmp;
        start = (start + 1) % capacity;
        (i_Queue)[start] = node;
      }
    }
    ByteBuffer * i_popFront() {
      ByteBuffer * ret = NULL;
      if (len != 0) {
        ret = (i_Queue)[start];
        start = (start + 1) % capacity;
        len--;
      }
      return ret;
    }
  private:
    ByteBuffer** i_Queue;
    int start;
    int len;
    int capacity;
    Lock queueLock;
};

class CircleQueue;
class SendWorker: public Thread {
  public:
    SendWorker();
    ~SendWorker();

    static void OnLibEventNotifiction(evutil_socket_t fd, short what, void *arg);
    static void OnWakeup(evutil_socket_t fd, short what, void *arg);

    void WakeupAndQuit();
    bool IsAllDelivered();
    void AddConnection(int sock, int64 sid);
    void RemoveConnection(int sock);

    void SendMsg(int64 sid, ByteBuffer * m) {
      getQueue(sid)->PushBack(m);
    }
  protected:
    virtual void Init();
    virtual void Run();
    virtual void Cleanup();
  private:
    void OnCanSend(int fd);

    //lock free;
    void setLastMsg(int64 sid, ByteBuffer* m);
    ByteBuffer* getLastMsg(int64 sid);
    void i_sendMsg(int sock, int64 sid, ByteBuffer* msg);
    //lock needed
    int64 getSid(int sock);
    CircleQueue* getQueue(int64 sid);
  private:
    enum {
      PIPE_OUT = 0,
      PIPE_IN
    };
    struct event_base* ebase;
    int wakeup_pipe[2];

    typedef map<int, struct event*> eventMap;
    typedef pair<int, struct event*> eventMapPair;
    typedef map<int64, CircleQueue*> sendQueueMap;
    typedef map<int, int64> sock2SidMap;
    typedef map<int64, ByteBuffer*> lastMessageMap;
    typedef pair<int64, ByteBuffer*> lastMsgPair;
    typedef set<int64> lastMsgFlag;
    eventMap eMap;
    sock2SidMap i_sock2SidMap;
    sendQueueMap i_sendQueueMap;
    lastMessageMap i_lastMessageSent;
    lastMsgFlag i_lastMsgFlag;

    Lock mapLock;
    Lock sendQueueLock;
};
#endif /* EVENT_CPP_H_ */
