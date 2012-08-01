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

#ifndef CIRCLE_QUEUE_H_
#define CIRCLE_QUEUE_H_

#include "base/lock.h"

namespace ZABCPP {
  template <class Type>
  class CircleQueue {
    public:
      CircleQueue(int capacity)
          : i_Queue(NULL), start(0), len(0), capacity(capacity) {
        if (0 < capacity)
          i_Queue = new Type*[capacity];
      }

      ~CircleQueue() {
        Clear();
        if (i_Queue != NULL){
          delete[] i_Queue;
          i_Queue = NULL;
        }
      }

      void PushBack(Type * node) {
        AutoLock guard(queueLock);
        i_pushBack(node);
      }

      Type * PopFront() {
        AutoLock guard(queueLock);
        return i_popFront();
      }

      bool Empty() {
        AutoLock guard(queueLock);
        return (len == 0);
      }

      void Clear() {
        if (i_Queue != NULL) {
          Type * temp = NULL;
          while ((temp = i_popFront()) != NULL) {
            delete temp;
          }
        }
      }
    private:
      void i_pushBack(Type * node) {
        if (node == NULL) {
          return;
        }

        if (len < capacity) {
          (i_Queue)[(start + len) % capacity] = node;
          len++;
        } else {
          Type *tmp = (i_Queue)[start];
          delete tmp;
          start = (start + 1) % capacity;
          (i_Queue)[start] = node;
        }
      }
      Type * i_popFront() {
        Type * ret = NULL;
        if (len != 0) {
          ret = (i_Queue)[start];
          (i_Queue)[start] = NULL;
          start = (start + 1) % capacity;
          len--;
        }
        return ret;
      }
    private:
      Type** i_Queue;
      int start;
      int len;
      int capacity;
      Lock queueLock;
  };

}

#endif /* CIRCLE_QUEUE_H_ */
