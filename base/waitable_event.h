/*
 * waitable_event.h
 *
 *  Created on: Jul 2, 2012
 *      Author: pchen
 */

#ifndef WAITABLE_EVENT_H_
#define WAITABLE_EVENT_H_

#include "lock.h"
#include <list>

//porting from goole chrome
//remove windows based code

class WaitableEvent {
  public:
    // If manual_reset is true, then to set the event state to non-signaled, a
    // consumer must call the Reset method.  If this parameter is false, then the
    // system automatically resets the event state to non-signaled after a single
    // waiting thread has been released.
    WaitableEvent(bool manual_reset, bool initially_signaled);

#if defined(OS_WIN)
    // Create a WaitableEvent from an Event HANDLE which has already been
    // created. This objects takes ownership of the HANDLE and will close it when
    // deleted.
    explicit WaitableEvent(HANDLE event_handle);

    // Releases ownership of the handle from this object.
    HANDLE Release();
#endif

    ~WaitableEvent();

    // Put the event in the un-signaled state.
    void Reset();

    // Put the event in the signaled state.  Causing any thread blocked on Wait
    // to be woken up.
    void Signal();

    // Returns true if the event is in the signaled state, else false.  If this
    // is not a manual reset event, then this test will cause a reset.
    bool IsSignaled();

    // Wait indefinitely for the event to be signaled.  Returns true if the event
    // was signaled, else false is returned to indicate that waiting failed.
    bool Wait();

    // Wait up until max_time has passed for the event to be signaled.  Returns
    // true if the event was signaled.  If this method returns false, then it
    // does not necessarily mean that max_time was exceeded.
    bool TimedWait(const int64 max_time_in_micros);
    // For asynchronous waiting, see WaitableEventWatcher

    // This is a private helper class. It's here because it's used by friends of
    // this class (such as WaitableEventWatcher) to be able to enqueue elements
    // of the wait-list
    class Waiter {
      public:
        // Signal the waiter to wake up.
        //
        // Consider the case of a Waiter which is in multiple WaitableEvent's
        // wait-lists. Each WaitableEvent is automatic-reset and two of them are
        // signaled at the same time. Now, each will wake only the first waiter in
        // the wake-list before resetting. However, if those two waiters happen to
        // be the same object (as can happen if another thread didn't have a chance
        // to dequeue the waiter from the other wait-list in time), two auto-resets
        // will have happened, but only one waiter has been signaled!
        //
        // Because of this, a Waiter may "reject" a wake by returning false. In
        // this case, the auto-reset WaitableEvent shouldn't act as if anything has
        // been notified.
        virtual bool Fire(WaitableEvent* signaling_event) = 0;

        // Waiters may implement this in order to provide an extra condition for
        // two Waiters to be considered equal. In WaitableEvent::Dequeue, if the
        // pointers match then this function is called as a final check. See the
        // comments in ~Handle for why.
        virtual bool Compare(void* tag) = 0;

      protected:
        virtual ~Waiter() {
        }
    };

  private:
    class WaitableEventKernel {
      public:
        WaitableEventKernel(bool manual_reset, bool initially_signaled)
            : manual_reset_(manual_reset), signaled_(initially_signaled) {
        }

        bool Dequeue(Waiter* waiter, void* tag);

        Lock lock_;
        const bool manual_reset_;
        bool signaled_;

        std::list<Waiter*> waiters_;
    };

    bool SignalAll();
    bool SignalOne();
    void Enqueue(Waiter* waiter);
    WaitableEventKernel* kernel_;
};

#endif /* WAITABLE_EVENT_H_ */
