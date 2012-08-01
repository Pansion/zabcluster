// Copyright (c) 2006-2008 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "condition_variable.h"

#include <errno.h>
#include <sys/time.h>

#include "lock.h"
#include "lock_impl.h"

class Time {
  public:
    static const int64 kMillisecondsPerSecond = 1000;
    static const int64 kMicrosecondsPerMillisecond = 1000;
    static const int64 kMicrosecondsPerSecond = kMicrosecondsPerMillisecond * kMillisecondsPerSecond;
    static const int64 kMicrosecondsPerMinute = kMicrosecondsPerSecond * 60;
    static const int64 kMicrosecondsPerHour = kMicrosecondsPerMinute * 60;
    static const int64 kMicrosecondsPerDay = kMicrosecondsPerHour * 24;
    static const int64 kMicrosecondsPerWeek = kMicrosecondsPerDay * 7;
    static const int64 kNanosecondsPerMicrosecond = 1000;
    static const int64 kNanosecondsPerSecond = kNanosecondsPerMicrosecond * kMicrosecondsPerSecond;
};

ConditionVariable::ConditionVariable(Lock* user_lock)
    : user_mutex_(user_lock->lock_.os_lock())
#if !defined(NDEBUG)
, user_lock_(user_lock)
#endif
{
  pthread_cond_init(&condition_, NULL);
}

ConditionVariable::~ConditionVariable() {
  pthread_cond_destroy(&condition_);
}

void ConditionVariable::Wait() {
#if !defined(NDEBUG)
  user_lock_->CheckHeldAndUnmark();
#endif
  pthread_cond_wait(&condition_, user_mutex_);
#if !defined(NDEBUG)
  user_lock_->CheckUnheldAndMark();
#endif
}

void ConditionVariable::TimedWait(const int64 max_time_in_micros) {
  int64 usecs = max_time_in_micros;

  // The timeout argument to pthread_cond_timedwait is in absolute time.
  struct timeval now;
  gettimeofday(&now, NULL);

  struct timespec abstime;
  abstime.tv_sec = now.tv_sec + (usecs / Time::kMicrosecondsPerSecond);
  abstime.tv_nsec = (now.tv_usec + (usecs % Time::kMicrosecondsPerSecond)) * Time::kNanosecondsPerMicrosecond;
  abstime.tv_sec += abstime.tv_nsec / Time::kNanosecondsPerSecond;
  abstime.tv_nsec %= Time::kNanosecondsPerSecond;

#if !defined(NDEBUG)
  user_lock_->CheckHeldAndUnmark();
#endif
  pthread_cond_timedwait(&condition_, user_mutex_, &abstime);
#if !defined(NDEBUG)
  user_lock_->CheckUnheldAndMark();
#endif
}

void ConditionVariable::Broadcast() {
  pthread_cond_broadcast(&condition_);
}

void ConditionVariable::Signal() {
  pthread_cond_signal(&condition_);
}
