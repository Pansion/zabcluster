// Copyright (c) 2010 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// This file is used for debugging assertion support.  The Lock class
// is functionally a wrapper around the LockImpl class, so the only
// real intelligence in the class is in the debugging logic.

#define NDEBUG
#if !defined(NDEBUG)

#include "lock.h"

Lock::Lock() : lock_() {
  owned_by_thread_ = false;
  owning_thread_id_ = static_cast<PlatformThreadId>(0);
}

void Lock::AssertAcquired() const {
}

void Lock::CheckHeldAndUnmark() {
  owned_by_thread_ = false;
  owning_thread_id_ = static_cast<PlatformThreadId>(0);
}

void Lock::CheckUnheldAndMark() {
  owned_by_thread_ = true;
  owning_thread_id_ = PlatformThread::CurrentId();
}

#endif  // NDEBUG
