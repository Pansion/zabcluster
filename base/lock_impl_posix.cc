// Copyright (c) 2006-2008 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "lock_impl.h"

#include <errno.h>

#define NDEBUG

LockImpl::LockImpl() {
#ifndef NDEBUG
  // In debug, setup attributes for lock error checking.
  pthread_mutexattr_t mta;
  int rv = pthread_mutexattr_init(&mta);
  rv = pthread_mutexattr_settype(&mta, PTHREAD_MUTEX_ERRORCHECK);
  rv = pthread_mutex_init(&os_lock_, &mta);
  rv = pthread_mutexattr_destroy(&mta);
#else
  // In release, go with the default lock attributes.
  pthread_mutex_init(&os_lock_, NULL);
#endif
}

LockImpl::~LockImpl() {
  pthread_mutex_destroy(&os_lock_);
}

bool LockImpl::Try() {
  int rv = pthread_mutex_trylock(&os_lock_);
  return rv == 0;
}

void LockImpl::Lock() {
  pthread_mutex_lock(&os_lock_);
}

void LockImpl::Unlock() {
  pthread_mutex_unlock(&os_lock_);
}
