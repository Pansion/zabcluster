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

#ifndef ZAB_CONSTANT_H_
#define ZAB_CONSTANT_H_

#include "base/basictypes.h"

namespace ZABCPP {
  static const int RECV_CAPACITY = 100;
  // Initialized to 1 to prevent sending
  // stale notifications to peers
  static const int SEND_CAPACITY = 1;

  static const int PACKETMAXSIZE = 1024 * 1024;

  static const int READ_BUF_LEN = 1024 * 16;

  static const int MAX_CONNECT_LEADER_RETRY = 5;

  /*
   * Maximum number of attempts to connect to a peer
   */
  static const int MAX_CONNECTION_ATTEMPTS = 2;

  static const int cnxTO = 5000;

  static const int MAX_LISTENER_RETIES = 3;
}


#endif /* ZAB_CONSTANT_H_ */
