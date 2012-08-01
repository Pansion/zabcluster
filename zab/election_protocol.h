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

#ifndef ELECTION_PROTOCOL_HXX_
#define ELECTION_PROTOCOL_HXX_

#include "base/basictypes.h"

namespace ZABCPP {

  typedef enum {
    LOOKING = 0,
    FOLLOWING,
    LEADING,
    OBSERVING
  } PeerState;

  typedef enum {
    PARTICIPANT = 0,
    OBSERVER
  } LearnerType;

  class Vote {
    public:
      Vote() {

      }
      Vote(int64 id, int64 zxid, int64 electionEpoch, int64 peerEpoch)
          : id(id), zxid(zxid), electionEpoch(electionEpoch), peerEpoch(peerEpoch), state(LOOKING) {

      }

      bool operator==(const Vote& v) const {
        return ((id == v.id) && (zxid == v.zxid) && (electionEpoch == v.electionEpoch) && (peerEpoch == v.peerEpoch));
      }

      int64 id;
      int64 zxid;
      int64 electionEpoch;
      int64 peerEpoch;
      int32 state;
  };

  class Notification {
    public:
      Notification()
          : leader(-1), zxid(-1), electionEpoch(-1), state(-1), sid(-1), peerEpoch(-1) {

      }
      /*
       * Proposed leader
       */
      int64 leader;

      /*
       * zxid of the proposed leader
       */
      int64 zxid;

      /*
       * Epoch
       */
      int64 electionEpoch;

      /*
       * current state of sender
       */
      int32 state;

      /*
       * Address of sender
       */
      int64 sid;

      /*
       * epoch of the proposed leader
       */
      int64 peerEpoch;
  };
}
;
#endif /* ELECTION_PROTOCOL_HXX_ */
