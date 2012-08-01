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

#ifndef QUORUM_PACKET_H_
#define QUORUM_PACKET_H_

#include "base/basictypes.h"
#include "base/byte_buffer.h"

namespace ZABCPP {
  static const int32 DIFF = 13;

  /**
   * This is for follower to truncate its logs
   */
  static const int32 TRUNC = 14;

  /**
   * This is for follower to download the snapshots
   */
  static const int32 SNAP = 15;

  /**
   * This tells the leader that the connecting peer is actually an observer
   */
  static const int32 OBSERVERINFO = 16;

  /**
   * This message type is sent by the leader to indicate it's zxid and if
   * needed, its database.
   */
  static const int32 NEWLEADER = 10;

  /**
   * This message type is sent by a follower to pass the last zxid. This is here
   * for backward compatibility purposes.
   */
  static const int32 FOLLOWERINFO = 11;

  /**
   * This message type is sent by the leader to indicate that the follower is
   * now uptodate andt can start responding to clients.
   */
  static const int32 UPTODATE = 12;

  /**
   * This message is the first that a follower receives from the leader.
   * It has the protocol version and the epoch of the leader.
   */
  static const int32 LEADERINFO = 17;

  /**
   * This message is used by the follow to ack a proposed epoch.
   */
  static const int32 ACKEPOCH = 18;

  /**
   * This message type is sent to a leader to request and mutation operation.
   * The payload will consist of a request header followed by a request.
   */
  static const int32 REQUEST = 1;

  /**
   * This message type is sent by a leader to propose a mutation.
   */
  static const int32 PROPOSAL = 2;

  /**
   * This message type is sent by a follower after it has synced a proposal.
   */
  static const int32 ACK = 3;

  /**
   * This message type is sent by a leader to commit a proposal and cause
   * followers to start serving the corresponding data.
   */
  static const int32 COMMIT = 4;

  /**
   * This message type is enchanged between follower and leader (initiated by
   * follower) to determine liveliness.
   */
  static const int32 PING = 5;

  /**
   * This message type is to validate a session that should be active.
   */
  static const int32 REVALIDATE = 6;

  /**
   * This message is a reply to a synchronize command flushing the pipe
   * between the leader and the follower.
   */
  static const int32 SYNC = 7;

  /**
   * This message type informs observers of a committed proposal.
   */
  static const int32 INFORM = 8;

  class QuorumPacket {
    public:
      QuorumPacket()
          : zxid(0) {

      }
      ;

      ~QuorumPacket() {

      }
      ;
      int32 type;
      int64 zxid;
      ByteBuffer payload;
    private:
      DISALLOW_COPY_AND_ASSIGN(QuorumPacket);
  };
}

#endif /* QUORUM_PACKET_H_ */
