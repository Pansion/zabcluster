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


#ifndef UTILS_H_
#define UTILS_H_
#include "base/basictypes.h"
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
using namespace std;

namespace ZABCPP {

  class ZxidUtils {
    public:
      static int64 GetEpochFromZxid(int64 zxid) {
        return zxid >> 32L;
      }
      ;

      static int64 GetCounterFromZxid(int64 zxid) {
        return zxid & 0xffffffffL;
      }
      ;

      static int64 MakeZxid(int64 epoch, int64 counter) {
        return (epoch << 32L) | (counter & 0xffffffffL);
      }
      ;
      static string HexStr(int64 v) {
        char buf[32];
        memset(buf, 0, 32);
        snprintf(buf, 32, "0x%llx", v);
        return string(buf);
      }

      static string HexStr(int32 v) {
        char buf[32];
        memset(buf, 0, 32);
        snprintf(buf, 32, "0x%x", v);
        return string(buf);
      }
      static string ZxidToString(int64 zxid) {
        return HexStr(zxid);
      }
      ;
  };

  class ZabUtil {
    public:
      static int SetNoneBlockFD(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags == -1)
          flags = 0;
        return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
      }
  };
}

#endif /* UTILS_H_ */
