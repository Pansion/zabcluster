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

#ifndef CLIENT_HANDLER_H_
#define CLIENT_HANDLER_H_

#include "client_protocol/redis_protocol.h"
#include "base/basictypes.h"
#include <map>

using namespace std;

namespace ZABCPP{
  class ZabQuorumServer;

  class ClientHandlerInterface{
    public:
      //handle client request
      virtual void HandleIncomingRequest(int, const char *, int) = 0;

      //handle client shutdown
      virtual void HandleClientShutdown(int) = 0;

      //set active zabserver
      void SetZabServer(ZabQuorumServer* z){
        zabServer = z;
      }
    public:
      ClientHandlerInterface():zabServer(NULL){};
      virtual ~ClientHandlerInterface(){};
    protected:
      ZabQuorumServer*                    zabServer;

    private:
      DISALLOW_COPY_AND_ASSIGN(ClientHandlerInterface);
  };

  class ClientHandlerRedis: public ClientHandlerInterface{
    public:
      ClientHandlerRedis();
      virtual ~ClientHandlerRedis();

      virtual void HandleIncomingRequest(int , const char *, int);

      virtual void HandleClientShutdown(int);
    private:
      struct redisClient* getClient(int);
      void cleanupClients();
      int  getCmdType(sds cmd);
    private:
      typedef map<int, struct redisClient*>      ClientMap;
      ClientMap                           clientMap;
      DISALLOW_COPY_AND_ASSIGN(ClientHandlerRedis);
  };
}


#endif /* CLIENT_HANDLER_H_ */
