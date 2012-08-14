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




#include "zab/peer_config.h"
#include "zab/quorum_peer.h"
#include "base/logging.h"
#include "event2/event.h"
#include <vector>
#include <signal.h>

using namespace ZABCPP;
using namespace std;
static void  handle_signal(evutil_socket_t fd, short event, void *arg) {
  struct event_base * b = static_cast<struct event_base*>(arg);
  event_base_loopbreak(b);
  ERROR("we got signal "<<fd);
}

int main(int argc, char *argv[]) {
  log_instance->setLogLevel(WARN_LOG_LEVEL);
  string cfg = "zoo.cfg";
  if (1 < argc) {
    cfg = argv[1];
  }
  INFO("zab cluster started, using configuration file: "<<cfg);
  QuorumPeerConfig peerConfig;
  peerConfig.parseCfg(cfg);

  QuorumPeer peer(&peerConfig);
  peer.Start();

  vector<struct event *>  eventList;
  struct event_base * base = event_base_new();
  struct event * es = evsignal_new(base, SIGHUP, &handle_signal, base);
  evsignal_add(es, NULL);
  eventList.push_back(es);

  es = evsignal_new(base, SIGINT, &handle_signal, base);
  evsignal_add(es, NULL);
  eventList.push_back(es);

  es = evsignal_new(base, SIGQUIT, &handle_signal, base);
  evsignal_add(es, NULL);
  eventList.push_back(es);

  es = evsignal_new(base, SIGABRT, &handle_signal, base);
  evsignal_add(es, NULL);
  eventList.push_back(es);

  es = evsignal_new(base, SIGSEGV, &handle_signal, base);
  evsignal_add(es, NULL);
  eventList.push_back(es);

  es = evsignal_new(base, SIGTERM, &handle_signal, base);
  evsignal_add(es, NULL);
  eventList.push_back(es);

  event_base_dispatch(base);
  //cleanup events;
  for(vector<struct event*>::iterator iter = eventList.begin();
      iter != eventList.end();
      iter ++) {
    struct event * e = *iter;
    if (e != NULL) {
      event_del(e);
      event_free(e);
    }
  }
  event_base_free(base);

  eventList.clear();

  INFO("zab_cluster will stop soon");
  peer.Stop();
  return 0;
}
