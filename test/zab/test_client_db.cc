/*
 * test_client_db.cc
 *
 *  Created on: Jul 19, 2012
 *      Author: pchen
 */

#include "base/logging.h"
#include "client_cnx_mgr.h"
#include "client_handler.h"
#include "quorum_server.h"
#include "peer_config.h"
#include "zab_constant.h"
#include "zabdb.h"
#include "zabdb_redis.h"
#include "event2/event.h"
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>

using namespace ZABCPP;
using namespace std;

class TestProcessor: public RequestProcessor{
  public:
    TestProcessor(){};
    virtual ~TestProcessor(){};

    static void OnLibEventNotification(evutil_socket_t fd, short what, void *arg){
      TestProcessor* p = static_cast<TestProcessor*>(arg);
      if (what & EV_READ){
        p->onRecvMsg(fd);
      }
    }
    virtual void ProcessRequest(const Request & req){
      ChannelMap::iterator iter = client2Server.find(req.clientfd);
      if (iter != client2Server.end()) {
        int sentnum = send(iter->second, req.request.Data(), req.request.Length(), 0);
      } else {
        int sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (sock < 0) {
          ERROR("Could not create socket");
          return;
        }
        int flag = 1;
        setsockopt(sock, /* socket affected */
        IPPROTO_TCP, /* set option at TCP level */
        TCP_NODELAY, /* name of option */
        (char *) &flag, /* the cast is historical cruft */
        sizeof(int));

        string addr = "127.0.0.1";
        int port = 6379;
        struct sockaddr_in peerAddr;
        memset(&peerAddr, 0, sizeof(peerAddr));
        peerAddr.sin_family = AF_INET; /* Internet address family */
        peerAddr.sin_addr.s_addr = inet_addr(addr.data()); /* Server IP address */
        peerAddr.sin_port = htons(port); /* Server port */

        if (connect(sock, (struct sockaddr *) &peerAddr, sizeof(peerAddr)) < 0) {
          ERROR("could not connect to server addr "<<addr<<" port "<<port<<" errno "<<errno);
          return;
        } else {
          INFO("create new channel to redis on "<<sock<<" for client on "<<req.clientfd);
          client2Server[req.clientfd] = sock;
          server2Client[sock] = req.clientfd;
          struct event* e = event_new(ebase, sock, EV_READ | EV_PERSIST, &TestProcessor::OnLibEventNotification, this);
          event_add(e, NULL);
          int sentnum = send(sock, req.request.Data(), req.request.Length(), 0);
          string redis(req.request.Data(), req.request.Length());
          //INFO("write redis request "<<redis<<" to server");
        }
      }
    }

    void RunEventLoop(){
      ebase = event_base_new();
      while(1) {
        event_base_dispatch(ebase);
        sleep(1);
      }
      event_base_free(ebase);
    }

  protected:
    void onRecvMsg(int fd){
      char buf[READ_BUF_LEN];
      memset(buf, 0, READ_BUF_LEN);
      int readnum = recv(fd, buf, READ_BUF_LEN, 0);
      if (readnum < 0) {
        ERROR("failed to read response from redis");
        return;
      }
      //INFO("send response to client "<<buf<<" len"<<readnum);
      ChannelMap::iterator iter = server2Client.find(fd);
      if (iter != server2Client.end()) {
        int sentnum = send(iter->second, buf, readnum, 0);
      }
    }
  private:
    struct event_base * ebase;
    typedef map<int, int> ChannelMap;
    ChannelMap            client2Server;
    ChannelMap            server2Client;
};

class DummyProcessor: public RequestProcessor{
  public:
    void  SetZabServer(ZabQuorumServer * z){
      zServer = z;
    }
    void SetSid(int64 sid) {
      myid = sid;
    }
    virtual void ProcessRequest(const Request & req){
      Request * r = new Request();
      r->zxid = zServer->GetNextZxid();
      r->clientfd = req.clientfd;
      r->sid = myid;
      r->client_type = Request::CT_REDIS;
      r->request.WriteBytes(req.request.Data(), req.request.Length());
      zServer->SubmitProposal(r);
      zServer->CommitProposal(r->zxid);
    }
  private:
    ZabQuorumServer* zServer;
    int64 myid;
};
int main(int argc, char *argv[]) {
  log_instance->setLogLevel(ERROR_LOG_LEVEL);
  string cfg = "zoo.cfg";
  if (1 < argc) {
    cfg = argv[1];
  }
  INFO("begin test leader election, using configuration file: "<<cfg);
  QuorumPeerConfig peerConfig;
  peerConfig.parseCfg(cfg);
//  TestProcessor tp;
  ZabDBRedis  zDb(peerConfig.serverId);
  zDb.SetServerAddr("127.0.0.1", peerConfig.zabDBPort);

  DummyProcessor tp;
  ZabQuorumServer zkServer(peerConfig.serverId, &tp, &zDb);
  tp.SetZabServer(&zkServer);
  tp.SetSid(peerConfig.serverId);

  zDb.Start();
  zkServer.Start();

  ClientCnxMgr cmgr(peerConfig.clientPort);
  ClientHandlerRedis  rhandler;
  rhandler.SetZabServer(&zkServer);
  cmgr.RegisterHanlder(&rhandler);
  cmgr.Start();
  while(1){
    sleep(10);
  }
  return 0;
}

