#include "election_cnxmgr.h"
#include "election_protocol.h"
#include "base/logging.h"
#include "stdio.h"
using namespace ZABCPP;
int main(int argc, char **argv) {

  log_instance->setLogLevel(20000);
  INFO("begin test leader election listener");
  QuorumPeerConfig peerConfig;
  peerConfig.parseCfg("zoo.cfg");
  peerConfig.serverId = 1;
  QuorumCnxMgr cnxMgr(&peerConfig);
  Listener testListener(&cnxMgr);
  testListener.Start();
  Message * msg = NULL;
  Notification n;
  while ((msg = cnxMgr.pollRecvQueue(-1)) != NULL) {
    INFO("get new message");
    ByteBuffer * sendBuf = new ByteBuffer(msg->buffer.Data(), msg->buffer.Length());
    INFO("echo back message");
    cnxMgr.toSend(msg->sid, sendBuf);
    n.sid = msg->sid;
    msg->buffer.ReadInt32(n.state);
    msg->buffer.ReadInt64(n.leader);
    msg->buffer.ReadInt64(n.zxid);
    msg->buffer.ReadInt64(n.electionEpoch);
    msg->buffer.ReadInt64(n.peerEpoch);

    char dumping[1024];
    memset(dumping, 0, 1024);
    sprintf(dumping, "sid:0x%llx,state:%d,leader:0x%llx, zxid:0x%llx, electionEpoch:0x%llx, peerEpoch:0x%llx", n.sid, n.state,
        n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
    INFO("Messge details:"<<dumping);
    delete msg;
  }
  INFO("end testing");
  return 0;
}
