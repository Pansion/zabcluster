zabcluster
==========

A c++ implementation of multi-master cluster based on ZAB(zookeeper) and Redis.

Introduction
  I have inerests in Paxos/Zab which were used to keep data consistent. 
  Then I found Apache Zookeeper which was successul and popular in the area. 
  Since I am not a expert on Java, I searched for a similar implmentation in C or C++ but failed. 
  Actually zabcluster was a kind of C++ version of Apache Zookeeper.
  It provide same feature as leader election/zab/HA of zookeeper. 
  
  And with difference:
  1.much less threads. 
    Zookeeper have bunch of threads for election/request handling. 
    e.g. If you have N nodes, there will be 2*(N-1) threads in each node which were only used for election. 
    During my tests, I can only run ~17 instance of zookeeper in a single host(alright, my desktop was really old and slow). 
    But for zabcluster, I can run 55 instances in same machine.
  2.support redis client.
  3.support redis server as storage.
  4.support much more clients.
  
Dependency
  Of course, redis-server should placed in your host.
  redis
    https://github.com/antirez/redis
  
  Thanks for below great open-source projects which make me a lot easier to finish all the work in a month.
  To build zabcluster, below libraries will be needed.
  
  libevent 2.0 
    http://libevent.org/ 
   
  log4cplus (1.0.4.1)
    http://log4cplus.sourceforge.net/
    
  

Build
  1. update Makefile.inc with your own libevent/log4cplus path
    LEV_DIR=/opt/libevent-2.0.19-stable
    LOGCPP_DIR=/opt/log4cplus-1.0.4.1
  2. make all
  
  NOTE:
  1.I am really not good at creating autoconf stuffs. In current version, you could only do "make all;make clean"
  2.I do not have a lot of machines to test different OS. The code was tested only in CentOS 5(32bit) and CentOS 6(64bit).
  
How to use
  I have provided two scripts gen_con.pl and multi_nodes.sh which could make you easier to run zabcluster in a single host
  
  gen_conf.pl 3  //it will generate configuration of 3 nodes cluster(include redis configuration)
  multi_nodes.sh 3 //it will launch 3 instances using configuration generated above
  
  After all intances were started up, you can play with them by using redis-benchmark or any other redis client.
  
  Sample output by using redis-benchmark:

[pansion@pansion src]$ ./redis-benchmark -p 2182 -n 100 -t set 
====== SET ======
  100 requests completed in 0.09 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

1.00% <= 31 milliseconds
4.00% <= 32 milliseconds
7.00% <= 33 milliseconds
12.00% <= 34 milliseconds
15.00% <= 35 milliseconds
19.00% <= 36 milliseconds
22.00% <= 37 milliseconds
26.00% <= 38 milliseconds
29.00% <= 39 milliseconds
33.00% <= 40 milliseconds
38.00% <= 41 milliseconds
62.00% <= 42 milliseconds
84.00% <= 43 milliseconds
89.00% <= 44 milliseconds
94.00% <= 45 milliseconds
97.00% <= 46 milliseconds
100.00% <= 47 milliseconds
1063.83 requests per second
    

Performance
  In my desktop(yes, old and slow), running 3 instances(zabcluster and redis-server) at same time, 
  "set" could reach 6~8k with latency 20~30ms, "get" could reach 30k with latency ~5ms.
  
  If you have better environment for performance testing, please share results with me.
  
Known issues
  1.Do not support PING_INLINE of redis-benchmark
  2.If redis-server was shutdown, zabcluster will not quit while it will also not process any new request
    I am still investgating solution to handle this case.
  3.No automatic sync between leader/follower. 
    If a node failed and want to re-join cluster, you should first use redis "sync" command to get latest data from leader redis-server.

Todo
  1.add automatic sync between leader/follower
  2.redis was a server. make redis a storage of zabcluster required additonal efforts to handle network connection.
  A embedded database like leveldb or bdb may be a better choice for the storage of zabcluster. 
  I am considering add leveldb support in later time.
  
  Any other request are welcome.
