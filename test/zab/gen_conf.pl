#!/usr/bin/perl
#Copyright (c) 2012, Pansion Chen <pansion dot zabcpp at gmail dot com>
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of the zabcpp nor the
#      names of its contributors may be used to endorse or promote products
#      derived from this software without specific prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY
#DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
use strict;


my $cfgLoc = "./batch_nodes";
my $serverCount = $ARGV[0];

my $baseElport = 4181;
my $baseZabport = 3181;
my $baseDataDir = $cfgLoc."/dt";
my %baseCfg = 
(
"tickTime"=>2000,
"initLimit"=>10,
"syncLimit"=>5
);

my %portCfg = 
(
"clientPort"=>2181,
"zabDBPort" =>6378,
);
#tickTime=2000
#initLimit=10
#syncLimit=5
#dataDir=./data
#clientPort=2182
#
#
#
#server.1=127.0.0.1:3181:4181
#server.2=127.0.0.1:3182:4182
#server.3=127.0.0.1:3183:4183

my %redisCfg = 
(
"daemonize"=>"no",
"pidfile"=>"/var/run/redis.pid",
"timeout"=>"0",
"loglevel"=>"debug",
"logfile"=>"stdout",
"databases"=>"16",
#"save"=>"900 1",
#"save"=>"300 10",
#"save"=>"60 10000",
"rdbcompression"=>"yes",
"dbfilename"=>"dump.rdb",
"slave-serve-stale-data"=>"yes",
"appendonly"=>"yes",
"appendfsync"=>"everysec",
"no-appendfsync-on-rewrite"=>"no",
"auto-aof-rewrite-percentage"=>"100",
"auto-aof-rewrite-min-size"=>"64mb",
"slowlog-log-slower-than"=>"10000",
"slowlog-max-len"=>"128",
"vm-enabled"=>"no",
"vm-swap-file"=>"/tmp/redis.swap",
"vm-max-memory"=>"0",
"vm-page-size"=>"32",
"vm-pages"=>"134217728",
"vm-max-threads"=>"4",
"hash-max-zipmap-entries"=>"512",
"hash-max-zipmap-value"=>"64",
"list-max-ziplist-entries"=>"512",
"list-max-ziplist-value"=>"64",
"set-max-intset-entries"=>"512",
"zset-max-ziplist-entries"=>"128",
"zset-max-ziplist-value"=>"64",
"activerehashing"=>"yes"
);

`rm -rf $cfgLoc/*`;

my $servId = 1;
my %nodes;
my $elPort = $baseElport;
my $zabPort = $baseZabport;
while($servId <= $serverCount)
{
	$nodes{"server.".$servId} = "127.0.0.1:".$zabPort.":".$elPort;
    $zabPort ++;
    $elPort ++;
    $servId ++;	
}

$servId = 1;
while($servId <= $serverCount)
{
	#create my id file
	my $servDataDir = $baseDataDir.$servId;
	`mkdir -p $servDataDir`;
	`echo $servId > $servDataDir"/myid"`;
	$nodes{"dataDir"} = $servDataDir;
	foreach my $k (sort keys %baseCfg)
	{
		`echo $k=$baseCfg{$k}>> $servDataDir"/zoo.cfg"`;
	}
	
	foreach my $kp (sort keys %portCfg)
	{
	   my $p = $portCfg{$kp} + $servId;
	   `echo $kp=$p>> $servDataDir"/zoo.cfg"`;	
	}
	
	foreach my $k1(sort keys %nodes)
	{
        `echo $k1=$nodes{$k1} >> $servDataDir"/zoo.cfg"`;
	}


    #redis confi generate
    my $rport = $portCfg{"zabDBPort"} + $servId;
    `echo port $rport >> $servDataDir"/redis.conf"`; 
    `echo dir $servDataDir >> $servDataDir"/redis.conf"`;
	foreach my $kr (keys %redisCfg)
	{
		`echo $kr $redisCfg{$kr} >> $servDataDir"/redis.conf"`;
	}
	
	$servId++;	
}
