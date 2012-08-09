#!/usr/bin/perl

use strict;
use Redis;
use threads;

sub worker($$)
{
    my $redis_addr = shift;
    my $counter = shift;
    my $tid = threads->tid();	
    my $id = $$.".".$tid;

    my $i = 0;
    my $r = Redis->new(server=>$redis_addr);
	while ($i < $counter)
    {
        $r->set($id.".".$i=>$i);   	
        $i++;
    }
    
    sleep(5);
    
    $i = 0;
    while($i < $counter)
    {
        my $v = $r->get($id.".".$i);
        if ($v != $i)
        {
            print "ERROR: data was not consistent for $id.$i, expected $i, but get $v from redis\n";
        }
        $i++;
    }
}

my $baseZabPort = shift;
my $serverCount = shift;
my $dataCounter = shift;

my $worker = 0;
my @workers;
while ($worker < $serverCount)
{
    my $zabPort = $baseZabPort + $worker;
    my $redis_addr = "127.0.0.1:".$zabPort;
    my $thr = threads->create(\&worker, $redis_addr, $dataCounter);
    if (! defined $thr)
    {
        print "Error: create worker thread for host $redis_addr failed\n";
        next;
    }
    my $tid = $thr->tid();
    print "Info:Worker thread $tid was created successfully\n";
    push(@workers, $thr);
    $worker++;
}

foreach (@workers)
{
    $_->join();
}

