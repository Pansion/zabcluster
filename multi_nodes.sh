
rm -f zabcluster_*.log*
for(( i = 1; i <= $1; i++))
do
#  rm -f node$i.log
  #  echo "testing $i"
  # NOTE: please update location of redis-server
  xterm -T "redis $i" -geometry 80x24+10+$i*20+250+$i*10 -e "/opt/open-source/redis-2.4.14/src/redis-server ./batch_nodes/dt$i/redis.conf" &
  sleep 1
  xterm -T "node $i" -geometry 80x24+10+$i*20+250+$i*10 -e "./zab_cluster ./batch_nodes/dt$i/zoo.cfg" &
#  xterm -T "node $i" -geometry 80x24+10+$i*20+250+$i*10 -e "./test_client_db ./batch_nodes/dt$i/zoo.cfg" &
  #sleep 1
done
