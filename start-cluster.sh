#!/usr/bin/env bash


cd $ZK_HOME

echo "启动zk"

nohup ./bin/zkServer.sh restart &
sleep 5s
cd $HADOOP_HOME
echo 'start dfs'
#sbin/start-dfs.sh
echo 'start mesos master'
nohup /usr/local/Cellar/mesos/1.4.1/sbin/mesos-master --ip=127.0.0.1 --zk=zk://127.0.0.1:2181/mesos --quorum=1 --work_dir=/tmp/mesos/master > /dev/null &
sleep 5s
echo 'start mesos agent'
nohup /usr/local/Cellar/mesos/1.4.1/sbin/mesos-slave --master=zk://127.0.0.1:2181/mesos --work_dir=/tmp/mesos/slave --ip=127.0.0.1  > /dev/null &

sleep 5s

#bin/mesos-appmaster.sh \
#    -Dmesos.master=zk://127.0.0.1:2181/mesos \
#    -Djobmanager.heap.mb=1024 \
#    -Djobmanager.rpc.port=6123 \
#    -Drest.port=8081 \
#    -Dmesos.resourcemanager.tasks.mem=4096 \
#    -Dtaskmanager.heap.mb=3500 \
#    -Dtaskmanager.numberOfTaskSlots=2 \
#    -Dparallelism.default=10