#!/usr/bin/env bash

echo "start master"
nohup /usr/local/sbin/mesos-master --registry=in_memory --ip=127.0.0.1 > /dev/null 2>&1 &

echo "start agent"
nohup  /usr/local/sbin/mesos-slave --master=127.0.0.1:5050  --work_dir=/tmp/mesos/agent > /dev/null 2>&1 &
