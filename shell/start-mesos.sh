#!/usr/bin/env bash
../sbin/start-mesos-dispatcher.sh --master mesos://master:5050 -h 127.0.0.1 --port 7077 --webui-port 8081
