**[docker]**

```bash
sudo yum remove docker  docker-common docker-selinux docker-engine
sudo yum install -y yum-utils device-mapper-persistent-data lvm2
yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
systemctl start docker
systemctl enable docker

```

**[docker ALI]**
```bash
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json <<-'EOF'
{
  "registry-mirrors": ["https://gbpursha.mirror.aliyuncs.com"]
}
EOF

sudo systemctl daemon-reload
sudo systemctl restart docker

```

**[docker-compose]**

```bash
sudo curl -L https://github.com/docker/compose/releases/download/1.17.1/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```


**[zk]**    6.15
docker run  --net host --name zookeeper --restart always -d zookeeper:3.4.12

**[master]**
docker run -it -d --net=host \
  -e MESOS_PORT=5050 \
  -e MESOS_ZK=zk://(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1 -d '/'):2181/mesos \
  -e MESOS_QUORUM=1 \
  -e MESOS_REGISTRY=in_memory \
  -e MESOS_LOG_DIR=/var/log/mesos \
  -e MESOS_WORK_DIR=/var/tmp/mesos \
  -v "$(pwd)/log/mesos:/var/log/mesos" \
  -v "$(pwd)/tmp/mesos:/var/tmp/mesos" \
  $(cat /etc/hosts|awk -F ' ' '{if(NR>3){print "--add-host " $2 ":" $1}}') \
  mesosphere/mesos-master:1.7.1  --no-hostname_lookup --ip=$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1 -d '/')
  
**[slave]**
docker run -it -d --net=host --privileged \
    -e MESOS_PORT=5051 \
    -e MESOS_MASTER=zk://master/mesos \
    -e MESOS_SWITCH_USER=0 \
    -e MESOS_CONTAINERIZERS=mesos \
    -e MESOS_LOG_DIR=/var/log/mesos \
    -e MESOS_WORK_DIR=/var/tmp/mesos \
    -v "$(pwd)/log/mesos:/var/log/mesos" \
    -v "$(pwd)/tmp/mesos:/var/tmp/mesos" \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /cgroup:/cgroup \
    -v /run/systemd/system:/run/systemd/system \
    -v /sys:/sys \
    -v /usr/bin/docker:/usr/bin/docker \
    $(cat /etc/hosts|awk -F ' ' '{if(NR>3){print "--add-host "$2":"$1}}') \
    mesosphere/mesos-slave:1.7.1 --no-systemd_enable_support --no-hostname_lookup --ip=$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | cut -f1 -d '/')
    
    
  
**[install yum  mesos]**
![mesos]https://www.jianshu.com/p/22a5ed4db6a5
