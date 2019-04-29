### mac 安装mesos

```bash
brew install mesos 
```  
启动master
```bash
start master
nohup /usr/local/sbin/mesos-master --registry=in_memory --ip=127.0.0.1 > /dev/null 2>&1 &

start agent
nohup  /usr/local/sbin/mesos-slave --master=127.0.0.1:5050  --work_dir=/tmp/mesos/agent > /dev/null 2>&1 &

```

设置mesos 环境变量
```bash
echo  'export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.dylib' > conf/spark-env.sh
echo ' export SPARK_EXECUTOR_URI=/Users/spafka/Desktop/flink/spark/spark-2.4.2-bin-2.6.5.tgz' > conf/spark-env.sh
echo 'export SPARK_MESOS_DISPATCHER_HOST=localhost' > conf/spark-env.sh

```
##spark on mesos 

[client mode]  driver在客户机
```bash

spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master mesos://localhost:5050 \
  --executor-memory 8G \
  --total-executor-cores 8\
  $pwd/../examples/target/original-spark-examples_$SPARK_SCALA_VERSION-$SPARK_VERSION.jar

```
[cluster mode ] driver在cluster
```bash
export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.dylib
export SPARK_EXECUTOR_URI=/Users/spafka/Desktop/flink/spark/spark-2.4.2-bin-2.6.5.tgz
export SPARK_MESOS_DISPATCHER_HOST=localhost
sbin/start-mesos-dispatcher.sh -m mesos://127.0.0.1:5050

source $pwd/base.sh
# Run on a Spark standalone cluster
spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master mesos://localhost:7077 \
  --executor-memory 1G \
  --total-executor-cores 2\
  --deploy-mode  cluster \
  $pwd/../examples/target/original-spark-examples_$SPARK_SCALA_VERSION-$SPARK_VERSION.jar
```

###
(spark on mesos guide)[http://spark.apache.org/docs/latest/running-on-mesos.html]
###
(Spark on Mesos cluster mode)[https://www.jianshu.com/p/5d8c090a3898]
###
(mesos marathon )[http://geek.csdn.net/news/detail/191491]


Property Name	Default	Meaning
spark.deploy.recoveryMode	NONE	The recovery mode setting to recover submitted Spark jobs with cluster mode when it failed and relaunches. This is only applicable for cluster mode when running with Standalone or Mesos.
spark.deploy.zookeeper.url	None	When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper URL to connect to.
spark.deploy.zookeeper.dir	None	When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper directory to store recovery state.
