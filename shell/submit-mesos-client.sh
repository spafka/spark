#!/usr/bin/env bash


export pwd=$(dirname "$0")
 # --class org.apache.spark.examples.sql.rdd.SparkWc \
export SPARK_SCALA_VERSION="2.12"
export SPARK_VERSION="2.4.2"
export HADOOP_VERSOIN="2.6.5"
../bin/spark-submit \
  --class org.apache.spark.examples.streaming.NetworkWordCount \
  --master mesos://zk://127.0.0.1:5050 \
  --executor-memory 2G \
  --total-executor-cores 2\
  --deploy-mode  client \
  http://master:80/examples/target/original-spark-examples_-$SPARK_VERSION.jar  master 9999
