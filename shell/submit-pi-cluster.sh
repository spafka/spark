#!/usr/bin/env bash


export SPARK_SCALA_VERSION="2.12"
export SPARK_VERSION="2.4.2"
export HADOOP_VERSOIN="2.6.5"
# Run on a Spark standalone cluster
../bin/spark-submit \
  --class org.apache.spark.examples.streaming.NetworkWordCount \
  --master mesos://127.0.0.1:5050 \
  --executor-memory 1G \
  --total-executor-cores 2\
  --deploy-mode  cluster \
  http://master:80/examples/target/original-spark-examples_$SPARK_SCALA_VERSION-$SPARK_VERSION.jar master 9999