package org.spafka.spark;

import org.apache.spark.executor.MesosExecutorBackend;
// in mesos
public class SCoarseGrainedExecutorBackend {

    public static void main(String[] args) {
        System.setProperty("SPARK_MASTER_IP", "localhost");
        System.setProperty("SPARK_HOME","/tmp");
        MesosExecutorBackend.main(args);
    }
}
