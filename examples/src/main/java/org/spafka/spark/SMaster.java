package org.spafka.spark;

import org.apache.spark.deploy.master.Master;


public class SMaster {

    public static void main(String[] args) {

        System.setProperty("SPARK_MASTER_IP", "localhost");
        System.setProperty("SPARK_HOME","spark-2.4.2-bin-2.6/spark-2.4.2-bin-2.6.5");
        Master.main(args);
    }
}
