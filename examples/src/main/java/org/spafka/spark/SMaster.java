package org.spafka.spark;

import org.apache.spark.deploy.master.Master;


public class SMaster {

    public static void main(String[] args) {

        System.setProperty("SPARK_MASTER_IP", "localhost");
        System.setProperty("SPARK_HOME","/tmp");
        Master.main(args);
    }
}
