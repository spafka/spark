package org.spafka.spark

object SparkSubmitNetWordCount {

  import org.apache.spark.deploy.SparkSubmit

  def main(args: Array[String]): Unit = {

    val strings: Array[String] = ("--master=spark://0.0.0.0:7077\n" +
      "--class=org.apache.spark.examples.sql.streaming.StructuredNetworkWordCount\n" +
      "--verbose=true\n" +
      "--deploy-mode=client\n" +
      "--supervise\n" +
      "--executor-memory=2G\n" +
      "--total-executor-cores=2\n" +
      "mini-cluster/spark-server/target/spark-server-1.0.jar\n")
      .split("\n")

    var args2: Array[String] = null
    if (args.length <= 2) {
      args2 = (Array("localhost", "5555"))
    } else {
      args2 = args
    }

    SparkSubmit.main(strings ++ args2);
  }

}
