package org.spafka.spark

object SparkSubmitPi {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.deploy.SparkSubmit

    val strings: Array[String] = ("--master=spark://0.0.0.0:7077\n" +
      "--class=org.apache.spark.examples.SparkPi\n" +
      "--verbose=true\n" +
      "--deploy-mode=cluster\n" +
      "--supervise\n" +
      "--executor-memory=2G\n" +
      "--total-executor-cores=2\n" +
      "mini-cluster/spark-server/target/spark-server-1.0.jar\n")
      .split("\n")

    SparkSubmit.main(strings)
  }
}
