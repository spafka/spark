package org.spafka.spark

object Repl {

  def main(args: Array[String]): Unit = {

    System.setProperty("scala.usejavacp", "true")
    System.setProperty("spark.master", "local[*]")

    "D:\\OpenAi\\Apache\\flink-spark-internal\\mini-cluster\\spark-server\\target"
    org.apache.spark.repl.Main.main(args)
  }

}
