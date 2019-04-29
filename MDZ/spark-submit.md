spark-submit.sh -> org.apache.spark.launcher.Main -> org.apache.spark.deploy.SparkSubmit

直接准备spark-submit参数

```scala
args= --master mesos://localhost:5050 --class org.apache.spark.examples.SparkPi  --executor-memory 1G --total-executor-cores 2 /Users/spafka/Desktop/flink/spark/shell/../examples/target/original-spark-examples_2.11-2.4.0-SNAPSHOT.jar

export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.dylib

```
ok let's go

![准备图 ](https://raw.githubusercontent.com/spafka/spark/master/MDZ/images/spark-submit-launch.png)

```scala
 
 val appArgs = new SparkSubmitArguments(args)
    if (appArgs.verbose) {
      // scalastyle:off println
      printStream.println(appArgs)
      // scalastyle:on println
    }
    appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs, uninitLog)
      case SparkSubmitAction.KILL => kill(appArgs)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
    }
```


```scala
 private[deploy] def prepareSubmitEnvironment(
      args: SparkSubmitArguments,
      conf: Option[HadoopConfiguration] = None)
      : (Seq[String], Seq[String], SparkConf, String) = {
    // Return values
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    val sparkConf = new SparkConf()
    var childMainClass = ""
    
     val isYarnCluster = clusterManager == YARN && deployMode == CLUSTER
        val isMesosCluster = clusterManager == MESOS && deployMode == CLUSTER
        val isStandAloneCluster = clusterManager == STANDALONE && deployMode == CLUSTER
        val isKubernetesCluster = clusterManager == KUBERNETES && deployMode == CLUSTER

```
对于mesos post jar to spark-mesos-dispatcher FrameWork
```scala

private[spark] class RestSubmissionClientApp extends SparkApplication {

  /** Submits a request to run the application and return the response. Visible for testing. */
  def run(
      appResource: String,
      mainClass: String,
      appArgs: Array[String],
      conf: SparkConf,
      env: Map[String, String] = Map()): SubmitRestProtocolResponse = {
    val master = conf.getOption("spark.master").getOrElse {
      throw new IllegalArgumentException("'spark.master' must be set.")
    }
    val sparkProperties = conf.getAll.toMap
    val client = new RestSubmissionClient(master)
    val submitRequest = client.constructSubmitRequest(
      appResource, mainClass, appArgs, sparkProperties, env)
    client.createSubmission(submitRequest)
  }

  override def start(args: Array[String], conf: SparkConf): Unit = {
    if (args.length < 2) {
      sys.error("Usage: RestSubmissionClient [app resource] [main class] [app args*]")
      sys.exit(1)
    }
    val appResource = args(0)
    val mainClass = args(1)
    val appArgs = args.slice(2, args.length)
    val env = RestSubmissionClient.filterSystemEnvironment(sys.env)
    run(appResource, mainClass, appArgs, conf, env)
  }

}

```
加载额外的jar,根据不同的集群模型加载不同的内容
```scala
var mainClass: Class[_] = null

    try {
      mainClass = Utils.classForName(childMainClass)
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace(printStream)
        if (childMainClass.contains("thriftserver")) {
          // scalastyle:off println
          printStream.println(s"Failed to load main class $childMainClass.")
          printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
          // scalastyle:on println
        }
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
      case e: NoClassDefFoundError =>
        e.printStackTrace(printStream)
        if (e.getMessage.contains("org/apache/hadoop/hive")) {
          // scalastyle:off println
          printStream.println(s"Failed to load hive class.")
          printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
          // scalastyle:on println
        }
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
    }

    val app: SparkApplication = if (classOf[SparkApplication].isAssignableFrom(mainClass)) {
      mainClass.newInstance().asInstanceOf[SparkApplication]
    } else {
      // SPARK-4170
      if (classOf[scala.App].isAssignableFrom(mainClass)) {
        printWarning("Subclasses of scala.App may not work correctly. Use a main() method instead.")
      }
      new JavaMainApplication(mainClass)
    }
    
     try {
          app.start(childArgs.toArray, sparkConf)
        }
      // 加载 spark-submit 的环境变量
      val sysProps = conf.getAll.toMap
         sysProps.foreach { case (k, v) =>
           sys.props(k) = v
         }
         // 反射执行
   mainMethod.invoke(null, args)
        
```

sparkContext 会从system.properties读取master等信息，但sc中硬编码的master优先级最高
```scala
class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  import SparkConf._

  /** Create a SparkConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new SparkConfigProvider(settings))
    _reader.bindEnv(new ConfigProvider {
      override def get(key: String): Option[String] = Option(getenv(key))
    })
    _reader
  }

  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  private[spark] def loadFromSystemProperties(silent: Boolean): SparkConf = {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
      set(key, value, silent)
    }
    this
  }

```


 ```text
8/01/23 23:58:38 DEBUG RestSubmissionClient: Sending POST request to server at http://localhost:7077/v1/submissions/create:
{
  "action" : "CreateSubmissionRequest",
  "appArgs" : [ ],
  "appResource" : "file:/Users/spafka/Desktop/flink/spark/shell/../examples/target/original-spark-examples_2.11-2.4.0-SNAPSHOT.jar",
  "clientSparkVersion" : "2.4.0-SNAPSHOT",
  "environmentVariables" : {
    "MESOS_NATIVE_JAVA_LIBRARY" : "/usr/local/lib/libmesos.dylib"
  },
  "mainClass" : "org.apache.spark.examples.SparkPi",
  "sparkProperties" : {
    "spark.jars" : "file:/Users/spafka/Desktop/flink/spark/shell/../examples/target/original-spark-examples_2.11-2.4.0-SNAPSHOT.jar",
    "spark.driver.supervise" : "false",
    "spark.app.name" : "org.apache.spark.examples.SparkPi",
    "spark.cores.max" : "2",
    "spark.submit.deployMode" : "cluster",
    "spark.master" : "mesos://localhost:7077",
    "spark.executor.memory" : "1G"
  }
}
18/01/23 23:58:38 DEBUG RestSubmissionClient: Response from the server:
{
  "action" : "CreateSubmissionResponse",
  "serverSparkVersion" : "2.4.0-SNAPSHOT",
  "submissionId" : "driver-20180123235838-0002",
  "success" : true
}
18/01/23 23:58:38 INFO RestSubmissionClient: Submission successfully created as driver-20180123235838-0002. Polling submission state...
18/01/23 23:58:38 INFO RestSubmissionClient: Submitting a request for the status of submission driver-20180123235838-0002 in mesos://localhost:7077.
18/01/23 23:58:38 DEBUG RestSubmissionClient: Sending GET request to server at http://localhost:7077/v1/submissions/status/driver-20180123235838-0002.
18/01/23 23:58:38 DEBUG RestSubmissionClient: Response from the server:
{
  "action" : "SubmissionStatusResponse",
  "driverState" : "QUEUED",
  "serverSparkVersion" : "2.4.0-SNAPSHOT",
  "submissionId" : "driver-20180123235838-0002",
  "success" : true
}
18/01/23 23:58:38 INFO RestSubmissionClient: State of driver driver-20180123235838-0002 is now QUEUED.
18/01/23 23:58:38 INFO RestSubmissionClient: Server responded with CreateSubmissionResponse:
{
  "action" : "CreateSubmissionResponse",
  "serverSparkVersion" : "2.4.0-SNAPSHOT",
  "submissionId" : "driver-20180123235838-0002",
  "success" : true
}
```


