###sparkContext
sparkContext是spark的核心，提供了对任务的划分，DAG，TASK，EXECUTOR,MAPSTATUS等

driver 
```scala
 // Create the Spark execution environment (cache, map output tracker, etc)
    _env = createSparkEnv(_conf, isLocal, listenerBus)


private def create(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Option[Int],
      isLocal: Boolean,
      numUsableCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    logError(s"sparkContext 创建driver => ${conf.getAll},${executorId}," +
      s"${bindAddress},${port},${isLocal},${numUsableCores}")

    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

    // Listener bus is only used on the driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }

    val securityManager = new SecurityManager(conf, ioEncryptionKey)
    if (isDriver) {
      securityManager.initializeAuth()
    }

    ioEncryptionKey.foreach { _ =>
      if (!securityManager.isEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    val systemName = if (isDriver) driverSystemName else executorSystemName
    val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port.getOrElse(-1), conf,
      securityManager, numUsableCores, !isDriver)

    // Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
    if (isDriver) {
      conf.set("spark.driver.port", rpcEnv.address.port.toString)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }
    // fixme 使用默认的java序列化工具
    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logError(s"Using serializer: ${serializer.getClass}")

    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

    val closureSerializer = new JavaSerializer(conf)

    def registerOrLookupEndpoint(
        name: String, endpointCreator: => RpcEndpoint):
      RpcEndpointRef = {
      if (isDriver) {
        logInfo("Registering " + name)
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }

    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)
    // fixme shuffler

    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
    // requires the MapOutputTracker itself
    // fixme shuffler endpoint
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass =
      shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

    val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)
    val memoryManager: MemoryManager =
      if (useLegacyMemoryManager) {
        new StaticMemoryManager(conf, numUsableCores)
      } else {
        UnifiedMemoryManager(conf, numUsableCores)
      }

    val blockManagerPort = if (isDriver) {
      conf.get(DRIVER_BLOCK_MANAGER_PORT)
    } else {
      conf.get(BLOCK_MANAGER_PORT)
    }
    // fixme blockTransferService
    val blockTransferService =
      new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress,
        blockManagerPort, numUsableCores)

    val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
      new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
      conf, isDriver)

    // NB: blockManager is not valid until initialize() is called later.
    val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
      serializerManager, conf, memoryManager, mapOutputTracker, shuffleManager,
      blockTransferService, securityManager, numUsableCores)

    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set("spark.executor.id", executorId)
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      msstart()
      ms
    }

    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {ew OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      memoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) {
      val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
      envInstance.driverTmpDir = Some(sparkFilesDir)

      logError(s"driver 临时目录 ${sparkFilesDir}")
    }

    envInstance
  }

```

```scala
  // Set SPARK_USER for user who is running SparkContext.
  val sparkUser = Utils.getCurrentUserName()

  private[spark] def schedulerBackend: SchedulerBackend = _schedulerBackend

  private[spark] def taskScheduler: TaskScheduler = _taskScheduler
  private[spark] def taskScheduler_=(ts: TaskScheduler): Unit = {
    _taskScheduler = ts
  }

  private[spark] def dagScheduler: DAGScheduler = _dagScheduler
  private[spark] def dagScheduler_=(ds: DAGScheduler): Unit = {
    _dagScheduler = ds
  }
  
   // Create the Spark execution environment (cache, map output tracker, etc)
   _env = createSparkEnv(_conf, isLocal, listenerBus)
   
   
   默认的executor memory
   _executorMemory = _conf.getOption("spark.executor.memory")
         .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
         .orElse(Option(System.getenv("SPARK_MEM"))
         .map(warnSparkMem))
         .map(Utils.memoryStringToMb)
         .getOrElse(1024)
         
     // 主要接收executor存活的消息
     // We need to register "HeartbeatReceiver" before "createTaskScheduler" because Executor will
     // retrieve "HeartbeatReceiver" in the constructor. (SPARK-6640)
    _heartbeatReceiver = env.rpcEnv.setupEndpoint(
        HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))
```


[DAGScheduler](https://github.com/spafka/spark/tree/master/MDZ/spark-dagScheduler.md) 是spark根据Rdd类型生成有向无环图 ，划分stage
[taskScheduler](https://github.com/spafka/spark/tree/master/MDZ/spark-taskcheduler.md)是spark根据具体的cluster mode，根据不同的集群api提交task，执行任务的（YARN，STANDALONE，MESOS）
```scala
// Create and start the scheduler
    val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)
    _schedulerBackend = sched
    _taskScheduler = ts
    _dagScheduler = new DAGScheduler(this)
    _heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)

    // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's
    // constructor
    _taskScheduler.start()
```
SparkContext.createTaskScheduler 根据集群模式，用不同的集群api提交任务
```scala

 /**
   * Create a task scheduler based on a given master URL.
   * Return a 2-tuple of the scheduler backend and the task scheduler.
   */
  private def createTaskScheduler(
      sc: SparkContext,
      master: String,
      deployMode: String): (SchedulerBackend, TaskScheduler) = {
    import SparkMasterRegex._

    // When running locally, don't try to re-execute tasks on failure.
    val MAX_LOCAL_TASK_FAILURES = 1

    master match {
      case "local" =>
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, 1)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_N_REGEX(threads) =>
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        if (threadCount <= 0) {
          throw new SparkException(s"Asked to run locally with $threadCount threads")
        }
        val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
        // local[*, M] means the number of cores on the computer with M failures
        // local[N, M] means exactly N threads with M failures
        val threadCount = if (threads == "*") localCpuCount else threads.toInt
        val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
        val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
        scheduler.initialize(backend)
        (backend, scheduler)

      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        val masterUrls = sparkUrl.split(",").map("spark://" + _)
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        (backend, scheduler)

      case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
        // Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
        val memoryPerSlaveInt = memoryPerSlave.toInt
        if (sc.executorMemory > memoryPerSlaveInt) {
          throw new SparkException(
            "Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
              memoryPerSlaveInt, sc.executorMemory))
        }

        val scheduler = new TaskSchedulerImpl(sc)
        val localCluster = new LocalSparkCluster(
          numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt, sc.conf)
        val masterUrls = localCluster.start()
        val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        backend.shutdownCallback = (backend: StandaloneSchedulerBackend) => {
          localCluster.stop()
        }
        (backend, scheduler)

        // 编译时，如果带上mesos或yarn ，则就能反射，获取mesos或yarn的 resource-managers
           case masterUrl =>
             // 例如 cm=MesosClusterManager
             val cm = getClusterManager(masterUrl) match {
               case Some(clusterMgr) => clusterMgr
               case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
             }
             try {
               val scheduler = cm.createTaskScheduler(sc, masterUrl)
               val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
               cm.initialize(scheduler, backend)
               (backend, scheduler)
             } catch {
               case se: SparkException => throw se
               case NonFatal(e) =>
                 throw new SparkException("External scheduler cannot be instantiated", e)
             }
         }
  }

```

Mesos 的 集群task api
[CoarseGrainedExecutorBackend](https://github.com/spafka/spark/tree/master/MDZ/CoarseGrainedExecutorBackend.md)
```scala
/**
 * Cluster Manager for creation of Mesos scheduler and backend
 */
private[spark] class MesosClusterManager extends ExternalClusterManager {
  private val MESOS_REGEX = """mesos://(.*)""".r

  override def canCreate(masterURL: String): Boolean = {
    masterURL.startsWith("mesos")
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskSchedulerImpl(sc)
  }

  override def createSchedulerBackend(sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    require(!sc.conf.get(IO_ENCRYPTION_ENABLED),
      "I/O encryption is currently not supported in Mesos.")

    val mesosUrl = MESOS_REGEX.findFirstMatchIn(masterURL).get.group(1)
    val coarse = sc.conf.getBoolean("spark.mesos.coarse", defaultValue = true)
    if (coarse) {
      new MesosCoarseGrainedSchedulerBackend(
        scheduler.asInstanceOf[TaskSchedulerImpl],
        sc,
        mesosUrl,
        sc.env.securityManager)
    } else {
      new MesosFineGrainedSchedulerBackend(
        scheduler.asInstanceOf[TaskSchedulerImpl],
        sc,
        mesosUrl)
    }
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}


```

