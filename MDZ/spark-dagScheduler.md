spark stage划分


WordCount的任务划分
```scala

 val conf = new SparkConf().set("spark.logLineage","true")
    val spark = SparkSession
      .builder
      .config(conf)
      .appName("Spark WC")
        .master("local[*]")
      .getOrCreate()

    val value = spark
      .sparkContext
      .textFile("/Users/spafka/Desktop/flink/spark/CONTRIBUTING.md")
      .flatMap(_.split(" "))
      .map((_, 1))
      .combineByKey(
        (v: Int) => v,
        (c: Int, v: Int) => c+v,
        (c1: Int, c2: Int) => c1 + c2
      )
   //   println(value.toDebugString)

    value.collect().foreach(println)
```

collect是一个action算子，代码执行到collect才会提交Job

```scala
/**
   * Return an array that contains all of the elements in this RDD.
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   * all the data is loaded into the driver's memory.
   */
  def collect(): Array[T] = withScope {
    val results: Array[Array[T]] = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    logWarning(s"rdd.colleect => ${results}")
    Array.concat(results: _*)
  }


```

```scala
 
    /**
     * Run a function on a given set of partitions in an RDD and pass the results to the given
     * handler function. This is the main entry point for all actions in Spark.
     *
     * @param rdd target RDD to run tasks on
     * @param func a function to run on each partition of the RDD
     * @param partitions set of partitions to run on; some jobs may not want to compute on all
     * partitions of the target RDD, e.g. for operations like `first()`
     * @param resultHandler callback to pass each result to
     */
    def runJob[T, U: ClassTag](
        rdd: RDD[T],
        func: (TaskContext, Iterator[T]) => U,
        partitions: Seq[Int],
        resultHandler: (Int, U) => Unit): Unit = {
      if (stopped.get()) {
        throw new IllegalStateException("SparkContext has been shutdown")
      }
      val callSite = getCallSite
      val cleanedFunc = clean(func)
      logInfo("Starting job: " + callSite.shortForm)
      if (conf.getBoolean("spark.logLineage", false)) {
        logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
      }
      dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
      progressBar.foreach(_.finishAll())
      rdd.doCheckpoint()
    }
    
     /**
       * Run an action job on the given RDD and pass all the results to the resultHandler function as
       * they arrive.
       *
       * @param rdd target RDD to run tasks on
       * @param func a function to run on each partition of the RDD
       * @param partitions set of partitions to run on; some jobs may not want to compute on all
       *   partitions of the target RDD, e.g. for operations like first()
       * @param callSite where in the user program this job was called
       * @param resultHandler callback to pass each result to
       * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
       *
       * @note Throws `Exception` when the job fails
       */
      def runJob[T, U](
          rdd: RDD[T],
          func: (TaskContext, Iterator[T]) => U,
          partitions: Seq[Int],
          callSite: CallSite,
          
          // rpcHandler int是分区编号，对每个partion的结果的handler
          resultHandler: (Int, U) => Unit,
          properties: Properties): Unit = {
        val start = System.nanoTime
        logWarning(s"====================++++++++++++====================")
        logWarning(s"spark 提交Job ++ >> rdd:${rdd}，func：${func},  partitions:${partitions}, callSite:${callSite}, resultHandler:${resultHandler}, properties:${properties})")
        val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
        ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)
        waiter.completionFuture.value.get match {
          case scala.util.Success(_) =>
            logInfo("Job %d finished: %s, took %f s".format
              (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
          case scala.util.Failure(exception) =>
            logInfo("Job %d failed: %s, took %f s".format
              (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
            // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
            val callerStackTrace = Thread.currentThread().getStackTrace.tail
            exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
            throw exception
        }
      }


```

```scala
/**
   * Submit an action job to the scheduler.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    
    // 提交一个job
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    
    // 自定义的线程池模型，接受模式匹配的Job类型，处理提交的类型
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }

```


```scala

// DAG事件处理器，处理
private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId, reason) =>
      dagScheduler.handleStageCancellation(stageId, reason)

    case JobCancelled(jobId, reason) =>
      dagScheduler.handleJobCancellation(jobId, reason)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val workerLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, workerLost)

    case WorkerRemoved(workerId, host, message) =>
      dagScheduler.handleWorkerRemoved(workerId, host, message)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case SpeculativeTaskSubmitted(task) =>
      dagScheduler.handleSpeculativeTaskSubmitted(task)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }


/**
 * An event loop to receive events from the caller and process all events in the event thread. It
 * will start an exclusive event thread to process all events.
 *
 * Note: The event queue will grow indefinitely. So subclasses should make sure `onReceive` can
 * handle events in time to avoid the potential OOM.
 */
private[spark] abstract class EventLoop[E](name: String) extends Logging {

  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()

  private val stopped = new AtomicBoolean(false)

  private val eventThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = {
      try {
        while (!stopped.get) {
          val event = eventQueue.take()
          try {

            logWarning(s"got enevt from eventLoopQueen >> ${event.getClass}")
            onReceive(event)
          } catch {
            case NonFatal(e) =>
              try {
                onError(e)
              } catch {
                case NonFatal(e) => logError("Unexpected error in " + name, e)
              }
          }
        }
      } catch {
        case ie: InterruptedException => // exit even if eventQueue is not empty
        case NonFatal(e) => logError("Unexpected error in " + name, e)
      }
    }

  }

  def start(): Unit = {
    if (stopped.get) {
      throw new IllegalStateException(name + " has already been stopped")
    }
    // Call onStart before starting the event thread to make sure it happens before onReceive
    onStart()
    eventThread.start()
  }

  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      eventThread.interrupt()
      var onStopCalled = false
      try {
        eventThread.join()
        // Call onStop after the event thread exits to make sure onReceive happens before onStop
        onStopCalled = true
        onStop()
      } catch {
        case ie: InterruptedException =>
          Thread.currentThread().interrupt()
          if (!onStopCalled) {
            // ie is thrown from `eventThread.join()`. Otherwise, we should not call `onStop` since
            // it's already called.
            onStop()
          }
      }
    } else {
      // Keep quiet to allow calling `stop` multiple times.
    }
  }

  /**
   * Put the event into the event queue. The event thread will process it later.
   */
  def post(event: E): Unit = {
    eventQueue.put(event)
  }

  /**
   * Return if the event thread has already been started but not yet stopped.
   */
  def isActive: Boolean = eventThread.isAlive

  /**
   * Invoked when `start()` is called but before the event thread starts.
   */
  protected def onStart(): Unit = {}

  /**
   * Invoked when `stop()` is called and the event thread exits.
   */
  protected def onStop(): Unit = {}

  /**
   * Invoked in the event thread when polling events from the event queue.
   *
   * Note: Should avoid calling blocking actions in `onReceive`, or the event thread will be blocked
   * and cannot process events in time. If you want to call some blocking actions, run them in
   * another thread.
   */
  protected def onReceive(event: E): Unit

  /**
   * Invoked if `onReceive` throws any non fatal error. Any non fatal error thrown from `onError`
   * will be ignored.
   */
  protected def onError(e: Throwable): Unit

}

```

```scala
private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      
        // stage的划分，十分重要
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)
  }

```

```scala
  /**
   * Create a ResultStage associated with the provided jobId.
   */
  private def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {

    val parents = getOrCreateParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    stageIdToStage(id) = stage

    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

```

Stage生成
Stage的调度是由DAGScheduler完成的。由RDD的有向无环图DAG切分出了Stage的有向无环图DAG。Stage的DAG通过最后执行的Stage为根进行广度优先遍历，遍历到最开始执行的Stage执行，如果提交的Stage仍有未完成的父母Stage，则Stage需要等待其父Stage执行完才能执行。同时DAGScheduler中还维持了几个重要的Key-Value集合结构，用来记录Stage的状态，这样能够避免过早执行和重复提交Stage。waitingStages中记录仍有未执行的父母Stage，防止过早执行。runningStages中保存正在执行的Stage，防止重复执行。failedStages中保存执行失败的Stage，需要重新执行，这里的设计是出于容错的考虑。

  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]
依赖关系
RDD的窄依赖是指父RDD的所有输出都会被指定的子RDD消费，即输出路径是固定的；宽依赖是指父RDD的输出会由不同的子RDD消费，即输出路径不固定。
调度器会计算RDD之间的依赖关系，将拥有持续窄依赖的RDD归并到同一个Stage中，而宽依赖则作为划分不同Stage的判断标准。
导致窄依赖的Transformation操作：map、flatMap、filter、sample；导致宽依赖的Transformation操作：sortByKey、reduceByKey、groupByKey、cogroupByKey、join、cartensian。

Stage分为两种：
ShuffleMapStage, in which case its tasks' results are input for another stage
其实就是,非最终stage, 后面还有其他的stage, 所以它的输出一定是需要shuffle并作为后续的输入。

这种Stage是以Shuffle为输出边界，其输入边界可以是从外部获取数据，也可以是另一个ShuffleMapStage的输出
其输出可以是另一个Stage的开始。
ShuffleMapStage的最后Task就是ShuffleMapTask。
在一个Job里可能有该类型的Stage，也可以能没有该类型Stage。

ResultStage, in which case its tasks directly compute the action that initiated a job (e.g. count(), save(), etc)
最终的stage, 没有输出, 而是直接产生结果或存储。

这种Stage是直接输出结果，其输入边界可以是从外部获取数据，也可以是另一个ShuffleMapStage的输出。
ResultStage的最后Task就是ResultTask，在一个Job里必定有该类型Stage。
一个Job含有一个或多个Stage，但至少含有一个ResultStage。

Stage类
stage的RDD参数只有一个RDD, final RDD, 而不是一系列的RDD。
因为在一个stage中的所有RDD都是map, partition不会有任何改变, 只是在data依次执行不同的map function所以对于TaskScheduler而言, 一个RDD的状况就可以代表这个stage。

Stage参数说明：
val id: Int //Stage的序号数值越大，优先级越高
val rdd: RDD[], //归属于本Stage的最后一个rdd
val numTasks: Int, //创建的Task数目，等于父RDD的输出Partition数目
val shuffleDep: Option[ShuffleDependency[, _, _]], //是否存在SuffleDependency，宽依赖
val parents: List[Stage], //父Stage列表
val jobId: Int //作业ID

```scala
private[spark] class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val shuffleDep: Option[ShuffleDependency[_, _, _]],  // Output shuffle if stage is a map stage
    val parents: List[Stage],
    val jobId: Int,
    val callSite: CallSite)
  extends Logging {

  val isShuffleMap = shuffleDep.isDefined
  val numPartitions = rdd.partitions.size
  val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)
  var numAvailableOutputs = 0

  /** Set of jobs that this stage belongs to. */
  val jobIds = new HashSet[Int]

  /** For stages that are the final (consists of only ResultTasks), link to the ActiveJob. */
  var resultOfJob: Option[ActiveJob] = None
  var pendingTasks = new HashSet[Task[_]]

  private var nextAttemptId = 0

  val name = callSite.shortForm
  val details = callSite.longForm

  /** Pointer to the latest [StageInfo] object, set by DAGScheduler. */
  var latestInfo: StageInfo = StageInfo.fromStage(this)

  def isAvailable: Boolean = {
    if (!isShuffleMap) {
      true
    } else {
      numAvailableOutputs == numPartitions
    }
  }

  def addOutputLoc(partition: Int, status: MapStatus) {
    val prevList = outputLocs(partition)
    outputLocs(partition) = status :: prevList
    if (prevList == Nil) {
      numAvailableOutputs += 1
    }
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId) {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      numAvailableOutputs -= 1
    }
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String) {
    var becameUnavailable = false
    for (partition <- 0 until numPartitions) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, numAvailableOutputs, numPartitions, isAvailable))
    }
  }

  /** Return a new attempt id, starting with 0. */
  def newAttemptId(): Int = {
    val id = nextAttemptId
    nextAttemptId += 1
    id
  }

  def attemptId: Int = nextAttemptId

  override def toString = "Stage " + id

  override def hashCode(): Int = id

  override def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }
}
```

处理Job，分割Job为Stage，封装Stage成TaskSet，最终提交给TaskScheduler的调用链
dagScheduler.handleJobSubmitted-->dagScheduler.submitStage-->dagScheduler.submitMissingTasks-->taskScheduler.submitTasks。

handleJobSubmitted函数
函数handleJobSubmitted和submitStage主要负责依赖性分析，对其处理逻辑做进一步的分析。
handleJobSubmitted最主要的工作是生成Stage，并根据finalStage来产生ActiveJob。

```scala
 private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      allowLocal: Boolean,
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: Stage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = newStage(finalRDD, partitions.size, None, jobId, callSite)
    } catch {
      //错误处理，告诉监听器作业失败，返回....
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    if (finalStage != null) {
      val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
      clearCacheLocs()
      logInfo("Got job %s (%s) with %d output partitions (allowLocal=%s)".format(
        job.jobId, callSite.shortForm, partitions.length, allowLocal))
      logInfo("Final stage: " + finalStage + "(" + finalStage.name + ")")
      logInfo("Parents of final stage: " + finalStage.parents)
      logInfo("Missing parents: " + getMissingParentStages(finalStage))
      val shouldRunLocally =
        localExecutionEnabled && allowLocal && finalStage.parents.isEmpty && partitions.length == 1
      val jobSubmissionTime = clock.getTimeMillis()
      if (shouldRunLocally) {
        // 很短、没有父stage的本地操作，比如 first() or take() 的操作本地执行
        // Compute very short actions like first() or take() with no parent stages locally.
        listenerBus.post(
          SparkListenerJobStart(job.jobId, jobSubmissionTime, Seq.empty, properties))
        runLocally(job)
      } else {
        // collect等操作走的是这个过程，更新相关的关系映射，用监听器监听，然后提交作业
        jobIdToActiveJob(jobId) = job
        activeJobs += job
        finalStage.resultOfJob = Some(job)
        val stageIds = jobIdToStageIds(jobId).toArray
        val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
        listenerBus.post(
          SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
        // 提交stage
        submitStage(finalStage)
      }
    }
    // 提交stage
    submitWaitingStages()
  }
```
 
newStage函数
```scala
 /**
   * Create a Stage -- either directly for use as a result stage, or as part of the (re)-creation
   * of a shuffle map stage in newOrUsedStage.  The stage will be associated with the provided
   * jobId. Production of shuffle map stages should always use newOrUsedStage, not newStage
   * directly.
   */
  private def newStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: Option[ShuffleDependency[_, _, _]],
      jobId: Int,
      callSite: CallSite)
    : Stage =
  {
    val parentStages = getParentStages(rdd, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new Stage(id, rdd, numTasks, shuffleDep, parentStages, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
    }
```
 
其中，Stage的初始化参数：在创建一个Stage之前，需要知道该Stage需要从多少个Partition读入数据，这个数值直接影响要创建多少个Task。也就是说，创建Stage时，已经清楚该Stage需要从多少不同的Partition读入数据，并写出到多少个不同的Partition中，输入和输出的个数均已明确。

getParentStages函数：
通过不停的遍历它之前的rdd，如果碰到有依赖是ShuffleDependency类型的，就通过getShuffleMapStage方法计算出来它的Stage来。

 ```scala
 /**
   * Get or create the list of parent stages for a given RDD. The stages will be assigned the
   * provided jobId if they haven't already been created with a lower jobId.
   */
  private def getParentStages(rdd: RDD[_], jobId: Int): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              parents += getShuffleMapStage(shufDep, jobId)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    parents.toList
  }
```
ActiveJob类
用户所提交的job在得到DAGScheduler的调度后，会被包装成ActiveJob，同时会启动JobWaiter阻塞监听job的完成状况。
同时依据job中RDD的dependency和dependency属性(NarrowDependency，ShufflerDependecy)，DAGScheduler会根据依赖关系的先后产生出不同的stage DAG(result stage, shuffle map stage)。
在每一个stage内部，根据stage产生出相应的task，包括ResultTask或是ShuffleMapTask，这些task会根据RDD中partition的数量和分布，产生出一组相应的task，并将其包装为TaskSet提交到TaskScheduler上去。
```scala
/**
 * Tracks information about an active job in the DAGScheduler.
 */
private[spark] class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,
    val func: (TaskContext, Iterator[_]) => _,
    val partitions: Array[Int],
    val callSite: CallSite,
    val listener: JobListener,
    val properties: Properties) {

  val numPartitions = partitions.length
  val finished = Array.fill[Boolean](numPartitions)(false)
  var numFinished = 0
}
```
submitStage函数
submitStage函数中会根据依赖关系划分stage，通过递归调用从finalStage一直往前找它的父stage，直到stage没有父stage时就调用submitMissingTasks方法提交改stage。这样就完成了将job划分为一个或者多个stage。
submitStage处理流程：

所依赖的Stage是否都已经完成，如果没有完成则先执行所依赖的Stage
如果所有的依赖已经完成，则提交自身所处的Stage
最后会在submitMissingTasks函数中将stage封装成TaskSet通过taskScheduler.submitTasks函数提交给TaskScheduler处理。
```scala
/** Submits stage, but first recursively submits any missing parents. */
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id) // 根据final stage发现是否有parent stage
        logDebug("missing: " + missing)
        if (missing == Nil) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get) // 如果没有parent stage需要执行, 则直接submit当前stage
        } else {
          for (parent <- missing) {
            submitStage(parent) // 如果有parent stage,需要先submit parent, 因为stage之间需要顺序执行
          }
          waitingStages += stage // 当前stage放到waitingStages中
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id)
    }
  }
```
getMissingParentStages
getMissingParentStages通过图的遍历，来找出所依赖的所有父Stage。
```scala

  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        if (getCacheLocs(rdd).contains(Nil)) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>  // 如果发现ShuffleDependency, 说明遇到新的stage
                val mapStage = getShuffleMapStage(shufDep, stage.jobId)
                // check shuffleToMapStage, 如果该stage已经被创建则直接返回, 否则newStage
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] => // 对于NarrowDependency, 说明仍然在这个stage中
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }
```
submitMissingTasks
可见无论是哪种stage，都是对于每个stage中的每个partitions创建task，并最终封装成TaskSet，将该stage提交给taskscheduler。
```scala

  /** Called when stage's parents are available and we can now do its task. */
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    stage.pendingTasks.clear()

    // First figure out the indexes of partition ids to compute.
    val partitionsToCompute: Seq[Int] = {
      if (stage.isShuffleMap) {
        (0 until stage.numPartitions).filter(id => stage.outputLocs(id) == Nil)
      } else {
        val job = stage.resultOfJob.get
        (0 until job.numPartitions).filter(id => !job.finished(id))
      }
    }

    val properties = if (jobIdToActiveJob.contains(jobId)) {
      jobIdToActiveJob(stage.jobId).properties
    } else {
      // this stage will be assigned to "default" pool
      null
    }

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage.latestInfo = StageInfo.fromStage(stage, Some(partitionsToCompute.size))
    outputCommitCoordinator.stageStart(stage.id)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] =
        if (stage.isShuffleMap) {
          closureSerializer.serialize((stage.rdd, stage.shuffleDep.get) : AnyRef).array()
        } else {
          closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func) : AnyRef).array()
        }
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString)
        runningStages -= stage
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}")
        runningStages -= stage
        return
    }

    val tasks: Seq[Task[_]] = if (stage.isShuffleMap) {
      partitionsToCompute.map { id =>
        val locs = getPreferredLocs(stage.rdd, id)
        val part = stage.rdd.partitions(id)
        new ShuffleMapTask(stage.id, taskBinary, part, locs)
      }
    } else {
      val job = stage.resultOfJob.get
      partitionsToCompute.map { id =>
        val p: Int = job.partitions(id)
        val part = stage.rdd.partitions(p)
        val locs = getPreferredLocs(stage.rdd, p)
        new ResultTask(stage.id, taskBinary, part, locs, id)
      }
    }

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      stage.pendingTasks ++= tasks
      logDebug("New pending tasks: " + stage.pendingTasks)
      taskScheduler.submitTasks(
        new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)
      logDebug("Stage " + stage + " is actually done; %b %d %d".format(
        stage.isAvailable, stage.numAvailableOutputs, stage.numPartitions))
    }
  }

```

![shuffleMapStadge.png](https://raw.githubusercontent.com/spafka/spark/tree/master/MDZ/images/shuffleMapStadge.png)

 ```scala
    val taskBinaryBytes: Array[Byte] =
         if (stage.isShuffleMap) {
           closureSerializer.serialize((stage.rdd, stage.shuffleDep.get) : AnyRef).array()
         } else {
           closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func) : AnyRef).array()
         }
 
 ````
 
 对于要序列化的shuffleTask，主要序列化shuffleDep
 
 ```scala
 combineByKey
 (v: Int) => v,
        (c: Int, v: Int) => c+v,
        (c1: Int, c2: Int) => c1 + c2
      )
```
 ![aggreator.png](https://raw.githubusercontent.com/spafka/spark/tree/master/MDZ/images/aggreator.png)
 序列化其中的函数，如aggerator，加载到broadcast，executor接收到此等函数，反序列化之执行task。 具体参考
 
 [spark-taskcheduler.md](https://github.com/spafka/spark/tree/master/MDZ/spark-taskcheduler.md)
 [CoarseGrainedExecutorBackend.md](https://github.com/spafka/spark/tree/master/MDZ/CoarseGrainedExecutorBackend.md)
 
 
参考资料
作者：JasonDing
链接：https://www.jianshu.com/p/d3b794567e2a
