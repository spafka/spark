我们来看ShuffledRDD中的compute方法：

ShuffledRDD::compute
```scala
override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
  val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
  SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
    .read()
    .asInstanceOf[Iterator[(K, C)]]
}
```
可以看到首先调用的是ShuffleManager的getReader方法来获得ShuffleReader，然后再调用ShuffleReader的read方法来读取map阶段输出的中间数据，而不管是HashShuffleManager还是SortShuffleManager，其getReader方法内部都是实例化了BlockStoreShuffleReader，而BlockStoreShuffleReader正是实现了ShuffleReader接口：
SortShuffleManager::getReader
```scala
override def getReader[K, C](
    handle: ShuffleHandle,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext): ShuffleReader[K, C] = {
  new BlockStoreShuffleReader(
    handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
}
```

然后来看BlockStoreShuffleReader的read方法是具体如何工作的，即如何读取Map阶段输出的中间结果的：

/** Read the combined key-values for this reduce task */
override def read(): Iterator[Product2[K, C]] = {
  // 首先实例化ShuffleBlockFetcherIterator
  val blockFetcherItr = new ShuffleBlockFetcherIterator(
    context,
    blockManager.shuffleClient,
    blockManager,
    mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
    // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
    SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024)
  // Wrap the streams for compression based on configuration
  val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
    blockManager.wrapForCompression(blockId, inputStream)
  }
  val ser = Serializer.getSerializer(dep.serializer)
  val serializerInstance = ser.newInstance()
  // Create a key/value iterator for each stream
  val recordIter = wrappedStreams.flatMap { wrappedStream =>
    // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
    // NextIterator. The NextIterator makes sure that close() is called on the
    // underlying InputStream when all records have been read.
    serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
  }
  // Update the context task metrics for each record read.
  val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
  val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
    recordIter.map(record => {
      readMetrics.incRecordsRead(1)
      record
    }),
    context.taskMetrics().updateShuffleReadMetrics())
  // An interruptible iterator must be used here in order to support task cancellation
  val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)
  val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
    if (dep.mapSideCombine) {
      // We are reading values that are already combined
      val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
      dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
    } else {
      // We don't know the value type, but also don't care -- the dependency *should*
      // have made sure its compatible w/ this aggregator, which will convert the value
      // type to the combined type C
      val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
      dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
    }
  } else {
    require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
    interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
  }
  // Sort the output if there is a sort ordering defined.
  dep.keyOrdering match {
    case Some(keyOrd: Ordering[K]) =>
      // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
      // the ExternalSorter won't spill to disk.
      val sorter =
        new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = Some(ser))
      sorter.insertAll(aggregatedIter)
      context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
      context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
      context.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.peakMemoryUsedBytes)
      CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
    case None =>
      aggregatedIter
  }
}
首先实例化ShuffleBlockFetcherIterator，实例化的时候传入了几个参数，这里介绍一下几个重要的：

blockManager.shuffleClient
mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition)
SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024
shuffleClient就是用来读取其他executors上的shuffle文件的，有可能是ExternalShuffleClient或者BlockTransferService：

private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
  val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
  new ExternalShuffleClient(transConf, securityManager, securityManager.isAuthenticationEnabled(),
    securityManager.isSaslEncryptionEnabled())
} else {
  blockTransferService
}
而默认使用的是BlockTransferService，因为externalShuffleServiceEnabled默认为false：

private[spark]
val externalShuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false)
接下来的mapOutputTracker.getMapSizesByExecutorId就是获得该reduce task的数据来源(数据的元数据信息)，传入的参数是shuffle的Id和partition的起始位置，返回的是Seq[(BlockManagerId, Seq[(BlockId, Long)])]，也就是说数据是来自于哪个节点的哪些block的，并且block的数据大小是多少。

def getMapSizesByExecutorId(shuffleId: Int, startPartition: Int, endPartition: Int)
    : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
  logDebug(s"Fetching outputs for shuffle $shuffleId, partitions $startPartition-$endPartition")
  // 获得Map阶段输出的中间计算结果的元数据信息
  val statuses = getStatuses(shuffleId)
  // Synchronize on the returned array because, on the driver, it gets mutated in place
  // 将获得的元数据信息转化成形如Seq[(BlockManagerId, Seq[(BlockId, Long)])]格式的位置信息，用来读取指定的Map阶段产生的数据
  statuses.synchronized {
    return MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses)
  }
}
最后的getSizeAsMb获取的是一项配置参数，代表一次从Map端获取的最大的数据量。

获取元数据信息
下面我们来着重分析一下是怎样通过getStatuses来获取元数据信息的：

private def getStatuses(shuffleId: Int): Array[MapStatus] = {
  // 根据shuffleId获得MapStatus组成的数组：Array[MapStatus]
  val statuses = mapStatuses.get(shuffleId).orNull
  if (statuses == null) {
    // 如果没有获取到就进行fetch操作
    logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
    val startTime = System.currentTimeMillis
    // 用来保存fetch来的MapStatus
    var fetchedStatuses: Array[MapStatus] = null
    fetching.synchronized {
      // 有可能有别的任务正在进行fetch，所以这里使用synchronized关键字保证同步
      // Someone else is fetching it; wait for them to be done
      while (fetching.contains(shuffleId)) {
        try {
          fetching.wait()
        } catch {
          case e: InterruptedException =>
        }
      }
      // Either while we waited the fetch happened successfully, or
      // someone fetched it in between the get and the fetching.synchronized.
      // 等待过后继续尝试获取
      fetchedStatuses = mapStatuses.get(shuffleId).orNull
      if (fetchedStatuses == null) {
        // We have to do the fetch, get others to wait for us.
        fetching += shuffleId
      }
    }
    if (fetchedStatuses == null) {
      // 如果得到了fetch的权利就进行抓取
      // We won the race to fetch the statuses; do so
      logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
      // This try-finally prevents hangs due to timeouts:
      try {
        // 调用askTracker方法发送消息，消息的格式为GetMapOutputStatuses(shuffleId)
        val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
        // 将得到的序列化后的数据进行反序列化
        fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
        logInfo("Got the output locations")
        // 保存到本地的mapStatuses中
        mapStatuses.put(shuffleId, fetchedStatuses)
      } finally {
        fetching.synchronized {
          fetching -= shuffleId
          fetching.notifyAll()
        }
      }
    }
    logDebug(s"Fetching map output statuses for shuffle $shuffleId took " +
      s"${System.currentTimeMillis - startTime} ms")
    if (fetchedStatuses != null) {
      // 最后将抓取到的元数据信息返回
      return fetchedStatuses
    } else {
      logError("Missing all output locations for shuffle " + shuffleId)
      throw new MetadataFetchFailedException(
        shuffleId, -1, "Missing all output locations for shuffle " + shuffleId)
    }
  } else {
    // 如果获取到了Array[MapStatus]就直接返回
    return statuses
  }
}
来看一下用来发送消息的askTracker方法，发送的消息是一个case class：GetMapOutputStatuses(shuffleId)

protected def askTracker[T: ClassTag](message: Any): T = {
  try {
    trackerEndpoint.askWithRetry[T](message)
  } catch {
    case e: Exception =>
      logError("Error communicating with MapOutputTracker", e)
      throw new SparkException("Error communicating with MapOutputTracker", e)
  }
}
MapOutputTrackerMasterEndpoint在接收到该消息后的处理：

override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
  case GetMapOutputStatuses(shuffleId: Int) =>
    val hostPort = context.senderAddress.hostPort
    logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
    // 获得Map阶段的输出数据的序列化后的元数据信息
    val mapOutputStatuses = tracker.getSerializedMapOutputStatuses(shuffleId)
    // 序列化后的大小
    val serializedSize = mapOutputStatuses.length
    // 判断是否超过maxAkkaFrameSize的限制
    if (serializedSize > maxAkkaFrameSize) {
      val msg = s"Map output statuses were $serializedSize bytes which " +
        s"exceeds spark.akka.frameSize ($maxAkkaFrameSize bytes)."
      /* For SPARK-1244 we'll opt for just logging an error and then sending it to the sender.
       * A bigger refactoring (SPARK-1239) will ultimately remove this entire code path. */
      val exception = new SparkException(msg)
      logError(msg, exception)
      context.sendFailure(exception)
    } else {
      // 如果没有超过限制就将获得的元数据信息返回
      context.reply(mapOutputStatuses)
    }
  case StopMapOutputTracker =>
    logInfo("MapOutputTrackerMasterEndpoint stopped!")
    context.reply(true)
    stop()
}
接着来看tracker(MapOutputTrackerMaster)的getSerializedMapOutputStatuses方法：

def getSerializedMapOutputStatuses(shuffleId: Int): Array[Byte] = {
  var statuses: Array[MapStatus] = null
  var epochGotten: Long = -1
  epochLock.synchronized {
    if (epoch > cacheEpoch) {
      cachedSerializedStatuses.clear()
      cacheEpoch = epoch
    }
    // 判断是否已经有缓存的数据
    cachedSerializedStatuses.get(shuffleId) match {
      case Some(bytes) =>
        // 如果有的话就直接返回缓存数据
        return bytes
      case None =>
        // 如果没有的话就从mapStatuses中获得
        statuses = mapStatuses.getOrElse(shuffleId, Array[MapStatus]())
        epochGotten = epoch
    }
  }
  // If we got here, we failed to find the serialized locations in the cache, so we pulled
  // out a snapshot of the locations as "statuses"; let's serialize and return that
  // 序列化操作
  val bytes = MapOutputTracker.serializeMapStatuses(statuses)
  logInfo("Size of output statuses for shuffle %d is %d bytes".format(shuffleId, bytes.length))
  // Add them into the table only if the epoch hasn't changed while we were working
  epochLock.synchronized {
    if (epoch == epochGotten) {
      // 缓存操作
      cachedSerializedStatuses(shuffleId) = bytes
    }
  }
  // 返回序列化后的数据
  bytes
}
获得序列化后的数据后会到getStatuses方法，将得到的序列化后的数据进行反序列化，并将反序列化后的数据保存到该Executor(也就是本地)的mapStatuses中，下次再使用的时候就不必重复的进行fetch操作，最后将获得的元数据信息转化成形如Seq[(BlockManagerId, Seq[(BlockId, Long)])]格式的位置信息，用来读取指定的Map阶段产生的数据。

根据得到的元数据信息抓取数据(分为远程和本地)
说完了这些参数后我们回到ShuffleBlockFetcherIterator的实例化过程，ShuffleBlockFetcherIterator实例化的时候会执行一个initialize()方法，用来进行一系列的初始化操作：

private[this] def initialize(): Unit = {
  // Add a task completion callback (called in both success case and failure case) to cleanup.
  // 不管最后task是success还是failure，都要进行cleanup操作
  context.addTaskCompletionListener(_ => cleanup())
  // Split local and remote blocks.
  // 将local和remote Blocks分离开，并将remote的返回给remoteRequests
  val remoteRequests = splitLocalRemoteBlocks()
  // Add the remote requests into our queue in a random order
  // 这里的fetchRequests是一个队列，我们将远程的请求以随机的顺序加入到该队列，然后使用下面的
  // fetchUpToMaxBytes方法取出队列中的远程请求，同时对大小进行限制
  fetchRequests ++= Utils.randomize(remoteRequests)
  // Send out initial requests for blocks, up to our maxBytesInFlight
  // 从fetchRequests取出远程请求，并使用sendRequest方法发送请求
  fetchUpToMaxBytes()
  val numFetches = remoteRequests.size - fetchRequests.size
  logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))
  // Get Local Blocks
  // 获取本地的Blocks
  fetchLocalBlocks()
  logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
}
首先来看一下splitLocalRemoteBlocks方法是如何将remote和local的blocks分离开来的：

private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
  // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
  // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
  // nodes, rather than blocking on reading output from one node.
  // 为了将大小控制在maxBytesInFlight以下，可以增加并行度，即从1个节点增加到5个
  val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
  logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)
  // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
  // at most maxBytesInFlight in order to limit the amount of data in flight.
  val remoteRequests = new ArrayBuffer[FetchRequest]
  // Tracks total number of blocks (including zero sized blocks)
  var totalBlocks = 0
  for ((address, blockInfos) <- blocksByAddress) {
    totalBlocks += blockInfos.size
    // 这里就是判断所要获取的是本地的block还是远程的block
    if (address.executorId == blockManager.blockManagerId.executorId) {
      // Filter out zero-sized blocks
      // 过滤掉大小为0的blocks
      localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
      numBlocksToFetch += localBlocks.size
    } else {
      // 这里就是远程的部分，主要就是构建remoteRequests
      val iterator = blockInfos.iterator
      var curRequestSize = 0L
      var curBlocks = new ArrayBuffer[(BlockId, Long)]
      while (iterator.hasNext) {
        val (blockId, size) = iterator.next()
        // Skip empty blocks
        if (size > 0) {
          curBlocks += ((blockId, size))
          remoteBlocks += blockId
          numBlocksToFetch += 1
          curRequestSize += size
        } else if (size < 0) {
          throw new BlockException(blockId, "Negative block size " + size)
        }
        // 满足大小的限制就构建一个FetchRequest并加入到remoteRequests中
        if (curRequestSize >= targetRequestSize) {
          // Add this FetchRequest
          remoteRequests += new FetchRequest(address, curBlocks)
          curBlocks = new ArrayBuffer[(BlockId, Long)]
          logDebug(s"Creating fetch request of $curRequestSize at $address")
          curRequestSize = 0
        }
      }
      // Add in the final request
      // 最后将剩余的blocks构成一个FetchRequest
      if (curBlocks.nonEmpty) {
        remoteRequests += new FetchRequest(address, curBlocks)
      }
    }
  }
  logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
  // 最后返回remoteRequests
  remoteRequests
}
这里的FetchRequest是一个数据结构，保存了要获取的blocks的位置信息，而remoteRequests就是这些FetchRequest组成的ArrayBuffer：

case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
  val size = blocks.map(_._2).sum
}
然后使用fetchUpToMaxBytes()方法来获取远程的blocks信息：

private def fetchUpToMaxBytes(): Unit = {
  // Send fetch requests up to maxBytesInFlight
  while (fetchRequests.nonEmpty &&
    (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
    sendRequest(fetchRequests.dequeue())
  }
}
可以看出内部就是从上一步获取的remoteRequests中取出一个FetchRequest并使用sendRequest发送该请求：

private[this] def sendRequest(req: FetchRequest) {
  logDebug("Sending request for %d blocks (%s) from %s".format(
    req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
  bytesInFlight += req.size
  // 1、首先获得要fetch的blocks的信息
  // so we can look up the size of each blockID
  val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
  val blockIds = req.blocks.map(_._1.toString)
  val address = req.address
  // 2、然后通过shuffleClient的fetchBlocks方法来获取对应远程节点上的数据
  // 默认是通过NettyBlockTransferService的fetchBlocks方法实现的
  shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
    new BlockFetchingListener {
      // 3、最后，不管成功还是失败，都将结果保存在results中
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        if (!isZombie) {
          // Increment the ref count because we need to pass this to a different thread.
          // This needs to be released after use.
          buf.retain()
          results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf))
          shuffleMetrics.incRemoteBytesRead(buf.size)
          shuffleMetrics.incRemoteBlocksFetched(1)
        }
        logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
      }
      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        results.put(new FailureFetchResult(BlockId(blockId), address, e))
      }
    }
  )
}
请求的结果最终保存在了results中，成功的就是SuccessFetchResult，失败的就是FailureFetchResult，具体是怎么fetchBlocks的就不在此说明，本文最后会给出一张图进行简要的概述，有兴趣的可以继续进行追踪，其实底层是通过NettyBlockTransferService实现的，通过index文件查找到data文件。

接下来看一下使用fetchLocalBlocks()方法来获取本地的blocks信息的过程：

private[this] def fetchLocalBlocks() {
  val iter = localBlocks.iterator
  while (iter.hasNext) {
    val blockId = iter.next()
    try {
      val buf = blockManager.getBlockData(blockId)
      shuffleMetrics.incLocalBlocksFetched(1)
      shuffleMetrics.incLocalBytesRead(buf.size)
      buf.retain()
      results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf))
    } catch {
      case e: Exception =>
        // If we see an exception, stop immediately.
        logError(s"Error occurred while fetching local blocks", e)
        results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
        return
    }
  }
}
这里就相对简单了，进行迭代，如果获取到就将SuccessFetchResult保存到results中，如果没有就将FailureFetchResult保存到results中，至此ShuffleBlockFetcherIterator的实例化及初始化过程结束，接下来我们再回到BlockStoreShuffleReader的read方法中：

override def read(): Iterator[Product2[K, C]] = {
  val blockFetcherItr = new ShuffleBlockFetcherIterator(
    context,
    blockManager.shuffleClient,
    blockManager,
    mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
    // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
    SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024)
  // Wrap the streams for compression based on configuration
  // 将上面获取的信息进行压缩处理
  val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
    blockManager.wrapForCompression(blockId, inputStream)
  }
  val ser = Serializer.getSerializer(dep.serializer)
  val serializerInstance = ser.newInstance()
  // Create a key/value iterator for each stream
  // 为每个stream创建一个key/value的iterator
  val recordIter = wrappedStreams.flatMap { wrappedStream =>
    // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
    // NextIterator. The NextIterator makes sure that close() is called on the
    // underlying InputStream when all records have been read.
    serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
  }
  // 统计系统相关的部分
  // Update the context task metrics for each record read.
  val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
  val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
    recordIter.map(record => {
      readMetrics.incRecordsRead(1)
      record
    }),
    context.taskMetrics().updateShuffleReadMetrics())
  // An interruptible iterator must be used here in order to support task cancellation
  val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)
  val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
    // 判断是否需要进行map端的聚合操作
    if (dep.mapSideCombine) {
      // We are reading values that are already combined
      val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
      dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
    } else {
      // We don't know the value type, but also don't care -- the dependency *should*
      // have made sure its compatible w/ this aggregator, which will convert the value
      // type to the combined type C
      val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
      dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
    }
  } else {
    require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
    interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
  }
  // 是否需要进行排序
  // Sort the output if there is a sort ordering defined.
  dep.keyOrdering match {
    case Some(keyOrd: Ordering[K]) =>
      // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabl
      // the ExternalSorter won't spill to disk.
      val sorter =
        new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = Some(ser))
      sorter.insertAll(aggregatedIter)
      context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
      context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
      context.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.peakMemoryUsedBytes)
      CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.sto
    case None =>
      aggregatedIter
  }
}
上面代码中aggregator和keyOrdering的部分在分析Shuffle Write的时候已经分析过了，这里我们再简单看一下相关的部分。

aggregator
首先来看aggregator部分：不管是否进行聚合操作，即不管最后执行的是combineCombinersByKey方法还是combineValuesByKey方法，最后都会执行ExternalAppendOnlyMap的insertAll方法：

combineCombinersByKey方法的实现：

def combineCombinersByKey(
    iter: Iterator[_ <: Product2[K, C]],
    context: TaskContext): Iterator[(K, C)] = {
  val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
  combiners.insertAll(iter)
  updateMetrics(context, combiners)
  combiners.iterator
}
combineValuesByKey方法的实现：

def combineValuesByKey(
    iter: Iterator[_ <: Product2[K, V]],
    context: TaskContext): Iterator[(K, C)] = {
  val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
  combiners.insertAll(iter)
  updateMetrics(context, combiners)
  combiners.iterator
}
而这个insertAll方法在Shuffle Write的部分已经介绍过了：

def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
  if (currentMap == null) {
    throw new IllegalStateException(
      "Cannot insert new elements into a map after calling iterator")
  }
  // An update function for the map that we reuse across entries to avoid allocating
  // a new closure each time
  var curEntry: Product2[K, V] = null
  val update: (Boolean, C) => C = (hadVal, oldVal) => {
    if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
  }
  while (entries.hasNext) {
    curEntry = entries.next()
    val estimatedSize = currentMap.estimateSize()
    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
    if (maybeSpill(currentMap, estimatedSize)) {
      currentMap = new SizeTrackingAppendOnlyMap[K, C]
    }
    currentMap.changeValue(curEntry._1, update)
    addElementsRead()
  }
}
而这里的next方法会最终调用ShuffleBlockFetcherIterator的next方法：

override def next(): (BlockId, InputStream) = {
  numBlocksProcessed += 1
  val startFetchWait = System.currentTimeMillis()
  currentResult = results.take()
  val result = currentResult
  val stopFetchWait = System.currentTimeMillis()
  shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)
  result match {
    case SuccessFetchResult(_, _, size, _) => bytesInFlight -= size
    case _ =>
  }
  // Send fetch requests up to maxBytesInFlight
  // 这里就是关键的代码，即不断的去抓去数据，直到抓去到所有的数据
  fetchUpToMaxBytes()
  result match {
    case FailureFetchResult(blockId, address, e) =>
      throwFetchFailedException(blockId, address, e)
    case SuccessFetchResult(blockId, address, _, buf) =>
      try {
        (result.blockId, new BufferReleasingInputStream(buf.createInputStream(), this))
      } catch {
        case NonFatal(t) =>
          throwFetchFailedException(blockId, address, t)
      }
  }
}
可以看到该方法中就是一直抓数据，直到所有的数据都抓取到，然后就是执行combiners.iterator：

override def iterator: Iterator[(K, C)] = {
  if (currentMap == null) {
    throw new IllegalStateException(
      "ExternalAppendOnlyMap.iterator is destructive and should only be called once.")
  }
  if (spilledMaps.isEmpty) {
    CompletionIterator[(K, C), Iterator[(K, C)]](currentMap.iterator, freeCurrentMap())
  } else {
    new ExternalIterator()
  }
}
接下来就看一下ExternalIterator的实例化都做了什么工作：

// A queue that maintains a buffer for each stream we are currently merging
// This queue maintains the invariant that it only contains non-empty buffers
private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]
// Input streams are derived both from the in-memory map and spilled maps on disk
// The in-memory map is sorted in place, while the spilled maps are already in sorted order
// 按照key的hashcode进行排序
private val sortedMap = CompletionIterator[(K, C), Iterator[(K, C)]](
  currentMap.destructiveSortedIterator(keyComparator), freeCurrentMap())
// 将map中的数据和spillFile中的数据的iterator组合在一起
private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)
// 不断迭代，直到将所有数据都读出来，最后将所有的数据保存在mergeHeap中
inputStreams.foreach { it =>
  val kcPairs = new ArrayBuffer[(K, C)]
  readNextHashCode(it, kcPairs)
  if (kcPairs.length > 0) {
    mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
  }
}
最后将所有读取的数据都保存在了mergeHeap中，再来看一下有排序的情况。

keyOrdering
// Sort the output if there is a sort ordering defined.
dep.keyOrdering match {
  case Some(keyOrd: Ordering[K]) =>
    // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
    // the ExternalSorter won't spill to disk.
    val sorter =
      new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = Some(ser))
    sorter.insertAll(aggregatedIter)
    context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
    context.internalMetricsToAccumulators(
      InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.peakMemoryUsedBytes)
    CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
  case None =>
    aggregatedIter
}
可以看到使用了ExternalSorter的insertAll方法，和Shuffle Write的时候操作是一样的，这里我们就不进行重复说明了，具体的内容可以参考上一篇文章，最后还是用张图来总结一下Shuffle Read的流程：

作者：sun4lower
链接：https://www.jianshu.com/p/37828e62e15c
來源：简书
著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。