### SPARK_RPC

spark主要适用于各个分布式组件之间的通讯，底层由netty包装的nio 通信。 clent 向server发送信息
先找到server 的EndPintRef引用，之后随后发送一个RpcMessage ，类似于queen，线程轮训queen，发送消息至Server
，Server有一个InBoxQueen，处理返回结果。发送消息的过程，主要由netty，java编写，位于network-common 下。netty最核心的代码org.apache.spark.network.server.TransportRequestHandler，根据rpc的类型，
交给RpcHandler 分发到不同的Inbox 处理
### server端
```scala
    val host = "localhost"
    // hashMap 的配置文件
    val conf = new SparkConf()
    val clientMode= false
    val port =11111
    val config = RpcEnvConfig(conf, "test", "localhost", "localhost", port,
      new SecurityManager(conf), 0, clientMode)
      
    // 通过工厂模式构造RpcEnv
    val rpcEnv= new NettyRpcEnvFactory().create(config)

    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)
    rpcEnv.setupEndpoint("test", helloEndpoint)
    rpcEnv.awaitTermination()
```

RpcEnv的构造
```scala

private[rpc] class NettyRpcEnvFactory extends RpcEnvFactory with Logging {

  def create(config: RpcEnvConfig): RpcEnv = {
    val sparkConf = config.conf
    // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
    // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
    val javaSerializerInstance =
      new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
    val nettyEnv =
      new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress,
        config.securityManager, config.numUsableCores)
    if (!config.clientMode) {
      val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
        nettyEnv.startServer(config.bindAddress, actualPort)
        (nettyEnv, nettyEnv.address.port)
      }
      try {
        Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
      } catch {
        case NonFatal(e) =>
          nettyEnv.shutdown()
          throw e
      }
    }
    nettyEnv
  }
}

```


看一下nettyPrcEnv 给我们创建了什么
```scala

  // 邮件分发器
  private val dispatcher: Dispatcher = new Dispatcher(this, numUsableCores)
  // ignore 
  private val streamManager = new NettyStreamManager(this)
  // netty的handler与dispatcher的Context
  private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))
```


Dispatcher，由一个线程池处理客户端的请求，投递到不同的endpoint，只做投递的作用
```scala

private[netty] class Dispatcher(nettyEnv: NettyRpcEnv, numUsableCores: Int) extends Logging {

  // EndPoint 即server 主要是server的一些接口，如onConnect等
  // EndPointRef 即Client 主要功能是send和ask（async）
  // inBox 维护了一个消息队列 linkList，为什么不用linkedblockingqueen
  // 因为 处理消息的线程已经是 private val receivers = new LinkedBlockingQueue[EndpointData]
  // 一个endPoint与endPointRef之间就有一个inBox,inBox包含了linked[InboxMessage]
  // 一个 new LinkedBlockingQueue[EndpointData]，接收所有rpchandler，传递景来的消息
  // 一个messageloop 处理blockingqueen中有地的消息
  /**
  * 其中的数据消费已经由 消费线程保证了
  *             private class MessageLoop extends Runnable {
  *                override def run(): Unit = {
  *                  try {
  *                    while (true) {
  *                      try {
  *                        val data = receivers.take()
  *                        if (data == PoisonPill) {
  *                          // Put PoisonPill back so that other MessageLoops can see it.
  *                         receivers.offer(PoisonPill)
  *                         return
  *                       }
  *                      data.inbox.process(Dispatcher.this)
  *                    } catch {
  *                      case NonFatal(e) => logError(e.getMessage, e)
  *                    }
  *                  }
  *                } catch {
  *                   case ie: InterruptedException => // exit
  *                }
  *              }
  *           }
**/
  private class EndpointData(
      val name: String,
      val endpoint: RpcEndpoint,
      val ref: NettyRpcEndpointRef) {
    val inbox = new Inbox(ref, endpoint)
  }

  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  // Track the receivers whose inboxes may contain messages.
  private val receivers = new LinkedBlockingQueue[EndpointData]


```

```scala
  /**
   * Process stored messages.
   */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    inbox.synchronized {
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      message = messages.poll()
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    while (true) {
      safelyCall(endpoint) {
        message match {
          case RpcMessage(_sender, content, context) =>
            try {
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case NonFatal(e) =>
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayMessage(_sender, content) =>
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message $message from ${_sender}")
            })

          case OnStart =>
            endpoint.onStart()
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }

          case OnStop =>
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            dispatcher.removeRpcEndpointRef(endpoint)
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      inbox.synchronized {
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          numActiveThreads -= 1
          return
        }
        message = messages.poll()
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }



```

消息的发送，邮递员的角色
```scala
private val transportContext = new TransportContext(transportConf,
    new NettyRpcHandler(dispatcher, this, streamManager))

```

```scala
/**
 * Dispatches incoming RPCs to registered endpoints.
 *
 * The handler keeps track of all client instances that communicate with it, so that the RpcEnv
 * knows which `TransportClient` instance to use when sending RPCs to a client endpoint (i.e.,
 * one that is not listening for incoming connections, but rather needs to be contacted via the
 * client socket).
 *
 * Events are sent on a per-connection basis, so if a client opens multiple connections to the
 * RpcEnv, multiple connection / disconnection events will be created for that client (albeit
 * with different `RpcAddress` information).
 * 
 * 
 * 接收客户端的rpc消息，分发到dispatcher
 */
private[netty] class NettyRpcHandler(
    dispatcher: Dispatcher,
    nettyEnv: NettyRpcEnv,
    streamManager: StreamManager) extends RpcHandler with Logging {

  // A variable to track the remote RpcEnv addresses of all clients
  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  // 接收邮件，分发到server
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }
```


### client 发送信息


rpcEnv 发送消息
```scala

object HelloworldClient {

  def main(args: Array[String]): Unit = {
    syncCall()
   // syncCall()
  }

  def syncCall() = {
    val conf = new SparkConf()
    val clientMode= true
    val port =11112
    val config = RpcEnvConfig(conf, "test", "localhost", "localhost", port,
      new SecurityManager(conf), 0, clientMode)
    val rpcEnv: RpcEnv =  new NettyRpcEnvFactory().create(config)
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress("localhost", 11111), "hello-service")
    endPointRef.send([String](SayHi("spafka"))

  }
}

```

消息投递到outbox(出站邮箱)
```scala

  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    if (receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      require(receiver.address != null,
        "Cannot send message to client endpoint with no listen address.")
      val targetOutbox = {
        val outbox = outboxes.get(receiver.address)
         if (outbox == null) {
          val newOutbox = new Outbox(this, receiver.address)
          val oldOutbox = outboxes.putIfAbsent(receiver.address, newOutbox)
          if (oldOutbox == null) {
            newOutbox
          } else {
            oldOutbox
          }
        } else {
          outbox
        }
      }
      if (stopped.get) {
        // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
        outboxes.remove(receiver.address)
        targetOutbox.stop()
      } else {
       // logInfo(s"客户端发送数据 outBox =${targetOutbox}>> ${message}")
        targetOutbox.send(message)
      }
    }
  }

```


netty 连接服务端，构造client ，发送消息
```scala
 /**
   * Drain the message queue. If there is other draining thread, just exit. If the connection has
   * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to setup the
   * connection.
   */
  private def drainOutbox(): Unit = {
    var message: OutboxMessage = null
    synchronized {
      if (stopped) {
        return
      }
      if (connectFuture != null) {
        // We are connecting to the remote address, so just exit
        return
      }
      if (client == null) {
        // There is no connect task but client is null, so we need to launch the connect task.
        logInfo(s"客户端尝试连接 服务端")
        launchConnectTask()
        logInfo(s"客户端连接服务端完成")
        return
      }
      if (draining) {
        // There is some thread draining, so just exit
        return
      }
      message = messages.poll()
      if (message == null) {
        return
      }
      draining = true
    }
    while (true) {
      try {
        val _client = synchronized { client }
        if (_client != null) {

          logInfo(s"客户端连接服务端 完成 发送数据")
          message.sendWith(_client)
        } else {
          assert(stopped == true)
        }
      } catch {
        case NonFatal(e) =>
          handleNetworkFailure(e)
          return
      }
      synchronized {
        if (stopped) {
          return
        }
        message = messages.poll()
        if (message == null) {
          draining = false
          return
        }
      }
    }
  }
```


利用netty，client向server发送消息，rpchander
```scala

/**
   * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
   * with the server's response or upon any failure.
   *
   * @param message The message to send.
   * @param callback Callback to handle the RPC's reply.
   * @return The RPC's id.
   */
  public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
    long startTime = System.currentTimeMillis();
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", getRemoteAddress(channel));
    }

    long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
    handler.addRpcRequest(requestId, callback);

    channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)))
        .addListener(future -> {
          if (future.isSuccess()) {
            long timeTaken = System.currentTimeMillis() - startTime;
            if (logger.isTraceEnabled()) {
              logger.trace("Sending request {} to {} took {} ms", requestId,
                getRemoteAddress(channel), timeTaken);
            }
          } else {
            String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
              getRemoteAddress(channel), future.cause());
            logger.error(errorMsg, future.cause());
            handler.removeRpcRequest(requestId);
            channel.close();
            try {
              callback.onFailure(new IOException(errorMsg, future.cause()));
            } catch (Exception e) {
              logger.error("Uncaught exception in RPC response callback handler!", e);
            }
          }
        });

    return requestId;
    
   
  }

```


服务端接收到消息 ，针对不同的请求分发
```scala
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
  @Override
  public void handle(RequestMessage request) {
    if (request instanceof ChunkFetchRequest) {
      processFetchRequest((ChunkFetchRequest) request);
    } else if (request instanceof RpcRequest) {
      processRpcRequest((RpcRequest) request);
    } else if (request instanceof OneWayMessage) {
      processOneWayMessage((OneWayMessage) request);
    } else if (request instanceof StreamRequest) {
      processStreamRequest((StreamRequest) request);
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

}
```
处理rpc消息
```java
private void processRpcRequest(final RpcRequest req) {
    try {

      logger.info("nettyhandler processRpcRequest{} -> {}",req.getClass(),req.requestId);
      rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        }
      });
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
      respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
    } finally {
      req.body().release();
    }
  }
```

把接收到的RpcRequestMessage 转化为 内部的equestMessageR，反序列化 rpcRequestMessage中的content
```scala
  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }

  override def receive(
      client: TransportClient,
      message: ByteBuffer): Unit = {
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage = {
    val addr = client.getChannel().remoteAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)
    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    val requestMessage = RequestMessage(nettyEnv, client, message)
    if (requestMessage.senderAddress == null) {
      // Create a new message with the socket address of the client as the sender.
      new RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content)
    } else {
      // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
      // the listening address
      val remoteEnvAddress = requestMessage.senderAddress
      if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
      }
      requestMessage
    }
  }

```
messageLoop 处理完之后，context返回给客户端

```java


org/apache/spark/network/server/TransportRequestHandler.java:217
 rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
        }

        @Override
        public void onFailure(Throwable e) {
          respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        }
        
        
          /**
           * Responds to a single message with some Encodable object. If a failure occurs while sending,
           * it will be logged and the channel closed.
           */
          private ChannelFuture respond(Encodable result) {
            SocketAddress remoteAddress = channel.remoteAddress();
            return channel.writeAndFlush(result).addListener(future -> {
              if (future.isSuccess()) {
                logger.trace("Sent result {} to client {}", result, remoteAddress);
              } else {
                logger.error(String.format("Error sending result %s to %s; closing connection",
                  result, remoteAddress), future.cause());
                channel.close();
              }
            });
          }

```


最后引用neoremid 做的几张图

![server](https://raw.githubusercontent.com/spafka/spark/master/MDZ/images/server-bootstarp.jpg)
![client](https://raw.githubusercontent.com/spafka/spark/master/MDZ/images/client.jpg)
![serer-reponse](https://raw.githubusercontent.com/spafka/spark/master/MDZ/images/server-replay.jpg)



一个典型的例子
```scala


  test("send a message remotely") {
    @volatile var message: String = null
    // Set up a RpcEndpoint using env
    env.setupEndpoint("send-remotely", new RpcEndpoint {
      override val rpcEnv = env

      override def receive: PartialFunction[Any, Unit] = {
        case msg: String => message = msg
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0, clientMode = true)
    // Use anotherEnv to find out the RpcEndpointRef
    val rpcEndpointRef = anotherEnv.setupEndpointRef(env.address, "send-remotely")
    try {
      rpcEndpointRef.send("hello")
      eventually(timeout(5 seconds), interval(10 millis)) {
        assert("hello" === message)
      }
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }


```

serve 启动一个endpoint,调用endPoint的start，启动本地服务，client 连接server的port，从concurretntHashmap中按名称锁定endpoint，
建立verfiyier连接之后，再建立对应的endPoint连接 Outbox 建立client连接，发送消息
