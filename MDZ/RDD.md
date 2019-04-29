## RDD

```java

/**
  * RDD 弹性分布式数据集，一个RDD包含一个数据集的分区，如HDFS 的一个block
  * 一个对分区数据集的操作，如map或shuffer
  * 以及对其他RDD的依赖，如join等
  * 可选的有一个分区器，用于shuffer
  * 一个可选的 preferred locations 用于本地数据的优先计算（HDFS）
  */
abstract class RDD[T: ClassTag](
                                 @transient private var _sc: SparkContext,
                                 @transient private var deps: Seq[Dependency[_]]
                               ) extends Serializable with Logging {

  // RDD[T] 不能是RDD[RDD]
  if (classOf[RDD[_]].isAssignableFrom(elementClassTag.runtimeClass)) {
    // This is a warning instead of an exception in order to avoid breaking user programs that
    // might have defined nested RDDs without running jobs with them.
    logWarning("Spark does not support nested RDDs (see SPARK-5063)")
  }
  }

```

核心函数，对分区的计算
```java

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to compute a given partition.
    */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]

```

例如最简单的 rdd.map ,就是 选择一个分区后，传一个闭包函数 对分区进行计算 

```scala
 def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }
  
```

会对每一个iter[T] f:T=>U 之后，对一个特定的partition[pid],进行分布式计算
```scala
_.map(_.splict(" "))
```
```java

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

 /**
  *  此处，compute 函数会调用abstract 父类RDD中的iterator函数
  *  
  *   final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
         if (storageLevel != StorageLevel.NONE) {
           getOrCompute(split, context)
         } else {
           computeOrReadCheckpoint(split, context)
         }
       }
       
       而iterator 会对所选分区[parition[i]]，调用子类的compute函数计算
       
       关于DAG的构造，task的提交下次分解,task构造完成，才会对Rdd进行【延迟】compute
  */
  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))

}

```

### 扩展

```java

 def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }
  
  
```

上面 构造一个新的RDD前，会调用一个函数

```text
    val cleanF = sc.clean(f)
```

这个函数在于执行分布式任务时，所有任务都是分发到各个executor去执行的  去除闭包对任务的影响至关重要

Spark对closure序列化的处理
快速了解下Scala对闭包的处理，下面是从Scala的REPL中执行的代码：

```text
scala> val n = 2
n: Int = 2

scala> val f = (x: Int) => x * n
f: Int => Int = <function1>

scala> Seq.range(0, 5).map(f)

res0: Seq[Int] = List(0, 2, 4, 6, 8)
```



f是采用Scala的=>语法糖定义的一个闭包，为了弄清楚Scala是如何处理闭包的，我们继续执行下面的代码：

```text
scala> f.getClass
res0: Class[_ <: Int => Int] = class $anonfun$1

scala> f.isInstanceOf[Function1[Int, Int]]
res1: Boolean = true

scala> f.isInstanceOf[Serializable]
res2: Boolean = true

```
可以看出f对应的类为$anonfun$1是Function1[Int, Int]的子类，而且实现了Serializable接口，这说明f是可以被序列化的。

Spark对于数据的处理基本都是基于闭包，下面是一个简单的Spark分布式处理数据的代码片段：

```java

val spark = SparkSession.builder().appName("demo").master("local").getOrCreate()
val sc = spark.sparkContext
val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
val sum = distData.map(x => x * 2).sum()
println(sum)  // 30.0

```
对于distData.map(x => x * 2)，map中传的一个匿名函数，也是一个非常简单的闭包，对distData中的每个元素*2，我们知道对于这种形式的闭包，Scala编译后是可以序列化的，所以我们的代码能正常执行也合情合理。将入我们将处理函数的闭包定义到一个类中，然后将代码改造为如下形式：

class Operation {
  val n = 2
  def multiply = (x: Int) => x * n
}
...
val sum = distData.map(new Operation().multiply).sum()
...
我们在去执行，会出现什么样的结果呢？实际执行会出现这样的异常：

```text
Exception in thread "main" org.apache.spark.SparkException: Task not serializable
    at org.apache.spark.util.ClosureCleaner$.ensureSerializable(ClosureCleaner.scala:298)
    ...
Caused by: java.io.NotSerializableException: Operation

```
Scala在构造闭包的时候会确定他所依赖的外部变量，并将它们的引用存到闭包对象中，这样能保证在不同的作用域中调用闭包不出现问题。

出现Task not serializable的异常，是由于我们的multiply函数依赖Operation类的变量n，虽然multiply是支持序列化的，但是Operation不支持序列化，这导致multiply函数在序列化的过程中出现了NotSerializable的异常，最终导致我们的Task序列化失败。为了确保multiply能被正常序列化，我们需要想办法去除对Operation的依赖，我们将代码做如下修改，在去执行就可以了：

```java

class Operation {
  def multiply = (x: Int) => x * 2
}
...
val sum = distData.map(new Operation().multiply).sum()
...
```
Spark对闭包序列化前，会通过工具类org.apache.spark.util.ClosureCleaner尝试clean掉闭包中无关的外部对象引用，ClosureCleaner对闭包的处理是在运行期间，相比Scala编译器，能更精准的去除闭包中无关的引用。这样做，一方面可以尽可能保证闭包可被序列化，另一方面可以减少闭包序列化后的大小，便于网络传输。
如果遇到Task not serializable的异常，就需要考虑下，闭包中是否或引用了无法序列化的对象，有的话，尝试去除依赖就可以了。