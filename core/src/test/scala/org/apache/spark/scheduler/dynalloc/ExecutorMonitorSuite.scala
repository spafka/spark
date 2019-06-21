/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.dynalloc

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.scheduler._
import org.apache.spark.storage._
import org.apache.spark.util.ManualClock

class ExecutorMonitorSuite extends SparkFunSuite {

  private val idleTimeoutMs = TimeUnit.SECONDS.toMillis(60L)
  private val storageTimeoutMs = TimeUnit.SECONDS.toMillis(120L)

  private val conf = new SparkConf()
    .set(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT.key, "60s")
    .set(DYN_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT.key, "120s")

  private var monitor: ExecutorMonitor = _
  private var client: ExecutorAllocationClient = _
  private var clock: ManualClock = _

  // List of known executors. Allows easily mocking which executors are alive without
  // having to use mockito APIs directly in each test.
  private val knownExecs = mutable.HashSet[String]()

  override def beforeEach(): Unit = {
    super.beforeEach()
    knownExecs.clear()
    clock = new ManualClock()
    client = mock(classOf[ExecutorAllocationClient])
    when(client.isExecutorActive(any())).thenAnswer { invocation =>
      knownExecs.contains(invocation.getArguments()(0).asInstanceOf[String])
    }
    monitor = new ExecutorMonitor(conf, client, clock)
  }

  test("basic executor timeout") {
    knownExecs += "1"
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", null))
    assert(monitor.executorCount === 1)
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))
  }

  test("SPARK-4951, SPARK-26927: handle out of order task start events") {
    knownExecs ++= Set("1", "2")

    monitor.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("1", 1)))
    assert(monitor.executorCount === 1)

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", null))
    assert(monitor.executorCount === 1)

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "2", null))
    assert(monitor.executorCount === 2)

    monitor.onExecutorRemoved(SparkListenerExecutorRemoved(clock.getTimeMillis(), "2", null))
    assert(monitor.executorCount === 1)

    knownExecs -= "2"

    monitor.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("2", 2)))
    assert(monitor.executorCount === 1)
  }

  test("track tasks running on executor") {
    knownExecs += "1"

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", null))
    monitor.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("1", 1)))
    assert(!monitor.isExecutorIdle("1"))

    // Start/end a few tasks and make sure the executor does not go idle.
    (2 to 10).foreach { i =>
      monitor.onTaskStart(SparkListenerTaskStart(i, 1, taskInfo("1", 1)))
      assert(!monitor.isExecutorIdle("1"))

      monitor.onTaskEnd(SparkListenerTaskEnd(i, 1, "foo", Success, taskInfo("1", 1), null))
      assert(!monitor.isExecutorIdle("1"))
    }

    monitor.onTaskEnd(SparkListenerTaskEnd(1, 1, "foo", Success, taskInfo("1", 1), null))
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(clock.getTimeMillis()).isEmpty)
    assert(monitor.timedOutExecutors(clock.getTimeMillis() + idleTimeoutMs + 1) === Seq("1"))
  }

  test("use appropriate time out depending on whether blocks are stored") {
    knownExecs += "1"
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", null))
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 0, "1"))
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.NONE))
    assert(monitor.isExecutorIdle("1"))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))

    monitor.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("1", 1)))
    assert(!monitor.isExecutorIdle("1"))
    monitor.onBlockUpdated(rddUpdate(1, 0, "1"))
    assert(!monitor.isExecutorIdle("1"))
    monitor.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.NONE))
    assert(!monitor.isExecutorIdle("1"))
  }

  test("keeps track of stored blocks for each rdd and split") {
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", null))

    monitor.onBlockUpdated(rddUpdate(1, 0, "1"))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 1, "1"))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    monitor.onBlockUpdated(rddUpdate(2, 0, "1"))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 1, "1", level = StorageLevel.NONE))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    monitor.onUnpersistRDD(SparkListenerUnpersistRDD(1))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) === Seq("1"))

    // Make sure that if we get an unpersist event much later, which moves an executor from having
    // cached blocks to no longer having cached blocks, it will time out based on the time it
    // originally went idle.
    clock.setTime(idleDeadline)
    monitor.onUnpersistRDD(SparkListenerUnpersistRDD(2))
    assert(monitor.timedOutExecutors(clock.getTimeMillis()) === Seq("1"))
  }

  test("handle timeouts correctly with multiple executors") {
    knownExecs ++= Set("1", "2", "3")

    // start exec 1 at 0s (should idle time out at 60s)
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", null))
    assert(monitor.isExecutorIdle("1"))

    // start exec 2 at 30s, store a block (should idle time out at 150s)
    clock.setTime(TimeUnit.SECONDS.toMillis(30))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "2", null))
    monitor.onBlockUpdated(rddUpdate(1, 0, "2"))
    assert(monitor.isExecutorIdle("2"))
    assert(!monitor.timedOutExecutors(idleDeadline).contains("2"))

    // start exec 3 at 60s (should idle timeout at 120s, exec 1 should time out)
    clock.setTime(TimeUnit.SECONDS.toMillis(60))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "3", null))
    assert(monitor.timedOutExecutors(clock.getTimeMillis()) === Seq("1"))

    // store block on exec 3 (should now idle time out at 180s)
    monitor.onBlockUpdated(rddUpdate(1, 0, "3"))
    assert(monitor.isExecutorIdle("3"))
    assert(!monitor.timedOutExecutors(idleDeadline).contains("3"))

    // advance to 140s, remove block from exec 3 (time out immediately)
    clock.setTime(TimeUnit.SECONDS.toMillis(140))
    monitor.onBlockUpdated(rddUpdate(1, 0, "3", level = StorageLevel.NONE))
    assert(monitor.timedOutExecutors(clock.getTimeMillis()).toSet === Set("1", "3"))

    // advance to 150s, now exec 2 should time out
    clock.setTime(TimeUnit.SECONDS.toMillis(150))
    assert(monitor.timedOutExecutors(clock.getTimeMillis()).toSet === Set("1", "2", "3"))
  }

  test("SPARK-27677: don't track blocks stored on disk when using shuffle service") {
    // First make sure that blocks on disk are counted when no shuffle service is available.
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", null))
    monitor.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.DISK_ONLY))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) ===  Seq("1"))

    conf.set(SHUFFLE_SERVICE_ENABLED, true).set(SHUFFLE_SERVICE_FETCH_RDD_ENABLED, true)
    monitor = new ExecutorMonitor(conf, client, clock)

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", null))
    monitor.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.MEMORY_ONLY))
    monitor.onBlockUpdated(rddUpdate(1, 1, "1", level = StorageLevel.MEMORY_ONLY))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) ===  Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 0, "1", level = StorageLevel.DISK_ONLY))
    assert(monitor.timedOutExecutors(idleDeadline).isEmpty)
    assert(monitor.timedOutExecutors(storageDeadline) ===  Seq("1"))

    monitor.onBlockUpdated(rddUpdate(1, 1, "1", level = StorageLevel.DISK_ONLY))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))

    // Tag the block as being both in memory and on disk, which may happen after it was
    // evicted and then restored into memory. Since it's still on disk the executor should
    // still be eligible for removal.
    monitor.onBlockUpdated(rddUpdate(1, 1, "1", level = StorageLevel.MEMORY_AND_DISK))
    assert(monitor.timedOutExecutors(idleDeadline) === Seq("1"))
  }

  test("track executors pending for removal") {
    knownExecs ++= Set("1", "2", "3")

    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "1", null))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "2", null))
    monitor.onExecutorAdded(SparkListenerExecutorAdded(clock.getTimeMillis(), "3", null))
    clock.setTime(idleDeadline)
    assert(monitor.timedOutExecutors().toSet === Set("1", "2", "3"))
    assert(monitor.pendingRemovalCount === 0)

    // Notify that only a subset of executors was killed, to mimic the case where the scheduler
    // refuses to kill an executor that is busy for whatever reason the monitor hasn't detected yet.
    monitor.executorsKilled(Seq("1"))
    assert(monitor.timedOutExecutors().toSet === Set("2", "3"))
    assert(monitor.pendingRemovalCount === 1)

    // Check the timed out executors again so that we're sure they're still timed out when no
    // events happen. This ensures that the monitor doesn't lose track of them.
    assert(monitor.timedOutExecutors().toSet === Set("2", "3"))

    monitor.onTaskStart(SparkListenerTaskStart(1, 1, taskInfo("2", 1)))
    assert(monitor.timedOutExecutors().toSet === Set("3"))

    monitor.executorsKilled(Seq("3"))
    assert(monitor.pendingRemovalCount === 2)

    monitor.onTaskEnd(SparkListenerTaskEnd(1, 1, "foo", Success, taskInfo("2", 1), null))
    assert(monitor.timedOutExecutors().isEmpty)
    clock.advance(idleDeadline)
    assert(monitor.timedOutExecutors().toSet === Set("2"))
  }

  private def idleDeadline: Long = clock.getTimeMillis() + idleTimeoutMs + 1
  private def storageDeadline: Long = clock.getTimeMillis() + storageTimeoutMs + 1

  private def taskInfo(
      execId: String,
      id: Int,
      speculative: Boolean = false,
      duration: Long = -1L): TaskInfo = {
    val start = if (duration > 0) clock.getTimeMillis() - duration else clock.getTimeMillis()
    val task = new TaskInfo(id, id, 1, start, execId, "foo.example.com",
      TaskLocality.PROCESS_LOCAL, speculative)
    if (duration > 0) {
      task.markFinished(TaskState.FINISHED, math.max(1, clock.getTimeMillis()))
    }
    task
  }

  private def rddUpdate(
      rddId: Int,
      splitIndex: Int,
      execId: String,
      level: StorageLevel = StorageLevel.MEMORY_ONLY): SparkListenerBlockUpdated = {
    SparkListenerBlockUpdated(
      BlockUpdatedInfo(BlockManagerId(execId, "1.example.com", 42),
        RDDBlockId(rddId, splitIndex), level, 1L, 0L))
  }

}
