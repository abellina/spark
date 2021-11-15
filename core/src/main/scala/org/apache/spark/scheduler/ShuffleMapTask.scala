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

package org.apache.spark.scheduler

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.rdd.RDD

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 * @param isBarrier whether this task belongs to a barrier stage. Spark must launch all the tasks
 *                  at the same time for a barrier stage.
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    isBarrier: Boolean = false)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, localProperties,
    serializedTaskMetrics, jobId, appId, appAttemptId, isBarrier)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) = {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, new Properties, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.distinct
  }

  override def runTask(context: TaskContext): MapStatus = {
    runTask(context, None)
  }
  override def runTask(context: TaskContext, plugins: Option[PluginContainer]): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    plugins.foreach(_.onEventStarted("shuffle_map_task_run"))
    plugins.foreach(_.onEventStarted("shuffle_map_task_deserialize"))
    plugins.foreach(_.onEventStarted("thread_mx_bean"))
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    plugins.foreach(_.onEventStopped("thread_mx_bean"))
    plugins.foreach(_.onEventStarted("instantiate closure serializer"))
    val ser = SparkEnv.get.closureSerializer.newInstance()
    plugins.foreach(_.onEventStopped("instantiate closure serializer"))
    plugins.foreach(_.onEventStarted("broadcast value"))
    val bValue = taskBinary.value
    plugins.foreach(_.onEventStopped("broadcast value"))
    val rddAndDep = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(bValue), Thread.currentThread.getContextClassLoader, plugins)
    plugins.foreach(_.onEventStopped("shuffle_map_task_deserialize"))
    _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    val rdd = rddAndDep._1
    val dep = rddAndDep._2
    // While we use the old shuffle fetch protocol, we use partitionId as mapId in the
    // ShuffleBlockId construction.
    val mapId = if (SparkEnv.get.conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
      partitionId
    } else context.taskAttemptId()
    plugins.foreach(_.onEventStarted("shuffle_map_task_write"))
    val res = dep.shuffleWriterProcessor.write(rdd, dep, mapId, context, partition)
    plugins.foreach(_.onEventStopped("shuffle_map_task_write"))
    plugins.foreach(_.onEventStopped("shuffle_map_task_run"))
    res
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
