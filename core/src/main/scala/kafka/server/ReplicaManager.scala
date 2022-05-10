/**
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
package kafka.server

import kafka.ssy.datachannel.HAClusterConfig
import org.apache.kafka.common.config.manager.ClusterConfigManager

import java.io.File
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.Lock
import com.yammer.metrics.core.Meter
import kafka.api._
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.common.RecordValidationException
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.HostedPartition.Online
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpointFile, OffsetCheckpoints}
import kafka.server.{FetchMetadata => SFetchMetadata}
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.Metadata.LeaderAndEpoch
import org.apache.kafka.clients.producer.internals.ProducerMetadata
import org.apache.kafka.clients.{ApiVersions, ClientDnsLookup, ClientUtils, KafkaClient, NetworkClient}
import org.apache.kafka.clients._
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.{ClusterResourceListeners, Topic}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LeaderAndIsrResponseData
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.PartitionView.DefaultPartitionView
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.replica._
import org.apache.kafka.common.requests.DescribeLogDirsResponse.{LogDirInfo, ReplicaInfo}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{ElectionType, IsolationLevel, Node, TopicPartition}
import org.apache.kafka.common.{Cluster, ElectionType, IsolationLevel, Node, TopicPartition}
import org.apache.kafka.common.internals.{ClusterResourceListeners, Topic}

import java.io.File
import java.net.InetSocketAddress
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.{Lock, ReentrantLock}
import java.util.{Optional, Properties}
import scala.collection.JavaConverters._
import scala.collection.{Map, Seq, Set, mutable}
import scala.compat.java8.OptionConverters._
import kafka.utils.Implicits._

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

case class LogDeleteRecordsResult(requestedOffset: Long, lowWatermark: Long, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/*
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param hw high watermark of the local replica
 * @param readSize amount of data that was read from the log i.e. size of the fetch
 * @param isReadFromLogEnd true if the request read up to the log end offset snapshot
 *                         when the read was initiated, false otherwise
 * @param preferredReadReplica the preferred read replica to be used for future fetches
 * @param exception Exception if error encountered while reading from the log
 */
//对log文件读取的返回结果
//表示副本管理器从副本本地日志(即Leader日志)读取到的消息数据以及相关元数据信息，如高水位值、Log Start Offset 值等
//主要是Leader副本的HW值
case class LogReadResult(info: FetchDataInfo,                           //读取到的数据
                         highWatermark: Long,                           //本地(leader)的HW
                         leaderLogStartOffset: Long,                    //leader的LSO
                         leaderLogEndOffset: Long,                      //leader的LEO
                         followerLogStartOffset: Long,
                         fetchTimeMs: Long,
                         readSize: Int,                                 //FETCH的消息大小，表示从log获取的数据大小
                         lastStableOffset: Option[Long],
                         preferredReadReplica: Option[Int] = None,      //the preferred read replica to be used for future fetches
                         followerNeedsHwUpdate: Boolean = false,
                         exception: Option[Throwable] = None) {

  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  def withEmptyFetchInfo: LogReadResult =
    copy(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY))

  override def toString =
    s"Fetch Data: [$info], HW: [$highWatermark], leaderLogStartOffset: [$leaderLogStartOffset], leaderLogEndOffset: [$leaderLogEndOffset], " +
    s"followerLogStartOffset: [$followerLogStartOffset], fetchTimeMs: [$fetchTimeMs], readSize: [$readSize], lastStableOffset: [$lastStableOffset], error: [$error]"

}

case class FetchPartitionData(error: Errors = Errors.NONE,
                              highWatermark: Long,                                     //本地(leader)的HW
                              logStartOffset: Long,                                    //leader的LSO
                              records: Records,                                        //Fetch到的数据
                              lastStableOffset: Option[Long],
                              abortedTransactions: Option[List[AbortedTransaction]],
                              preferredReadReplica: Option[Int],
                              isReassignmentFetch: Boolean)


/**
 * Trait to represent the state of hosted partitions. We create a concrete (active) Partition
 * instance when the broker receives a LeaderAndIsr request from the controller indicating
 * that it should be either a leader or follower of a partition.
 */
sealed trait HostedPartition
object HostedPartition {
  /**
   * This broker does not have any state for this partition locally.
   */
  final object None extends HostedPartition

  /**
   * This broker hosts the partition and it is online.
   */
  final case class Online(partition: Partition) extends HostedPartition

  /**
   * This broker hosts the partition, but it is in an offline log directory.
   */
  final object Offline extends HostedPartition
}

object ReplicaManager {
  //recovery-point-offset-checkpoint：leader副本所在的broker中保存的检查点文件，记录各副本的HW
  val HighWatermarkFilename = "replication-offset-checkpoint"
  val IsrChangePropagationBlackOut = 5000L
  val IsrChangePropagationInterval = 60000L
}

class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     val zkClient: KafkaZkClient,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val isShuttingDown: AtomicBoolean,
                     quotaManagers: QuotaManagers,
                     val brokerTopicStats: BrokerTopicStats,
                     val metadataCache: MetadataCache,
                     logDirFailureChannel: LogDirFailureChannel,
                     val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
                     val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                     val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords],
                     val delayedElectLeaderPurgatory: DelayedOperationPurgatory[DelayedElectLeader],
                     threadNamePrefix: Option[String]) extends Logging with KafkaMetricsGroup {

  def this(config: KafkaConfig,
           metrics: Metrics,
           time: Time,
           zkClient: KafkaZkClient,
           scheduler: Scheduler,
           logManager: LogManager,
           isShuttingDown: AtomicBoolean,
           quotaManagers: QuotaManagers,
           brokerTopicStats: BrokerTopicStats,
           metadataCache: MetadataCache,
           logDirFailureChannel: LogDirFailureChannel,
           threadNamePrefix: Option[String] = None) {
    this(config, metrics, time, zkClient, scheduler, logManager, isShuttingDown,
      quotaManagers, brokerTopicStats, metadataCache, logDirFailureChannel,
      DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", brokerId = config.brokerId,
        purgeInterval = config.producerPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", brokerId = config.brokerId,
        purgeInterval = config.fetchPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedDeleteRecords](
        purgatoryName = "DeleteRecords", brokerId = config.brokerId,
        purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedElectLeader](
        purgatoryName = "ElectLeader", brokerId = config.brokerId),
      threadNamePrefix)
  }

  /* epoch of the controller that last changed the leader */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  private val localBrokerId = config.brokerId
  private val allPartitions = new Pool[TopicPartition, HostedPartition](
    valueFactory = Some(tp => HostedPartition.Online(Partition(tp, time, this)))
  )
  private val replicaStateChangeLock = new Object
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManagers.follower)
  // Didi-Kafka 灾备 1
  // 初始化 MirrorFetcherManager
  val mirrorFetcherManager = createMirrorFetcherManager(metrics, time, threadNamePrefix, quotaManagers.mirrorFollower)

  val replicaAlterLogDirsManager = createReplicaAlterLogDirsManager(quotaManagers.alterLogDirs, brokerTopicStats)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  @volatile var highWatermarkCheckpoints: Map[String, OffsetCheckpointFile] = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap

  this.logIdent = s"[ReplicaManager broker=$localBrokerId] "
  private val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)

  //当ISR集合发生变化时会将变更的副本记录到isrChangeSet中,后面ISR变更通知任务会使用到
  private val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  //在ISR变更任务中记录最新一次ISR变更时间,ISR变更通知任务会使用到
  private val lastIsrChangeMs = new AtomicLong(System.currentTimeMillis())
  //Isr变更广播任务最近一次写入ZooKeeper的时间
  private val lastIsrPropagationMs = new AtomicLong(System.currentTimeMillis())

  /*** Didi-Kafka 灾备 1 ↓ ***/
  //镜像Topic名 -> 镜像Topic 的映射
  private val mirrorTopics = mutable.HashMap[String, MirrorTopic]()

  // local topic -> remote topic
  /** 存储 本地Topic名 -> 镜像Topic 的映射，会被MirrorCoordinator周期性检测并创建或更新本地Topic以与MirrorTopic的partition数相同 */
  private val mirrorTopicsByLocal = mutable.HashMap[String, MirrorTopic]()
  /*** Didi-Kafka 灾备 1 ↑ ***/

  private var logDirFailureHandler: LogDirFailureHandler = null

  private class LogDirFailureHandler(name: String, haltBrokerOnDirFailure: Boolean) extends ShutdownableThread(name) {
    override def doWork(): Unit = {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      if (haltBrokerOnDirFailure) {
        fatal(s"Halting broker because dir $newOfflineLogDir is offline")
        Exit.halt(1)
      }
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  // Visible for testing
  private[server] val replicaSelectorOpt: Option[ReplicaSelector] = createReplicaSelector()

  newGauge("LeaderCount", () => leaderPartitionsIterator.size)
  // Visible for testing
  private[kafka] val partitionCount = newGauge("PartitionCount", () => allPartitions.size)
  newGauge("OfflineReplicaCount", () => offlinePartitionCount)
  newGauge("UnderReplicatedPartitions", () => underReplicatedPartitionCount)
  newGauge("UnderMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isUnderMinIsr))
  newGauge("AtMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isAtMinIsr))
  newGauge("ReassigningPartitions", () => reassigningPartitionsCount)

  def reassigningPartitionsCount: Int = leaderPartitionsIterator.count(_.isReassigning)

  val isrExpandRate: Meter = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS)
  val isrShrinkRate: Meter = newMeter("IsrShrinksPerSec", "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate: Meter = newMeter("FailedIsrUpdatesPerSec", "failedUpdates", TimeUnit.SECONDS)

  def underReplicatedPartitionCount: Int = leaderPartitionsIterator.count(_.isUnderReplicated)

  def startHighWatermarkCheckPointThread(): Unit = {
    if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks _, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  //记录有变化的副本
  def recordIsrChange(topicPartition: TopicPartition): Unit = {
    isrChangeSet synchronized {
      //当ISR集合发生变化时会将变更的副本记录到isrChangeSet中,后面ISR变更通知任务会使用到
      isrChangeSet += topicPartition
      //同时更新lastIsrChangeMs时间，后面ISR变更通知任务会使用到
      lastIsrChangeMs.set(System.currentTimeMillis())
    }
  }
  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  //ISR变动时，为了让其他的broker能收到ISR变动的通知，会往zk的/isr_change_notification注册相应数据
  def maybePropagateIsrChanges(): Unit = {
    val now = System.currentTimeMillis()
    isrChangeSet synchronized {
      /**
       *当检测到分区的ISR集合发生变化时， 还需要检查以下两个条件：
       * 1.上一次ISR集合发生变化距离现在已经超过5s。
       * 2.上一次写入ZooKeeper的时间距离现在已经超过60s。
       */
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + ReplicaManager.IsrChangePropagationBlackOut < now ||
          lastIsrPropagationMs.get() + ReplicaManager.IsrChangePropagationInterval < now)) {
        //在ZooKeeper的/isr_change_notification路径下创建一个以isr_change_开头的持久顺序节点
        zkClient.propagateIsrChanges(isrChangeSet)
        isrChangeSet.clear()
        //更新最近一次写入ZooKeeper的时间
        lastIsrPropagationMs.set(now)
      }
    }
  }

  // When ReplicaAlterDirThread finishes replacing a current replica with a future replica, it will
  // remove the partition from the partition state map. But it will not close itself even if the
  // partition state map is empty. Thus we need to call shutdownIdleReplicaAlterDirThread() periodically
  // to shutdown idle ReplicaAlterDirThread
  def shutdownIdleReplicaAlterLogDirsThread(): Unit = {
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
  }

  def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

  def hasDelayedElectionOperations: Boolean = delayedElectLeaderPurgatory.numDelayed != 0

  def tryCompleteElection(key: DelayedOperationKey): Unit = {
    val completed = delayedElectLeaderPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d ElectLeader.".format(key.keyLabel, completed))
  }

  def startup(): Unit = {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    //ISR 定时检查任务,周期性的检测是否有副本掉队,进而收缩isr列表,
    // 每config.replicaLagTimeMaxMs / 2执行一次.replicaLagTimeMaxMs的默认值是10000，也就是默认5s执行一次该任务
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    //ISR 定时广播任务,将ISR列表的收缩广播给其他节点。其执行周期默认为 2500L ,即2.5秒
    scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _, period = 2500L, unit = TimeUnit.MILLISECONDS)
    scheduler.schedule("shutdown-idle-replica-alter-log-dirs-thread", shutdownIdleReplicaAlterLogDirsThread _, period = 10000L, unit = TimeUnit.MILLISECONDS)

    // If inter-broker protocol (IBP) < 1.0, the controller will send LeaderAndIsrRequest V0 which does not include isNew field.
    // In this case, the broker receiving the request cannot determine whether it is safe to create a partition if a log directory has failed.
    // Thus, we choose to halt the broker on any log diretory failure if IBP < 1.0
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
    /*** Didi-Kafka 灾备 ↓ ***/
    //以10秒为周期循环更新镜像Topic元数据
    mirrorMetadataThread = new RemoteMetadataThread("mirror-remote-metadata-thread", 10 * 1000, TimeUnit.MILLISECONDS)
    mirrorMetadataThread.start()
    /*** Didi-Kafka 灾备 ↑ ***/
  }

  private def maybeRemoveTopicMetrics(topic: String): Unit = {
    val topicHasOnlinePartition = allPartitions.values.exists {
      case HostedPartition.Online(partition) => topic == partition.topic
      case HostedPartition.None | HostedPartition.Offline => false
    }
    if (!topicHasOnlinePartition)
      brokerTopicStats.removeMetrics(topic)
  }

  def stopReplica(topicPartition: TopicPartition, deletePartition: Boolean)  = {
    stateChangeLogger.trace(s"Handling stop replica (delete=$deletePartition) for partition $topicPartition")

    if (deletePartition) {
      getPartition(topicPartition) match {
        case HostedPartition.Offline =>
          throw new KafkaStorageException(s"Partition $topicPartition is on an offline disk")

        case hostedPartition @ HostedPartition.Online(removedPartition) =>
          if (allPartitions.remove(topicPartition, hostedPartition)) {
            maybeRemoveTopicMetrics(topicPartition.topic)
            // this will delete the local log. This call may throw exception if the log is on offline directory
            removedPartition.delete()
          }

        case HostedPartition.None =>
          stateChangeLogger.trace(s"Ignoring stop replica (delete=$deletePartition) for partition " +
            s"$topicPartition as replica doesn't exist on broker")
      }

      // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
      // This could happen when topic is being deleted while broker is down and recovers.
      if (logManager.getLog(topicPartition).isDefined)
        logManager.asyncDelete(topicPartition)
      if (logManager.getLog(topicPartition, isFuture = true).isDefined)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }

    // If we were the leader, we may have some operations still waiting for completion.
    // We force completion to prevent them from timing out.
    completeDelayedFetchOrProduceRequests(topicPartition)

    stateChangeLogger.trace(s"Finished handling stop replica (delete=$deletePartition) for partition $topicPartition")
  }

  private def completeDelayedFetchOrProduceRequests(topicPartition: TopicPartition): Unit = {
    val topicPartitionOperationKey = TopicPartitionOperationKey(topicPartition)
    delayedProducePurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
  }

  def stopReplicas(stopReplicaRequest: StopReplicaRequest): (mutable.Map[TopicPartition, Errors], Errors) = {
    replicaStateChangeLock synchronized {
      val responseMap = new collection.mutable.HashMap[TopicPartition, Errors]
      if (stopReplicaRequest.controllerEpoch() < controllerEpoch) {
        stateChangeLogger.warn("Received stop replica request from an old controller epoch " +
          s"${stopReplicaRequest.controllerEpoch}. Latest known controller epoch is $controllerEpoch")
        (responseMap, Errors.STALE_CONTROLLER_EPOCH)
      } else {
        val partitions = stopReplicaRequest.partitions.asScala.toSet
        controllerEpoch = stopReplicaRequest.controllerEpoch
        // First stop fetchers for all partitions, then stop the corresponding replicas
        // Didi-Kafka 灾备
        //为添加了镜像TopicPartition的分区执行 removeMirrors(remoteTopic, localPartitionId)
        removeMirrors(partitions.flatMap(p => mirrorTopicsByLocal.get(p.topic)
          .map(mt => new TopicPartition(mt.remoteTopic, p.partition()))), "local state change to stop replica")
        replicaFetcherManager.removeFetcherForPartitions(partitions)
        replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)
        for (topicPartition <- partitions){
          try {
            stopReplica(topicPartition, stopReplicaRequest.deletePartitions)
            responseMap.put(topicPartition, Errors.NONE)
          } catch {
            case e: KafkaStorageException =>
              stateChangeLogger.error(s"Ignoring stop replica (delete=${stopReplicaRequest.deletePartitions}) for " +
                s"partition $topicPartition due to storage exception", e)
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
          }
        }
        (responseMap, Errors.NONE)
      }
    }
  }

  def getPartition(topicPartition: TopicPartition): HostedPartition = {
    Option(allPartitions.get(topicPartition)).getOrElse(HostedPartition.None)
  }

  def isAddingReplica(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    getPartition(topicPartition) match {
      case Online(partition) => partition.isAddingReplica(replicaId)
      case _ => false
    }
  }

  // Visible for testing
  def createPartition(topicPartition: TopicPartition): Partition = {
    val partition = Partition(topicPartition, time, this)
    allPartitions.put(topicPartition, HostedPartition.Online(partition))
    partition
  }

  def nonOfflinePartition(topicPartition: TopicPartition): Option[Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) => Some(partition)
      case HostedPartition.None | HostedPartition.Offline => None
    }
  }

  // An iterator over all non offline partitions. This is a weakly consistent iterator; a partition made offline after
  // the iterator has been constructed could still be returned by this iterator.
  //获取所有inline的partition
  private def nonOfflinePartitionsIterator: Iterator[Partition] = {
    allPartitions.values.iterator.flatMap {
      case HostedPartition.Online(partition) => Some(partition)
      case HostedPartition.None | HostedPartition.Offline => None
    }
  }

  private def offlinePartitionCount: Int = {
    allPartitions.values.iterator.count(_ == HostedPartition.Offline)
  }

  //获取topicPartition的Partition对象
  def getPartitionOrException(topicPartition: TopicPartition, expectLeader: Boolean): Partition = {
    getPartitionOrError(topicPartition, expectLeader) match {
      case Left(Errors.KAFKA_STORAGE_ERROR) =>
        throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory")

      case Left(error) =>
        throw error.exception(s"Error while fetching partition state for $topicPartition")

      case Right(partition) => partition
    }
  }

  def getPartitionOrError(topicPartition: TopicPartition, expectLeader: Boolean): Either[Errors, Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) =>
        Right(partition)

      case HostedPartition.Offline =>
        Left(Errors.KAFKA_STORAGE_ERROR)

      case HostedPartition.None if metadataCache.contains(topicPartition) =>
        if (expectLeader) {
          // The topic exists, but this broker is no longer a replica of it, so we return NOT_LEADER which
          // forces clients to refresh metadata to find the new location. This can happen, for example,
          // during a partition reassignment if a produce request from the client is sent to a broker after
          // the local replica has been deleted.
          Left(Errors.NOT_LEADER_FOR_PARTITION)
        } else {
          Left(Errors.REPLICA_NOT_AVAILABLE)
        }

      case HostedPartition.None =>
        Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    }
  }

  def localLogOrException(topicPartition: TopicPartition): Log = {
    getPartitionOrException(topicPartition, expectLeader = false).localLogOrException
  }

  def futureLocalLogOrException(topicPartition: TopicPartition): Log = {
    getPartitionOrException(topicPartition, expectLeader = false).futureLocalLogOrException
  }

  def futureLogExists(topicPartition: TopicPartition): Boolean = {
    getPartitionOrException(topicPartition, expectLeader = false).futureLog.isDefined
  }

  def localLog(topicPartition: TopicPartition): Option[Log] = {
    nonOfflinePartition(topicPartition).flatMap(_.log)
  }

  def getLogDir(topicPartition: TopicPartition): Option[String] = {
    localLog(topicPartition).map(_.dir.getParent)
  }

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied;
   * if the callback function itself is already synchronized on some object then pass this object to avoid deadlock.
   */
  //实现副本写入，即向副本底层日志写入消息
  //需要副本写入的场景有 4 个:
  //1. 生产者向 Leader 副本写入消息
  //2. Follower 副本拉取消息后写入副本
  //3. 消费者组写入组信息
  //4. 事务管理器写入事务信息（包括事务标记、事务元数据等）
  def appendRecords(timeout: Long,                                                                                // 请求处理超时时间
                    requiredAcks: Short,                                                                          // 请求acks设置(-1，0，1)
                    internalTopicsAllowed: Boolean,                                                               // 是否允许写入内部主题
                    origin: AppendOrigin,                                                                         // 写入方来源：副本、coordinator、客户端、Mirror(Kafka灾备)
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],                                      // 待写入消息
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit,                             // 回调函数
                    delayedProduceLock: Option[Lock] = None,                                                      // 专门用来保护消费者组操作线程安全的锁对象，在其他场景中用不到
                    recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => ()   // 消息格式转换操作的回调统计逻辑，主要用于统计消息格式转换操作过程中的一些数据指标
                   ): Unit = {
    // requiredAcks合法取值是-1，0，1，否则视为非法
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = time.milliseconds
      // 调用appendToLocalLog方法写入消息集合到本地日志
      val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,
        origin, entriesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  // 设置下一条待写入消息的位移值
                  result.info.lastOffset + 1, // required offset
                  // 构建PartitionResponse封装写入结果
                  new PartitionResponse(result.error, result.info.firstOffset.getOrElse(-1), result.info.logAppendTime,
                    result.info.logStartOffset, result.info.recordErrors.asJava, result.info.errorMessage)) // response status
      }
      // 尝试更新消息格式转换的指标数据
      recordConversionStatsCallback(localProduceResults.map { case (k, v) => k -> v.info.recordConversionStats })
      // 需要等待其他副本完成写入
      if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        // create delayed produce operation
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        // 创建DelayedProduce延时请求对象
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
        val producerRequestKeys = entriesPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable.
        // 再一次尝试完成该延时请求
        // 如果暂时无法完成，则将对象放入到相应的Purgatory中等待后续处理
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // we can respond immediately
        // 无需等待其他副本写入完成，可以立即发送Response
        val produceResponseStatus = produceStatus.map { case (k, status) => k -> status.responseStatus }
        // 调用回调逻辑然后返回即可
        responseCallback(produceResponseStatus)
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      // 如果requiredAcks值不合法
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UnknownLogAppendInfo.firstOffset.getOrElse(-1), RecordBatch.NO_TIMESTAMP, LogAppendInfo.UnknownLogAppendInfo.logStartOffset)
      }
      // 构造INVALID_REQUIRED_ACKS异常并封装进回调函数调用中
      responseCallback(responseStatus)
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long]): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // reject delete records operation on internal topics
      if (Topic.isInternal(topicPartition.topic)) {
        (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = getPartitionOrException(topicPartition, expectLeader = true)
          val logDeleteResult = partition.deleteRecordsOnLeader(requestedOffset)
          (topicPartition, logDeleteResult)
        } catch {
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: OffsetOutOfRangeException |
                   _: PolicyViolationException |
                   _: KafkaStorageException) =>
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
          case t: Throwable =>
            error("Error processing delete records operation on partition %s".format(topicPartition), t)
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
        }
      }
    }
  }

  // If there exists a topic partition that meets the following requirement,
  // we need to put a delayed DeleteRecordsRequest and wait for the delete records operation to complete
  //
  // 1. the delete records operation on this partition is successful
  // 2. low watermark of this partition is smaller than the specified offset
  private def delayedDeleteRecordsRequired(localDeleteRecordsResults: Map[TopicPartition, LogDeleteRecordsResult]): Boolean = {
    localDeleteRecordsResults.exists{ case (_, deleteRecordsResult) =>
      deleteRecordsResult.exception.isEmpty && deleteRecordsResult.lowWatermark < deleteRecordsResult.requestedOffset
    }
  }

  /**
   * For each pair of partition and log directory specified in the map, if the partition has already been created on
   * this broker, move its log files to the specified log directory. Otherwise, record the pair in the memory so that
   * the partition will be created in the specified log directory when broker receives LeaderAndIsrRequest for the partition later.
   */
  def alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors] = {
    replicaStateChangeLock synchronized {
      partitionDirs.map { case (topicPartition, destinationDir) =>
        try {
          /* If the topic name is exceptionally long, we can't support altering the log directory.
           * See KAFKA-4893 for details.
           * TODO: fix this by implementing topic IDs. */
          if (Log.logFutureDirName(topicPartition).size > 255)
            throw new InvalidTopicException("The topic name is too long.")
          if (!logManager.isLogDirOnline(destinationDir))
            throw new KafkaStorageException(s"Log directory $destinationDir is offline")

          getPartition(topicPartition) match {
            case HostedPartition.Online(partition) =>
              // Stop current replica movement if the destinationDir is different from the existing destination log directory
              if (partition.futureReplicaDirChanged(destinationDir)) {
                replicaAlterLogDirsManager.removeFetcherForPartitions(Set(topicPartition))
                partition.removeFutureLocalReplica()
              }
            case HostedPartition.Offline =>
              throw new KafkaStorageException(s"Partition $topicPartition is offline")

            case HostedPartition.None => // Do nothing
          }

          // If the log for this partition has not been created yet:
          // 1) Record the destination log directory in the memory so that the partition will be created in this log directory
          //    when broker receives LeaderAndIsrRequest for this partition later.
          // 2) Respond with ReplicaNotAvailableException for this partition in the AlterReplicaLogDirsResponse
          logManager.maybeUpdatePreferredLogDir(topicPartition, destinationDir)

          // throw ReplicaNotAvailableException if replica does not exist for the given partition
          val partition = getPartitionOrException(topicPartition, expectLeader = false)
          partition.localLogOrException

          // If the destinationLDir is different from the current log directory of the replica:
          // - If there is no offline log directory, create the future log in the destinationDir (if it does not exist) and
          //   start ReplicaAlterDirThread to move data of this partition from the current log to the future log
          // - Otherwise, return KafkaStorageException. We do not create the future log while there is offline log directory
          //   so that we can avoid creating future log for the same partition in multiple log directories.
          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          if (partition.maybeCreateFutureReplica(destinationDir, highWatermarkCheckpoints)) {
            val futureLog = futureLocalLogOrException(topicPartition)
            logManager.abortAndPauseCleaning(topicPartition)

            val initialFetchState = InitialFetchState(BrokerEndPoint(config.brokerId, "localhost", -1),
              partition.getLeaderEpoch, futureLog.highWatermark)
            replicaAlterLogDirsManager.addFetcherForPartitions(Map(topicPartition -> initialFetchState))
          }

          (topicPartition, Errors.NONE)
        } catch {
          case e@(_: InvalidTopicException |
                  _: LogDirNotFoundException |
                  _: ReplicaNotAvailableException |
                  _: KafkaStorageException) =>
            (topicPartition, Errors.forException(e))
          case t: Throwable =>
            error("Error while changing replica dir for partition %s".format(topicPartition), t)
            (topicPartition, Errors.forException(t))
        }
      }
    }
  }

  /*
   * Get the LogDirInfo for the specified list of partitions.
   *
   * Each LogDirInfo specifies the following information for a given log directory:
   * 1) Error of the log directory, e.g. whether the log is online or offline
   * 2) size and lag of current and future logs for each partition in the given log directory. Only logs of the queried partitions
   *    are included. There may be future logs (which will replace the current logs of the partition in the future) on the broker after KIP-113 is implemented.
   */
  def describeLogDirs(partitions: Set[TopicPartition]): Map[String, LogDirInfo] = {
    val logsByDir = logManager.allLogs.groupBy(log => log.dir.getParent)

    config.logDirs.toSet.map { logDir: String =>
      val absolutePath = new File(logDir).getAbsolutePath
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        logsByDir.get(absolutePath) match {
          case Some(logs) =>
            val replicaInfos = logs.filter { log =>
              partitions.contains(log.topicPartition)
            }.map { log =>
              log.topicPartition -> new ReplicaInfo(log.size, getLogEndOffsetLag(log.topicPartition, log.logEndOffset, log.isFuture), log.isFuture)
            }.toMap

            (absolutePath, new LogDirInfo(Errors.NONE, replicaInfos.asJava))
          case None =>
            (absolutePath, new LogDirInfo(Errors.NONE, Map.empty[TopicPartition, ReplicaInfo].asJava))
        }

      } catch {
        case _: KafkaStorageException =>
          (absolutePath, new LogDirInfo(Errors.KAFKA_STORAGE_ERROR, Map.empty[TopicPartition, ReplicaInfo].asJava))
        case t: Throwable =>
          error(s"Error while describing replica in dir $absolutePath", t)
          (absolutePath, new LogDirInfo(Errors.forException(t), Map.empty[TopicPartition, ReplicaInfo].asJava))
      }
    }.toMap
  }

  def getLogEndOffsetLag(topicPartition: TopicPartition, logEndOffset: Long, isFuture: Boolean): Long = {
    localLog(topicPartition) match {
      case Some(log) =>
        if (isFuture)
          log.logEndOffset - logEndOffset
        else
          math.max(log.highWatermark - logEndOffset, 0)
      case None =>
        // return -1L to indicate that the LEO lag is not available if the replica is not created or is offline
        DescribeLogDirsResponse.INVALID_OFFSET_LAG
    }
  }

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsResponse.PartitionResponse] => Unit): Unit = {
    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        DeleteRecordsPartitionStatus(
          result.requestedOffset, // requested offset
          new DeleteRecordsResponse.PartitionResponse(result.lowWatermark, result.error)) // response status
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      // create delayed delete records operation
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed delete records operation
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed delete records operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys)
    } else {
      // we can respond immediately
      val deleteRecordsResponseStatus = deleteRecordsStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(deleteRecordsResponseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  //TODO-ssy 用于判断消息集合被写入到日志之后，是否需要等待其他副本也写入成功:
  private def delayedProduceRequestRequired(requiredAcks: Short,
                                            entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                            localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    // 1. requiredAcks 必须等于 -1
    requiredAcks == -1 &&
    // 2. 依然有数据尚未写完
    entriesPerPartition.nonEmpty &&
    // 至少有一个分区的消息已经成功地被写入到本地日志。
    // 如果所有分区的数据写入都不成功，就表明可能出现了很严重的错误，此时，比较明智的做法是不再等待，而是直接返回错误给发送方。
    // 相反地，如果有部分分区成功写入，而部分分区写入失败了，就表明可能是由偶发的瞬时错误导致的
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  //requiredAcks合法取值是-1，0，1，否则视为非法
  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  //写入消息集合到本地日志，实现消息写入
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               origin: AppendOrigin,
                               entriesPerPartition: Map[TopicPartition, MemoryRecords],   //各对应分区需要追加的消息数据
                               requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {

    def processFailedRecord(topicPartition: TopicPartition, t: Throwable) = {
      val logStartOffset = getPartition(topicPartition) match {
        case HostedPartition.Online(partition) => partition.logStartOffset
        case HostedPartition.None | HostedPartition.Offline => -1L
      }
      brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      error(s"Error processing append operation on partition $topicPartition", t)

      logStartOffset
    }

    trace(s"Append [$entriesPerPartition] to local log")
    // 遍历处理每个 topic 分区及其待追加的消息数据
    entriesPerPartition.map { case (topicPartition, records) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      // 如果要写入的主题是内部主题，而internalTopicsAllowed=false，则返回错误
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
      } else {
        try {
          // 获取分区对象
          val partition = getPartitionOrException(topicPartition, expectLeader = true)
          // 向该分区对象的leader副本写入消息集合
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks)
          val numAppendedMessages = info.numMessages

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          trace(s"${records.sizeInBytes} written to log $topicPartition beginning at offset " +
            s"${info.firstOffset.getOrElse(-1)} and ending at offset ${info.lastOffset}")
          // 返回每个分区写入的消息结果
          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          // 找不到 topic 分区对应的 Partition 对象
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderForPartitionException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case rve: RecordValidationException =>
            val logStartOffset = processFailedRecord(topicPartition, rve.invalidException)
            val recordErrors = rve.recordErrors
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(
              logStartOffset, recordErrors, rve.invalidException.getMessage), Some(rve.invalidException)))
          case t: Throwable =>
            val logStartOffset = processFailedRecord(topicPartition, t)
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset), Some(t)))
        }
      }
    }
  }

  def fetchOffsetForTimestamp(topicPartition: TopicPartition,
                              timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = {
    val partition = getPartitionOrException(topicPartition, expectLeader = fetchOnlyFromLeader)
    partition.fetchOffsetForTimestamp(timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader)
  }

  def legacyFetchOffsetsForTimestamp(topicPartition: TopicPartition,
                                     timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = {
    val partition = getPartitionOrException(topicPartition, expectLeader = fetchOnlyFromLeader)
    partition.legacyFetchOffsetsForTimestamp(timestamp, maxNumOffsets, isFromConsumer, fetchOnlyFromLeader)
  }

  /**
   * Fetch messages from a replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied.
   * Consumers may fetch from any replica, but followers can only fetch from the leader.
   */
  //从本地文件读取消息数据
  // 1. 首先调用 readFromLog() 函数进行实际的数据读取操作
  // 2. 根据相关配置决定是否立即回调函数
  //    KafkaApis.handleFetchRequest().processResponseCallback()
  //    将数据返回给请求方
  // 3. 如果不能立即返回，则生成一个延迟操作，将其投入到延迟队列等待触发
  def fetchMessages(timeout: Long,                                                          // 请求处理超时时间。
                    replicaId: Int,                                                         // 副本 ID。对于消费者而言，该参数值是 -1；对于 Follower 副本而言，该值就是 Follower 副本所在的 Broker ID
                    fetchMinBytes: Int,                                                     // 够获取的最小字节数
                    fetchMaxBytes: Int,                                                     // 够获取的最大字节数
                    hardMaxBytesLimit: Boolean,                                             // 能否超过最大字节数
                    fetchInfos: Seq[(TopicPartition, PartitionData)],                       // 规定了读取分区的信息，比如要读取哪些分区、从这些分区的哪个位移值开始读、最多可以读多少字节等等
                    quota: ReplicaQuota,                                                    // 这是一个配额控制类，主要是为了判断是否需要在读取的过程中做限速控制
                    responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit,    // 回调逻辑函数。当请求被处理完成后，调用该方法执行收尾逻辑
                    isolationLevel: IsolationLevel,
                    clientMetadata: Option[ClientMetadata]): Unit = {
    // 判断该读取请求是否来自于Follower副本
    val isFromFollower = Request.isValidBrokerId(replicaId)
    // 判断该读取请求是否来自于Consumer
    val isFromConsumer = !(isFromFollower || replicaId == Request.FutureLocalReplicaId)
    // 根据请求发送方判断可读取范围:
    val fetchIsolation = if (!isFromConsumer)
      // 如果请求来自于Follower副本，那么可以读到LEO值
      FetchLogEnd
    else if (isolationLevel == IsolationLevel.READ_COMMITTED)
      //如果请求来自于配置了READ_COMMITTED的消费者，那么可以读到Log Stable Offset值
      FetchTxnCommitted
    else
      //如果请求来自于普通消费者，那么可以读到高水位值
      FetchHighWatermark

    // Restrict fetching to leader if request is from follower or from a client with older version (no ClientMetadata)
    val fetchOnlyFromLeader = isFromFollower || (isFromConsumer && clientMetadata.isEmpty)
    //用于从Leader副本的本地磁盘中的日志文件读取数据，进行实际的数据读取操作
    def readFromLog(): Seq[(TopicPartition, LogReadResult)] = {
      //1. 这个方法的核心是调用 readFromLocalLog() 方法
      //    从本地的磁盘里面去读取 Leader 副本的本地日志
      val result = readFromLocalLog(
        replicaId = replicaId,
        fetchOnlyFromLeader = fetchOnlyFromLeader,
        fetchIsolation = fetchIsolation,
        fetchMaxBytes = fetchMaxBytes,
        hardMaxBytesLimit = hardMaxBytesLimit,
        readPartitionInfo = fetchInfos,
        quota = quota,
        clientMetadata = clientMetadata)
      // 2. 如果是来自分区副本 Follower 的 Fetch 请求，
      // 调用 updateFollowerFetchState() 更新Leader副本本地保存的：
      //                1. Leader副本的HW、
      //                2. 远程副本的 LEO
      //此外，尝试调整ISR列表等
      // 这部分处理主要和 Kafka 分区主从副本的数据同步机制有关
      if (isFromFollower) updateFollowerFetchState(replicaId, result)
      else result
    }

    //1. 获取读取结果
    val logReadResults = readFromLog()

    // check if this fetch request can be satisfied right away
    var bytesReadable: Long = 0
    var errorReadingData = false
    val logReadResultMap = new mutable.HashMap[TopicPartition, LogReadResult]
    var anyPartitionsNeedHwUpdate = false
    //最后会将多个分区的读取结果(包含Leader副本 HW)放到集合中，然后在合适的时机返回给Follower副本所在的节点( 对应同步数据第七步 )
    logReadResults.foreach { case (topicPartition, logReadResult) =>
      //如果读取发生错误
      if (logReadResult.error != Errors.NONE)
        errorReadingData = true
      // 统计总共可读取的字节数
      bytesReadable = bytesReadable + logReadResult.info.records.sizeInBytes
      //将读取结果放入集合
      logReadResultMap.put(topicPartition, logReadResult)
      if (isFromFollower && logReadResult.followerNeedsHwUpdate) {
        anyPartitionsNeedHwUpdate = true
      }
    }

    //2. 根据相关配置决定是否立即回调函数将数据返回给请求方
    //是KafkaApis.handleFetchRequest() 的回调函数 processResponseCallback()
    //上面所说的合适的时机，分为 立即返回 和 延时返回，
    // 当满足下面5个条件之一时，便立即返回，否则会进行延时处理：
    //    1. 请求携带的超时参数小于 0，也就是说请求方不愿意等待
    //    2. 请求订阅的 topic 为空
    //    3. 服务端读到了足够的数据，可以返回
    //    4. 读取数据时发生了异常
    //    5. 请求处理期间 Kafka 集群版本发生了变更
    // respond immediately if 1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    //                        5) any of the requested partitions need HW update
    if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData || anyPartitionsNeedHwUpdate) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
          result.lastStableOffset, result.info.abortedTransactions, result.preferredReadReplica, isFromFollower && isAddingReplica(tp, replicaId))
      }
      //调用回调函数 将数据返回给请求方
      responseCallback(fetchPartitionData)
    } else {
      //3. 如果不能立即返回，则生成一个延迟操作，将其投入到延迟队列等待触发
      // construct the fetch results from the read results
      val fetchPartitionStatus = new mutable.ArrayBuffer[(TopicPartition, FetchPartitionStatus)]
      fetchInfos.foreach { case (topicPartition, partitionData) =>
        logReadResultMap.get(topicPartition).foreach(logReadResult => {
          val logOffsetMetadata = logReadResult.info.fetchOffsetMetadata
          fetchPartitionStatus += (topicPartition -> FetchPartitionStatus(logOffsetMetadata, partitionData))
        })
      }
      val fetchMetadata: SFetchMetadata = SFetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit,
        fetchOnlyFromLeader, fetchIsolation, isFromFollower, replicaId, fetchPartitionStatus)
      // 构建DelayedFetch延时请求对象
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, clientMetadata,
        responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => TopicPartitionOperationKey(tp) }

      // try to complete the request immediately, otherwise put it into the purgatory;
      // this is because while the delayed fetch operation is being created, new requests
      // may arrive and hence make this operation completable.
      // 再一次尝试完成请求，如果依然不能完成，则交由Purgatory等待后续处理
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  //用于从Leader副本的日志文件读取数据，进行实际的数据读取操作
  //方法的处理比较简明:
  // 重点在于遍历 topic 列表，
  // 调用 replicaManager.fetchMessages().readFromLocalLog().read() 方法
  // 读取每个 topic 下分区内存储的消息数据，
  // 可以看到这个方法的关键处理是调用 Partition.readRecords() 方法执行读取操作
  def readFromLocalLog(replicaId: Int,                                            // 请求的 follower 副本 ID
                       fetchOnlyFromLeader: Boolean,                              // 是否只读 leader 副本的消息，一般 debug 模式下可以读 follower 副本的数据
                       fetchIsolation: FetchIsolation,
                       fetchMaxBytes: Int,                                        // 最大 fetch 字节数
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)],   // 记录了每个需要Fetch消息的Topic副本的信息，包括从每个分区读取的起始 offset(Follower LEO) 、Fetch最大字节数、follower保存的当前leader的epoch

                       quota: ReplicaQuota,
                       clientMetadata: Option[ClientMetadata]): Seq[(TopicPartition, LogReadResult)] = {

    // 读取每个 topic 下分区内存储的消息数据
    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      //读取的起始偏移量,即Follower副本的 LEO
      val offset = fetchInfo.fetchOffset
      //读取的最大字节数
      val partitionFetchSize = fetchInfo.maxBytes
      //follower 副本的LSO
      val followerLogStartOffset = fetchInfo.logStartOffset

      brokerTopicStats.topicStats(tp.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      //  计算读取消息的 offset 上界，如果是来自消费者的请求，则上界为 HW，如果是来自 follower 的请求，则上界为 LEO
      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
      try {
        trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
          s"remaining response limit $limitBytes" +
          (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        //获取topicPartition的Partition对象
        val partition = getPartitionOrException(tp, expectLeader = fetchOnlyFromLeader)
        val fetchTimeMs = time.milliseconds

        // If we are the leader, determine the preferred read-replica
        val preferredReadReplica = clientMetadata.flatMap(
          metadata => findPreferredReadReplica(tp, metadata, replicaId, fetchInfo.fetchOffset, fetchTimeMs))

        if (preferredReadReplica.isDefined) {
          replicaSelectorOpt.foreach{ selector =>
            debug(s"Replica selector ${selector.getClass.getSimpleName} returned preferred replica " +
              s"${preferredReadReplica.get} for $clientMetadata")
          }
          // If a preferred read-replica is set, skip the read
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = offsetSnapshot.highWatermark.messageOffset,
            leaderLogStartOffset = offsetSnapshot.logStartOffset,
            leaderLogEndOffset = offsetSnapshot.logEndOffset.messageOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = -1L,
            readSize = 0,
            lastStableOffset = Some(offsetSnapshot.lastStableOffset.messageOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None)
        } else {
          // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
          //read()方法的关键处理是调用 Partition.readRecords() 方法执行读取操作
          //读取数据并获取读取结果，每个分区的读取结果中，都包含了:
          //                1. Leader 副本的 LEO、
          //                2.              HW、
          //                3.              LogStartOffset，
          //                4. Follower 副本的LogStartOffset
          //                等信息
          //主要是获取了 Leader 副本的高水位值：
          val readInfo: LogReadInfo = partition.readRecords(
            //读取的起始偏移量，即Follower的LEO
            fetchOffset = fetchInfo.fetchOffset,
            //Follower副本保存的Leader epoch
            currentLeaderEpoch = fetchInfo.currentLeaderEpoch,
            maxBytes = adjustedMaxBytes,
            fetchIsolation = fetchIsolation,
            fetchOnlyFromLeader = fetchOnlyFromLeader,
            minOneMessage = minOneMessage)

          // Check if the HW known to the follower is behind the actual HW
          val followerNeedsHwUpdate: Boolean = partition.getReplica(replicaId)
            .exists(replica => replica.lastSentHighWatermark < readInfo.highWatermark)

          //获取读到的数据
          val fetchDataInfo = if (shouldLeaderThrottle(quota, tp, replicaId)) {
            // If the partition is being throttled, simply return an empty set.
            //如果分区被限流了，那么返回一个空集合
            FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else if (!hardMaxBytesLimit && readInfo.fetchedData.firstEntryIncomplete) {
            // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
            // progress in such cases and don't need to report a `RecordTooLargeException`
            //如果返回的消息集合不完整，也返回一个空集合
            FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else {
            //正常返回
            readInfo.fetchedData
          }

          //封装返回结果
          LogReadResult(info = fetchDataInfo,
            //Leader的HW
            highWatermark = readInfo.highWatermark,
            //Leader的LogStartOffset
            leaderLogStartOffset = readInfo.logStartOffset,
            //Leader的LEO
            leaderLogEndOffset = readInfo.logEndOffset,
            //Follower的LogStartOffset
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = fetchTimeMs,
            readSize = adjustedMaxBytes,
            lastStableOffset = Some(readInfo.lastStableOffset),
            preferredReadReplica = preferredReadReplica,
            followerNeedsHwUpdate = followerNeedsHwUpdate,
            //异常信息
            exception = None)
        }
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderForPartitionException |
                 _: UnknownLeaderEpochException |
                 _: FencedLeaderEpochException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: OffsetOutOfRangeException) =>
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            readSize = 0,
            lastStableOffset = None,
            exception = Some(e))
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

          val fetchSource = Request.describeReplicaId(replicaId)
          error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
            s"on partition $tp: $fetchInfo", e)

          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            readSize = 0,
            lastStableOffset = None,
            exception = Some(e))
      }
    }

    //读取的最大字节
    var limitBytes = fetchMaxBytes
    //封装结果对象
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    //是否至少返回一条消息
    var minOneMessage = !hardMaxBytesLimit

    //遍历待FETCH分区，进行消息数据的读取，
    //这个方法的关键处理是调用 Partition.readRecords() 方法执行读取操作
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      //调用 read() 方法读取 topic 分区内存储的消息数据
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      //获取从每个分区读取的数据的字节数
      val recordBatchSize = readResult.info.records.sizeInBytes
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      // 如果我们读到一次消息，就把从一个分区至少返回一条消息的配置项去掉
      if (recordBatchSize > 0)
        minOneMessage = false
      //更新还可以读取的字节数，如果已读取的数据超过限制了就不能读了
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      //将分区的读取结果保存到结果集合中
      result += (tp -> readResult)
    }
    //返回结果集
    result
  }

  /**
    * Using the configured [[ReplicaSelector]], determine the preferred read replica for a partition given the
    * client metadata, the requested offset, and the current set of replicas. If the preferred read replica is the
    * leader, return None
    */
  def findPreferredReadReplica(tp: TopicPartition,
                               clientMetadata: ClientMetadata,
                               replicaId: Int,
                               fetchOffset: Long,
                               currentTimeMs: Long): Option[Int] = {
    val partition = getPartitionOrException(tp, expectLeader = false)

    if (partition.isLeader) {
      if (Request.isValidBrokerId(replicaId)) {
        // Don't look up preferred for follower fetches via normal replication
        Option.empty
      } else {
        replicaSelectorOpt.flatMap { replicaSelector =>
          val replicaEndpoints = metadataCache.getPartitionReplicaEndpoints(tp, new ListenerName(clientMetadata.listenerName))
          var replicaInfoSet: Set[ReplicaView] = partition.remoteReplicas
            // Exclude replicas that don't have the requested offset (whether or not if they're in the ISR)
            .filter(replica => replica.logEndOffset >= fetchOffset)
            .filter(replica => replica.logStartOffset <= fetchOffset)
            .map(replica => new DefaultReplicaView(
              replicaEndpoints.getOrElse(replica.brokerId, Node.noNode()),
              replica.logEndOffset,
              currentTimeMs - replica.lastCaughtUpTimeMs))
            .toSet

          if (partition.leaderReplicaIdOpt.isDefined) {
            val leaderReplica: ReplicaView = partition.leaderReplicaIdOpt
              .map(replicaId => replicaEndpoints.getOrElse(replicaId, Node.noNode()))
              .map(leaderNode => new DefaultReplicaView(leaderNode, partition.localLogOrException.logEndOffset, 0L))
              .get
            replicaInfoSet ++= Set(leaderReplica)

            val partitionInfo = new DefaultPartitionView(replicaInfoSet.asJava, leaderReplica)
            replicaSelector.select(tp, clientMetadata, partitionInfo).asScala
              .filter(!_.endpoint.isEmpty)
              // Even though the replica selector can return the leader, we don't want to send it out with the
              // FetchResponse, so we exclude it here
              .filter(!_.equals(leaderReplica))
              .map(_.endpoint.id)
          } else {
            None
          }
        }
      }
    } else {
      None
    }
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, topicPartition: TopicPartition, replicaId: Int): Boolean = {
    val isReplicaInSync = nonOfflinePartition(topicPartition).exists(_.inSyncReplicaIds.contains(replicaId))
    !isReplicaInSync && quota.isThrottled(topicPartition) && quota.isQuotaExceeded
  }

  def getLogConfig(topicPartition: TopicPartition): Option[LogConfig] = localLog(topicPartition).map(_.config)

  def getMagic(topicPartition: TopicPartition): Option[Byte] = getLogConfig(topicPartition).map(_.messageFormatVersion.recordVersion.value)

  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) : Seq[TopicPartition] =  {
    replicaStateChangeLock synchronized {
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) {
        val stateControllerEpochErrorMessage = s"Received update metadata request with correlation id $correlationId " +
          s"from an old controller ${updateMetadataRequest.controllerId} with epoch ${updateMetadataRequest.controllerEpoch}. " +
          s"Latest known controller epoch is $controllerEpoch"
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateChangeLogger.messageWithPrefix(stateControllerEpochErrorMessage))
      } else {
        val deletedPartitions = metadataCache.updateMetadata(correlationId, updateMetadataRequest)
        controllerEpoch = updateMetadataRequest.controllerEpoch
        /*** Didi-Kafka 灾备 ↓ ***/
        // remove mirror unknown topic
        updateMetadataRequest.partitionStates().asScala
          .filter(s => s.leader() != LeaderAndIsr.LeaderDuringDelete)
          .foreach(s => removeUnknownLocalMirrorTopic(s.topicName()))
        /*** Didi-Kafka 灾备 ↑ ***/
        deletedPartitions
      }
    }
  }

  /**
   * 1. 校验controller epoch是否合规，只处理比自己epoch大且本地有副本的tp的请求
   * 2. 调用makeLeaders和makeFollowers方法构造新增的leader partition和follower partition【此处为主要逻辑，后面小结详细介绍】
   * 3. 如果是第一次收到请求，启动定时更新hw的线程
   * 4. 停掉空的Fetcher线程
   * 5. 调用回调函数，让coordinator处理新增的leader partition和follower partition
   */
  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndIsrRequest: LeaderAndIsrRequest,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): LeaderAndIsrResponse = {
    if (stateChangeLogger.isTraceEnabled) {
      leaderAndIsrRequest.partitionStates.asScala.foreach { partitionState =>
        stateChangeLogger.trace(s"Received LeaderAndIsr request $partitionState " +
          s"correlation id $correlationId from controller ${leaderAndIsrRequest.controllerId} " +
          s"epoch ${leaderAndIsrRequest.controllerEpoch}")
      }
    }
    replicaStateChangeLock synchronized {
      //如果leaderAndIsrRequest请求中携带的Controller Epoch小于当前节点的controllerEpoch，
      // 说明是过期Controller发送的请求
      if (leaderAndIsrRequest.controllerEpoch < controllerEpoch) {
        stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller ${leaderAndIsrRequest.controllerId} with " +
          s"correlation id $correlationId since its controller epoch ${leaderAndIsrRequest.controllerEpoch} is old. " +
          s"Latest known controller epoch is $controllerEpoch")
        //抛出响应的异常
        leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_CONTROLLER_EPOCH.exception)
      } else {
        //初始化一个结果集，保存分区和对应处理结果
        val responseMap = new mutable.HashMap[TopicPartition, Errors]
        //获取控制器节点的BrokerId
        val controllerId = leaderAndIsrRequest.controllerId
        //更新当前的Controller Epoch 值
        controllerEpoch = leaderAndIsrRequest.controllerEpoch

        // First check partition's leader epoch
        //定义存储分区信息的集合
        val partitionStates = new mutable.HashMap[Partition, LeaderAndIsrPartitionState]()
        val updatedPartitions = new mutable.HashSet[Partition]

        // 遍历LeaderAndIsrRequest请求中的所有分区状态，进行Leader Epoch校验
        leaderAndIsrRequest.partitionStates.asScala.foreach { partitionState =>
          val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
          val partitionOpt = getPartition(topicPartition) match {
            case HostedPartition.Offline =>
              stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                "partition is in an offline log directory")
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
              None

            case HostedPartition.Online(partition) =>
              updatedPartitions.add(partition)
              Some(partition)

            case HostedPartition.None =>
              val partition = Partition(topicPartition, time, this)
              allPartitions.putIfNotExists(topicPartition, HostedPartition.Online(partition))
              updatedPartitions.add(partition)
              Some(partition)
          }

          partitionOpt.foreach { partition =>
            //获取当前分区对象的Leader Epoch
            val currentLeaderEpoch = partition.getLeaderEpoch
            //获取请求中携带的Leader Epoch
            val requestLeaderEpoch = partitionState.leaderEpoch
            //如果请求中的Leader Epoch > 当前分区的Leader Epoch
            if (requestLeaderEpoch > currentLeaderEpoch) {
              // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
              // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
              //且分区的初始副本分配中包含当前节点
              if (partitionState.replicas.contains(localBrokerId))
                //将分区状态放入partitionState集合
                partitionStates.put(partition, partitionState)
              else {
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
                  s"correlation id $correlationId epoch $controllerEpoch for partition $topicPartition as itself is not " +
                  s"in assigned replica list ${partitionState.replicas.asScala.mkString(",")}")
                responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
              }
            } else if (requestLeaderEpoch < currentLeaderEpoch) {
              //如果请求中的Leader Epoch < 当前分区的Leader Epoch，说明是过期leader的请求
              stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                s"leader epoch $requestLeaderEpoch is smaller than the current " +
                s"leader epoch $currentLeaderEpoch")
              //将该分区放入结果集，并封装一个STALE_CONTROLLER_EPOCH错误
              responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
            } else {
              stateChangeLogger.debug(s"Ignoring LeaderAndIsr request from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                s"leader epoch $requestLeaderEpoch matches the current leader epoch")
              //将该分区放入结果集，并封装一个STALE_CONTROLLER_EPOCH错误
              responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
            }
          }
        }

        // TODO-ssy 确定哪些分区的Leader副本在本broker上
        val partitionsTobeLeader = partitionStates.filter { case (_, partitionState) =>
          partitionState.leader == localBrokerId
        }
        // TODO-ssy 确定哪些分区的Follower副本在本broker上
        val partitionsToBeFollower = partitionStates -- partitionsTobeLeader.keys

        val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
        //为partitionsTobeLeader集合中的所有分区在本机的副本执行成为Leader的操作，调用makeLeaders方法
        val partitionsBecomeLeader = if (partitionsTobeLeader.nonEmpty)
          makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, correlationId, responseMap,
            highWatermarkCheckpoints)
        else
          Set.empty[Partition]
        //为partitionsToBeFollower集合中的所有分区在本机的副本执行成为Follower的操作，调用makeFollowers方法
        val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
          makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap,
            highWatermarkCheckpoints)
        else
          Set.empty[Partition]

        /*
         * KAFKA-8392
         * For topic partitions of which the broker is no longer a leader, delete metrics related to
         * those topics. Note that this means the broker stops being either a replica or a leader of
         * partitions of said topics
         */
        val leaderTopicSet = leaderPartitionsIterator.map(_.topic).toSet
        val followerTopicSet = partitionsBecomeFollower.map(_.topic).toSet
        followerTopicSet.diff(leaderTopicSet).foreach(brokerTopicStats.removeOldLeaderMetrics)

        // remove metrics for brokers which are not followers of a topic
        leaderTopicSet.diff(followerTopicSet).foreach(brokerTopicStats.removeOldFollowerMetrics)

        //遍历请求中的所有分区
        leaderAndIsrRequest.partitionStates.asScala.foreach { partitionState =>
          val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
          /*
           * If there is offline log directory, a Partition object may have been created by getOrCreatePartition()
           * before getOrCreateReplica() failed to create local replica due to KafkaStorageException.
           * In this case ReplicaManager.allPartitions will map this topic-partition to an empty Partition object.
           * we need to map this topic-partition to OfflinePartition instead.
           */
          /**
           * 如果指定了一个离线的分区路径，那么通过getOrCreatePartition方法创建分区的本地副本抛出异常之前，getOrCreatePartition方法
           * 可能已经创建了一个对应的分区对象了，这时在allPartitions对象池中，就会添加一个Partition对象为空的TopicPartition，
           * 这里将这个空的Partition对象修改为OfflinePartition
           */
          if (localLog(topicPartition).isEmpty)
          //如果当前节点没有指定分区的本地副本，且该分区的状态为离线状态，则更新该分区为离线分区
            markPartitionOffline(topicPartition)
        }

        // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
        // have been completely populated before starting the checkpointing there by avoiding weird race conditions
        startHighWatermarkCheckPointThread()

        val futureReplicasAndInitialOffset = new mutable.HashMap[TopicPartition, InitialFetchState]
        //遍历前面Leader Epoch校验正常的分区，看是否需要做副本迁移
        for (partition <- updatedPartitions) {
          val topicPartition = partition.topicPartition
          //如果已经定义了指定分区对应的Future日志对象
          // 这里用于做副本迁移，当用户希望将副本从一个目录迁移到另一个目录时，会将迁移后的目录定义为-future后缀的目录
          //然后在该目录下创建future日志，当future日志追上current日志时，来替换当前分区的日志
          if (logManager.getLog(topicPartition, isFuture = true).isDefined) {
            partition.log.foreach { log =>
              val leader = BrokerEndPoint(config.brokerId, "localhost", -1)

              // Add future replica to partition's map
              partition.createLogIfNotExists(Request.FutureLocalReplicaId, isNew = false, isFutureReplica = true,
                highWatermarkCheckpoints)

              // pause cleaning for partitions that are being moved and start ReplicaAlterDirThread to move
              // replica from source dir to destination dir
              logManager.abortAndPauseCleaning(topicPartition)

              futureReplicasAndInitialOffset.put(topicPartition, InitialFetchState(leader,
                partition.getLeaderEpoch, log.highWatermark))
            }
          }
        }
        //副本修改日志路径管理器拉取指定分区的数据到future副本
        replicaAlterLogDirsManager.addFetcherForPartitions(futureReplicasAndInitialOffset)

        // 关闭空闲副本拉取线程
        replicaFetcherManager.shutdownIdleFetcherThreads()
        // 关闭空闲日志路径数据迁移线程
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
        // Didi-Kafka 灾备
        mirrorFetcherManager.shutdownIdleFetcherThreads()
        // 执行Leader变更之后的回调逻辑
        onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
        val responsePartitions = responseMap.iterator.map { case (tp, error) =>
          new LeaderAndIsrPartitionError()
            .setTopicName(tp.topic)
            .setPartitionIndex(tp.partition)
            .setErrorCode(error.code)
        }.toBuffer
        //封装LeaderAndIsrResponse响应
        new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
          .setErrorCode(Errors.NONE.code)
          .setPartitionErrors(responsePartitions.asJava))
      }
    }
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,
                          controllerEpoch: Int,
                          partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Errors],
                          highWatermarkCheckpoints: OffsetCheckpoints): Set[Partition] = {
    partitionStates.keys.foreach { partition =>
      stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from " +
        s"controller $controllerId epoch $controllerEpoch starting the become-leader transition for " +
        s"partition ${partition.topicPartition}")
    }

    for (partition <- partitionStates.keys)
      responseMap.put(partition.topicPartition, Errors.NONE)

    //保存成功转为leader的partition副本
    val partitionsToMakeLeaders = mutable.Set[Partition]()


    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
      // Update the partition information to be the leader
      partitionStates.foreach { case (partition, partitionState) =>
        try {
          //TODO-ssy 尝试将该partition副本变为leader，若转换成功则加入返回列表
          if (partition.makeLeader(controllerId, partitionState, correlationId, highWatermarkCheckpoints)) {
            partitionsToMakeLeaders += partition
            stateChangeLogger.trace(s"Stopped fetchers as part of become-leader request from " +
              s"controller $controllerId epoch $controllerEpoch with correlation id $correlationId for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch})")
          } else
            stateChangeLogger.info(s"Skipped the become-leader state change after marking its " +
              s"partition as leader with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} (last update controller epoch ${partitionState.controllerEpoch}) " +
              s"since it is already the leader for the partition.")
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-leader state change with " +
              s"correlation id $correlationId from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) since " +
              s"the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the leader for partition $partition in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

    } catch {
      case e: Throwable =>
        partitionStates.keys.foreach { partition =>
          stateChangeLogger.error(s"Error while processing LeaderAndIsr request correlationId $correlationId received " +
            s"from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition}", e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    /*** Didi-Kafka 灾备 1 ↓ ***/
    // add mirror topic to fetch remote data
    // TODO-ssy 对成功转换为leader的副本，执行makeMirrors方法
    val partitionsToMakeMirrors = partitionsToMakeLeaders
      .flatMap(p => mirrorTopicsByLocal.get(p.topic)
        .map(mt => new TopicPartition(mt.remoteTopic, p.partitionId)))
    makeMirrors(partitionsToMakeMirrors)
    /*** Didi-Kafka 灾备 1 ↑ ***/

    partitionStates.keys.foreach { partition =>
      stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
        s"epoch $controllerEpoch for the become-leader transition for partition ${partition.topicPartition}")
    }

    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. Clear the produce and fetch requests in the purgatory
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   */
  private def makeFollowers(controllerId: Int,
                            controllerEpoch: Int,
                            partitionStates: Map[Partition, LeaderAndIsrPartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Errors],
                            highWatermarkCheckpoints: OffsetCheckpoints) : Set[Partition] = {
    partitionStates.foreach { case (partition, partitionState) =>
      stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
        s"epoch $controllerEpoch starting the become-follower transition for partition ${partition.topicPartition} with leader " +
        s"${partitionState.leader}")
    }

    for (partition <- partitionStates.keys)
      responseMap.put(partition.topicPartition, Errors.NONE)

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()
    try {
      /** TODO-ssy Didi-Kafka 灾备 1 ↓ **/
      //对转换为follower的partition副本，执行removeMirrors方法
      // First stop mirror fetchers for all the partitions
      removeMirrors(
        //partitionStates.keySet.filterlp => mirrorTopics.contains(p.topic)).map(_.topicPartition),
        partitionStates.keySet.flatMap(p => mirrorTopicsByLocal.get(p.topic)
          .map(mt => new TopicPartition(mt.remoteTopic, p.partitionId))), "local state change to follower")
      /** Didi-Kafka 灾备 1 ↑ **/

      // TODO: Delete leaders from LeaderAndIsrRequest
      partitionStates.foreach { case (partition, partitionState) =>
        val newLeaderBrokerId = partitionState.leader
        try {
          metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
            // Only change partition state when the leader is available
            case Some(_) =>
              if (partition.makeFollower(controllerId, partitionState, correlationId, highWatermarkCheckpoints))
                partitionsToMakeFollower += partition
              else
                stateChangeLogger.info(s"Skipped the become-follower state change after marking its partition as " +
                  s"follower with correlation id $correlationId from controller $controllerId epoch $controllerEpoch " +
                  s"for partition ${partition.topicPartition} (last update " +
                  s"controller epoch ${partitionState.controllerEpoch}) " +
                  s"since the new leader $newLeaderBrokerId is the same as the old leader")
            case None =>
              // The leader broker should always be present in the metadata cache.
              // If not, we should record the error message and abort the transition process for this partition
              stateChangeLogger.error(s"Received LeaderAndIsrRequest with correlation id $correlationId from " +
                s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
                s"(last update controller epoch ${partitionState.controllerEpoch}) " +
                s"but cannot become follower since the new leader $newLeaderBrokerId is unavailable.")
              // Create the local replica even if the leader is unavailable. This is required to ensure that we include
              // the partition's high watermark in the checkpoint file (see KAFKA-1647)
              partition.createLogIfNotExists(localBrokerId, isNew = partitionState.isNew, isFutureReplica = false,
                highWatermarkCheckpoints)
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-follower state change with correlation id $correlationId from " +
              s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) with leader " +
              s"$newLeaderBrokerId since the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the follower for partition $partition with leader " +
              s"$newLeaderBrokerId in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(s"Stopped fetchers as part of become-follower request from controller $controllerId " +
          s"epoch $controllerEpoch with correlation id $correlationId for partition ${partition.topicPartition} with leader " +
          s"${partitionStates(partition).leader}")
      }

      partitionsToMakeFollower.foreach { partition =>
        completeDelayedFetchOrProduceRequests(partition.topicPartition)
      }

      partitionsToMakeFollower.foreach { partition =>
        stateChangeLogger.trace(s"Truncated logs and checkpointed recovery boundaries for partition " +
          s"${partition.topicPartition} as part of become-follower request with correlation id $correlationId from " +
          s"controller $controllerId epoch $controllerEpoch with leader ${partitionStates(partition).leader}")
      }

      if (isShuttingDown.get()) {
        partitionsToMakeFollower.foreach { partition =>
          stateChangeLogger.trace(s"Skipped the adding-fetcher step of the become-follower state " +
            s"change with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
            s"partition ${partition.topicPartition} with leader ${partitionStates(partition).leader} " +
            "since it is shutting down")
        }
      } else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map { partition =>
          val leader = metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get
            .brokerEndPoint(config.interBrokerListenerName)
          val fetchOffset = partition.localLogOrException.highWatermark
          partition.topicPartition -> InitialFetchState(leader, partition.getLeaderEpoch, fetchOffset)
       }.toMap

        //为分区 Follower 副本设置 Fetcher 线程，
        // 该线程用于从分区Leader 副本处同步消息数据
        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
        partitionsToMakeFollowerWithLeaderAndOffset.foreach { case (partition, initialFetchState) =>
          stateChangeLogger.trace(s"Started fetcher to new leader as part of become-follower " +
            s"request from controller $controllerId epoch $controllerEpoch with correlation id $correlationId for " +
            s"partition $partition with leader ${initialFetchState.leader}")
        }
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"Error while processing LeaderAndIsr request with correlationId $correlationId " +
          s"received from controller $controllerId epoch $controllerEpoch", e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    partitionStates.keys.foreach { partition =>
      stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
        s"epoch $controllerEpoch for the become-follower transition for partition ${partition.topicPartition} with leader " +
        s"${partitionStates(partition).leader}")
    }

    partitionsToMakeFollower
  }

  //周期性的检测是否有副本掉队，进而收缩isr列表
  //遍历所有在线状态的分区,检查是否需要收缩
  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")

    // Shrink ISRs for non offline partitions
    //遍历所有在线的副本
    allPartitions.keys.foreach { topicPartition =>
      nonOfflinePartition(topicPartition).foreach(_.maybeShrinkIsr())
    }
  }

  /**
   * Update the follower's fetch state on the leader based on the last fetch request and update `readResult`.
   * If the follower replica is not recognized to be one of the assigned replicas, do not update
   * `readResult` so that log start/end offset and high watermark is consistent with
   * records in fetch response. Log start/end offset and high watermark may change not only due to
   * this fetch request, e.g., rolling new log segment and removing old log segment may move log
   * start offset further than the last offset in the fetched records. The followers will get the
   * updated leader's state in the next fetch response.
   */
  //用于分区 Leader 副本对来自 Follower 的 Fetch 请求的处理
  // 核心处理执行 Partition.updateFollowerFetchState() 方法,
  // 更新leader副本保存的follower副本fetch的相关信息fetchState
  private def updateFollowerFetchState(followerId: Int,
                                       readResults: Seq[(TopicPartition, LogReadResult)]): Seq[(TopicPartition, LogReadResult)] = {
    readResults.map { case (topicPartition, readResult) =>
      val updatedReadResult = if (readResult.error != Errors.NONE) {
        debug(s"Skipping update of fetch state for follower $followerId since the " +
          s"log read returned error ${readResult.error}")
        readResult
      } else {
        nonOfflinePartition(topicPartition) match {
          case Some(partition) =>
            //更新 Leader 副本保存的远程副本列表中的 LEO
            if (partition.updateFollowerFetchState(followerId,
              followerFetchOffsetMetadata = readResult.info.fetchOffsetMetadata,
              followerStartOffset = readResult.followerLogStartOffset,
              followerFetchTimeMs = readResult.fetchTimeMs,
              leaderEndOffset = readResult.leaderLogEndOffset,
              lastSentHighwatermark = readResult.highWatermark)) {
              readResult
            } else {
              warn(s"Leader $localBrokerId failed to record follower $followerId's position " +
                s"${readResult.info.fetchOffsetMetadata.messageOffset}, and last sent HW since the replica " +
                s"is not recognized to be one of the assigned replicas ${partition.assignmentState.replicas.mkString(",")} " +
                s"for partition $topicPartition. Empty records will be returned for this partition.")
              readResult.withEmptyFetchInfo
            }
          case None =>
            warn(s"While recording the replica LEO, the partition $topicPartition hasn't been created.")
            readResult
        }
      }
      topicPartition -> updatedReadResult
    }
  }

  private def leaderPartitionsIterator: Iterator[Partition] =
    nonOfflinePartitionsIterator.filter(_.leaderLogIfLocal.isDefined)

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
    nonOfflinePartition(topicPartition).flatMap(_.leaderLogIfLocal.map(_.logEndOffset))

  // Flushes the highwatermark value for all partitions to the highwatermark file
  def checkpointHighWatermarks(): Unit = {
    val localLogs = nonOfflinePartitionsIterator.flatMap { partition =>
      val logsList: mutable.Set[Log] = mutable.Set()
      partition.log.foreach(logsList.add)
      partition.futureLog.foreach(logsList.add)
      logsList
    }.toBuffer
    val logsByDir = localLogs.groupBy(_.dir.getParent)
    for ((dir, logs) <- logsByDir) {
      val hwms = logs.map(log => log.topicPartition -> log.highWatermark).toMap
      try {
        highWatermarkCheckpoints.get(dir).foreach(_.write(hwms))
      } catch {
        case e: KafkaStorageException =>
          error(s"Error while writing to highwatermark file in directory $dir", e)
      }
    }
  }

  // Used only by test
  def markPartitionOffline(tp: TopicPartition): Unit = replicaStateChangeLock synchronized {
    allPartitions.put(tp, HostedPartition.Offline)
    Partition.removeMetrics(tp)
  }

  // logDir should be an absolute path
  // sendZkNotification is needed for unit test
  def handleLogDirFailure(dir: String, sendZkNotification: Boolean = true): Unit = {
    if (!logManager.isLogDirOnline(dir))
      return
    warn(s"Stopping serving replicas in dir $dir")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = nonOfflinePartitionsIterator.filter { partition =>
        partition.log.exists { _.dir.getParent == dir }
      }.map(_.topicPartition).toSet

      val partitionsWithOfflineFutureReplica = nonOfflinePartitionsIterator.filter { partition =>
        partition.futureLog.exists { _.dir.getParent == dir }
      }.toSet

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions)
      replicaAlterLogDirsManager.removeFetcherForPartitions(newOfflinePartitions ++ partitionsWithOfflineFutureReplica.map(_.topicPartition))

      partitionsWithOfflineFutureReplica.foreach(partition => partition.removeFutureLocalReplica(deleteFromLogDir = false))
      newOfflinePartitions.foreach { topicPartition =>
        markPartitionOffline(topicPartition)
      }
      newOfflinePartitions.map(_.topic).foreach { topic: String =>
        maybeRemoveTopicMetrics(topic)
      }
      highWatermarkCheckpoints = highWatermarkCheckpoints.filter { case (checkpointDir, _) => checkpointDir != dir }

      warn(s"Broker $localBrokerId stopped fetcher for partitions ${newOfflinePartitions.mkString(",")} and stopped moving logs " +
           s"for partitions ${partitionsWithOfflineFutureReplica.mkString(",")} because they are in the failed log directory $dir.")
    }
    logManager.handleLogDirFailure(dir)

    if (sendZkNotification)
      zkClient.propagateLogDirEvent(localBrokerId)
    warn(s"Stopped serving replicas in dir $dir")
  }

  def removeMetrics(): Unit = {
    removeMetric("LeaderCount")
    removeMetric("PartitionCount")
    removeMetric("OfflineReplicaCount")
    removeMetric("UnderReplicatedPartitions")
    removeMetric("UnderMinIsrPartitionCount")
    removeMetric("AtMinIsrPartitionCount")
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true): Unit = {
    info("Shutting down")
    removeMetrics()
    if (logDirFailureHandler != null)
      logDirFailureHandler.shutdown()
    /*** Didi-Kafka 灾备 ↓ ***/
    if (mirrorMetadataThread != null)
      mirrorMetadataThread.shutdown()
    remoteClients.foreach {
      case (clusterId, client) => {
        try client.close()
        catch {
          case e: Exception =>
            error(s"Failed to close cluster $clusterId network client", e)
        }
      }
    }
    /*** Didi-Kafka 灾备 ↑ ***/
    replicaFetcherManager.shutdown()
    // Didi-Kafka 灾备
    mirrorFetcherManager.shutdown()
    replicaAlterLogDirsManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    delayedElectLeaderPurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    replicaSelectorOpt.foreach(_.close)
    info("Shut down completely")
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String], quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)
  }

  /*** Didi-Kafka 灾备 ↓ ***/
  protected def createMirrorFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String],
                                           quotaManager: ReplicationQuotaManager) = {
    new MirrorFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)
  }
  /*** Didi-Kafka 灾备 ↑ ***/

  protected def createReplicaAlterLogDirsManager(quotaManager: ReplicationQuotaManager, brokerTopicStats: BrokerTopicStats) = {
    new ReplicaAlterLogDirsManager(config, this, quotaManager, brokerTopicStats)
  }

  protected def createReplicaSelector(): Option[ReplicaSelector] = {
    config.replicaSelectorClassName.map { className =>
      val tmpReplicaSelector: ReplicaSelector = CoreUtils.createObject[ReplicaSelector](className)
      tmpReplicaSelector.configure(config.originals())
      tmpReplicaSelector
    }
  }

  def lastOffsetForLeaderEpoch(requestedEpochInfo: Map[TopicPartition, OffsetsForLeaderEpochRequest.PartitionData]): Map[TopicPartition, EpochEndOffset] = {
    requestedEpochInfo.map { case (tp, partitionData) =>
      val epochEndOffset = getPartition(tp) match {
        case HostedPartition.Online(partition) =>
          partition.lastOffsetForLeaderEpoch(partitionData.currentLeaderEpoch, partitionData.leaderEpoch,
            fetchOnlyFromLeader = true)

        case HostedPartition.Offline =>
          new EpochEndOffset(Errors.KAFKA_STORAGE_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

        case HostedPartition.None if metadataCache.contains(tp) =>
          new EpochEndOffset(Errors.NOT_LEADER_FOR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

        case HostedPartition.None =>
          new EpochEndOffset(Errors.UNKNOWN_TOPIC_OR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
      tp -> epochEndOffset
    }
  }

  def electLeaders(
    controller: KafkaController,
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    responseCallback: Map[TopicPartition, ApiError] => Unit,
    requestTimeout: Int
  ): Unit = {

    val deadline = time.milliseconds() + requestTimeout

    def electionCallback(results: Map[TopicPartition, Either[ApiError, Int]]): Unit = {
      val expectedLeaders = mutable.Map.empty[TopicPartition, Int]
      val failures = mutable.Map.empty[TopicPartition, ApiError]
      results.foreach {
        case (partition, Right(leader)) => expectedLeaders += partition -> leader
        case (partition, Left(error)) => failures += partition -> error
      }

      if (expectedLeaders.nonEmpty) {
        val watchKeys = expectedLeaders.iterator.map {
          case (tp, _) => TopicPartitionOperationKey(tp)
        }.toBuffer

        delayedElectLeaderPurgatory.tryCompleteElseWatch(
          new DelayedElectLeader(
            math.max(0, deadline - time.milliseconds()),
            expectedLeaders,
            failures,
            this,
            responseCallback
          ),
          watchKeys
        )
      } else {
          // There are no partitions actually being elected, so return immediately
          responseCallback(failures)
      }
    }

    controller.electLeaders(partitions, electionType, electionCallback)
  }

  /***TODO-ssy Didi-Kafka 灾备 ↓ ***/
  newGauge("MirrorTopicCount", () => mirrorTopics.size)
  newGauge("MirrorRemoteClusterCount", () => remoteClients.size)
  newGauge("MirrorUnknownTopicCount", () => unknownMirrorTopicLocalNames.size)
  newGauge("MirrorPartitionCount", () => mirrorFetcherManager.allPartitions().size)
  newGauge("MirrorFetchedPartitionCount" , () => mirrorFetcherManager.allFetchedPartitions().size)
  newGauge("MirrorFailedPartitionCount", () => mirrorFetcherManager.allFailedPartitions().size)
  newGauge("MirrorDelayedPartitionCount", () => delayedMakeMirrorPartitions.size)
  newGauge("MirrorPartitionMetadataCount", () => remoteLeaderAndEpochs.size)

  /** 保存 remoteCluster -> KafkaClient 映射，其中KafkaClient的clientId为: "mirror-metadata-cluster-<remoteCluster>" */
  private val remoteClients = mutable.HashMap[String, KafkaClient]()
  /** 保存remoteCluster对应的ProducerMetadata（remoteCluster -> ProducerMetadata）键值对 **/
  private val remoteMetadatas = mutable.HashMap[String, ProducerMetadata]()
  //Remote topic leader and epochs
  /** ReplicaManager 中启动的 mirrorMetadataThread 中循环执行的主方法updateRemoteTopicMetadata()中添加 */
  private val remoteLeaderAndEpochs = mutable.HashMap[TopicPartition, LeaderAndEpoch]()
  //unknown remote topic partition metadata
  //当为partition执行makeMirror()时，若 remoteLeaderAndEpochs 中不存在该partition则将其加入到 delayedMakeMirrorPartitions 中
  private val delayedMakeMirrorPartitions = mutable.Set[TopicPartition]()
  //添加镜像Topic时，若该MirrorTopic的localTopic在本地元数据中不存在，则将该localTopic加入 unknownMirrorTopicLocalNames 中
  private val unknownMirrorTopicLocalNames = mutable.Set[String]()

  /**
   * 需要本节点处理的未获取元数据的localTopic
   * 在每一台KafkaServer中MirrorCoordinator循环执行的主方法syncTopicPartitions中：
   * 将 添加镜像Topic时，对在本地元数据中不存在的localTopic 中：
   *  1. 被__mirror_state分配给本broker
   *  2. 不在 unknownMirrorTopicLocalNamesNeedMetadata 中(即未曾被本机处理过)
   * 的localTopic加入到 unknownMirrorTopicLocalNamesNeedMetadata 中，准备进行和镜像Topic的Topic结构对齐
   */
  val unknownMirrorTopicLocalNamesNeedMetadata = mutable.Set[String]()

  /* lock protecting access to mirror fetchers */
  private val mirrorFetcherLock = new ReentrantLock()
  //在ReplicaManager.startup()中初始化并启动
  private var mirrorMetadataThread: RemoteMetadataThread = null

  //创建对镜像集群的连接客户端
  //clientId为: mirror-metadata-cluster-<镜像集群名>
  private def newRemoteClient(remoteCluster: String, config: HAClusterConfig): (KafkaClient, ProducerMetadata) = {
    info(s"Create remote connection for cluster $remoteCluster")
    val logContext = new LogContext(s" [MirrorCoordinator cluster=$remoteCluster] ")
    val listeners = new ClusterResourceListeners()
    val addresses: util.List[InetSocketAddress] = ClientUtils.parseAndValidateAddresses(
      config.getList(HAClusterConfig.BOOTSTRAP_SERVERS_CONFIG),     //获取 bootstrap.servers 参数
      ClientDnsLookup.DEFAULT)
    val metadata = new ProducerMetadata(
      config.getLong(HAClusterConfig.RETRY_BACKOFF_MS_CONFIG),      //retry.backoff.ms
      config.getLong(HAClusterConfig.METADATA_MAX_AGE_CONFIG),      //metadata.max.age.ms
      config.getLong(HAClusterConfig.METADATA_MAX_AGE_CONFIG),      //metadata.max.age.ms
      logContext,
      listeners,
      time)

    metadata.bootstrap(addresses)

    val channelBuilder = ClientUtils.createChannelBuilder(config, time, logContext)
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.getLong(HAClusterConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
      metrics,
      time,
      "mirror-metadata",
      Map("remote-cluster-id" -> remoteCluster).asJava,
      false,
      channelBuilder,
      logContext
    )
    //clientId为: mirror-metadata-cluster-<镜像集群名>
    val kafkaClient = new NetworkClient(
      selector,
      metadata,
      s"mirror-metadata-cluster-$remoteCluster",
      1,
      0,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.getInt(HAClusterConfig.RECEIVE_BUFFER_CONFIG),
      config.getInt(HAClusterConfig.REQUEST_TIMEOUT_MS_CONFIG),
      ClientDnsLookup.DEFAULT,
      time,
      true,
      new ApiVersions,
      logContext
    )
    (kafkaClient, metadata)
  }

  //获取镜像集群所有配置（通过配置管理）
  private def getMirrorConfig(remoteCluster: String): HAClusterConfig = {
    info(s"Get mirror config for cluster $remoteCluster configs ${ClusterConfigManager.getConfigs(remoteCluster)}")
    val props = new Properties()
    props ++= ClusterConfigManager.getConfigs(remoteCluster)
    new HAClusterConfig(props)
  }

  //TODO-ssy ReplicaManager 中启动的 mirrorMetadataThread 中循环执行的主方法
  //更新镜像Topic元数据
  def updateRemoteTopicMetadata(): Unit = {
    val nowMs = time.milliseconds()                       //开始同步的时间戳
    val mirrorTopics = allMirrorTopics()                  //镜像Topic名 -> 镜像Topic 的映射map
    val mirrorTopicsByLocal = allMirrorTopicsByLocal()    //本地Topic名 -> 镜像Topic 的映射map
    val hostedMirrorTopics = allHostedMirrorTopics()      //所有保存的镜像Topic的MirrorTopic集合
    //未获取localTopic元数据的MirrorTopic集合
    val unknownMirrorTopics = unknownMirrorTopicLocalNamesNeedMetadata.flatMap(topic => mirrorTopicsByLocal.get(topic))

    //TODO trace
    trace( s"Begin update remote topics ${hostedMirrorTopics.map(_.remoteTopic).mkString(", ")} metadata.")
    //存在未获取元数据的localTopic
    if (unknownMirrorTopicLocalNames.nonEmpty)
    info(s"Unknown mirror topics of local topic name ${unknownMirrorTopicLocalNames.mkString(", ")}")

    // 1. maybe init new cluster client
    /** remoteCluster -> updateVersion 映射，存储每个 remoteCluster 的更新前版本号updateVersion(每更新成功1次，updateVersion自增1,主要是用于判断metadata是否更新)**/
    val requiresVersion = mutable.HashMap[String, Int]()
    /**TODO-ssy 1. 为未配置Client的集群创建Client并创建集群元数据(根据集群配置) */
    (hostedMirrorTopics ++ unknownMirrorTopics).map(_.remoteCluster)
      .filterNot(remoteClients.contains)
      .foreach {
        remoteCluster => {
          //根据remoteCluster名和其配置信息创建连接到该集群的Client及元数据
          val remoteClientAndMetadata = newRemoteClient(remoteCluster, getMirrorConfig(remoteCluster))
          /** 保存remoteCluster对应的Client（remoteCluster -> KafkaClient）键值对 **/
          //其中KafkaClient的clientId为: "mirror-metadata-cluster-<remoteCluster>"
          //(remoteClients的唯一添加入口)
          remoteClients += remoteCluster -> remoteClientAndMetadata._1
          //(remoteMetadatas的唯一添加入口)
          /** 添加remoteCluster对应的ProducerMetadata集群元数据（remoteCluster -> ProducerMetadata）键值对 **/
          remoteMetadatas += remoteCluster -> remoteClientAndMetadata._2
          /** 更新 remoteCluster 的更新版本号updateVersion **/
          requiresVersion += remoteCluster -> remoteClientAndMetadata._2.updateVersion()
        }
      }
    //refresh topic
    /** TODO-ssy 2. 向remoteMetadatas中对应集群元数据添加remoteTopic信息 */
    hostedMirrorTopics ++ unknownMirrorTopics.foreach {
      case MirrorTopic(remoteTopic, remoteCluster, _, _, _, _) =>
        remoteMetadatas.get(remoteCluster).foreach(_.add(remoteTopic, nowMs))
    }

    // 3. request metadata update 开始元数据更新
    // i. for delay partitons
    // delayedMakeMirrorPartitions unknown remote topic partition metadata
    (mirrorFetcherManager.allDelayedPartitions() ++ mirrorFetcherManager.allFailedPartitions() ++
      delayedMakeMirrorPartitions)
      .flatMap {
        tp =>
          info(s"Request metadata update for delayed remote partition $tp")
          mirrorTopics.get(tp.topic)
      }
      .map(_.remoteCluster)
      .foreach {
        remoteCluster: String => {
          remoteMetadatas.get(remoteCluster).foreach {
            //存储每个 remoteCluster 的更新前版本号updateVersion
            r => requiresVersion += remoteCluster -> r.requestUpdate()
          }
        }
      }

    // ii. for unknown mirror topic
    unknownMirrorTopics
      .map(_.remoteCluster )
      .foreach {
        remoteCluster: String => {
          remoteMetadatas.get(remoteCluster).foreach {
            //存储每个 remoteCluster 的更新前版本号updateVersion
            r => requiresVersion += remoteCluster -> r.requestUpdate()
          }
        }
      }

    //4. poll send request and receive response
    //获取所有镜像集群元数据
    remoteClients.values.foreach(_.poll(0, nowMs))

    //5. wait metadata
    //确保所有镜像集群元数据都成功更新
    while ((time.milliseconds() - nowMs < 9 * 1000) && requiresVersion.exists {
      case (remoteCluster, lastVersion) =>
        info(s"Requested cluster $remoteCluster metadate new version: ${remoteMetadatas(remoteCluster).updateVersion()} last version: $lastVersion")
        val condition = remoteMetadatas(remoteCluster).updateVersion() <= lastVersion
        if (condition) {
          remoteClients(remoteCluster).poll(10, time.milliseconds)
        }
        condition
    }) {
      time.sleep(100)
    }

    //TODO trace
    trace(s"Remote topic metadata ${remoteMetadatas.values.map(_.fetch())}")
    info(s"All mirror fetcher remote partitions ${mirrorFetcherManager.allPartitions()}")

    //6. handle state change, maybe change mirror fetcher
    val partitionsToMakeMirrors = mutable.Set[TopicPartition]()
    (mirrorFetcherManager.allPartitions() ++ delayedMakeMirrorPartitions)
      .map(tp => (tp, mirrorTopics.get(tp.topic)))
      .foreach {
        case (topicPartition, maybeTopic: Option[MirrorTopic]) => {
          maybeTopic.map(t =>
            remoteMetadatas(t.remoteCluster)).map(_.currentLeader(topicPartition)).foreach {
              // TODO 考虑低版本可能不存在epoch
              case leaderAndEpoch if leaderAndEpoch.leader.isPresent => {
                // remoteLeaderAndEpochs的唯一添加入口
                /** 保存该partition副本最新的leaderAndEpoch，若之前不存在该partition副本的记录则返回null，否则返回该副本之前的 leaderAndEpoch **/
                //remoteLeaderAndEpochs的唯一添加入口
                val oldLeaderAndEpoch = remoteLeaderAndEpochs.put(topicPartition, leaderAndEpoch)
                //delayedMakeMirrorPartitions的唯一处理入口
                if (delayedMakeMirrorPartitions.contains(topicPartition)) {
                  //add mirror fetcher
                  // 为延迟同步的镜像partition添加fetcher进程
                  info(s"Will add mirror fetcher for delayed remote partition ${topicPartition}")
                  partitionsToMakeMirrors += topicPartition
                } else if (oldLeaderAndEpoch.isDefined &&
                  (leaderAndEpoch.epoch != oldLeaderAndEpoch.get.epoch || leaderAndEpoch.leader !=
                    oldLeaderAndEpoch.get.leader)) {
                  //epoch change
                  info(s"Will change mirror fetcher for remote partition ${topicPartition} from ${oldLeaderAndEpoch.get} to ${leaderAndEpoch}")
                  //add mirror fetcher
                  partitionsToMakeMirrors += topicPartition
                }
              }
              case _ => None
          }
        }
      }
    //7. make mirrors
    makeMirrors(partitionsToMakeMirrors, localStateChange = false)

    // 8.close idle client
    val clusters = (hostedMirrorTopics ++ unknownMirrorTopics)
     .map(_.remoteCluster)
    remoteClients.filterNot(c => clusters.contains(c._1)).foreach {
    case (clusterId, client) => {
      info(s"Close idle cluster $clusterId network client")
      try client.close()
      catch {
        case e: Exception =>
          error (s"Failed to close cluster $clusterId network client", e)
      } finally {
        remoteMetadatas -= clusterId
        remoteClients -= clusterId
      }
    }
    }

    //TODO trace
    info(s"Remote topic leader and epochs ${remoteLeaderAndEpochs}")
  }

  //根据MirrorTopicPartition获取localTopicPartition具体信息
  def getPartitionOrExceptionByRemoteTopicPartition(remoteTopicPartition: TopicPartition,    //镜像TopicPartition
                                                    expectLeader: Boolean                    //这个参数只和出现异常时报错类型有关
                                                   ): Partition = {
    getLocalTopicPartitionByRemoteTopicPartition(remoteTopicPartition) match {
      case Some(tp) => getPartitionOrException(tp, expectLeader)
      case None => throw Errors.UNKNOWN_TOPIC_OR_PARTITION.exception(s"Can't find remote partition $remoteTopicPartition")
    }
  }

  //通过镜像TopicPartition获取LocalTopicPartition
  def getLocalTopicPartitionByRemoteTopicPartition(remoteTopicPartition: TopicPartition):
  Option[TopicPartition] = {
    mirrorTopics.get(remoteTopicPartition.topic()).map(mt => new TopicPartition(mt.localTopic,
      remoteTopicPartition.partition()))
  }

  def allMirrorTopics(): Map[String, MirrorTopic] = {
    mirrorTopics.toMap
  }

  //获取所有保存的镜像Topic的MirrorTopic
  def allHostedMirrorTopics(): Set[MirrorTopic] = {
    (mirrorFetcherManager.allPartitions() ++ delayedMakeMirrorPartitions)
      .flatMap {
        tp => mirrorTopics.get(tp.topic)
      }
  }

  //所有 本地Topic名 -> 镜像Topic 的映射
  def allMirrorTopicsByLocal(): Map[String, MirrorTopic] =
  {
    mirrorTopicsByLocal.toMap
  }

  def getUnknownMirrorTopicLocalNames(): Set[String] = {
    unknownMirrorTopicLocalNames
  }

  def remoteClusterMetadata(remoteCluster: String): Option[Cluster] = {
    remoteMetadatas.get(remoteCluster).map(_.fetch())
  }

  //delayedMakeMirrorPartitions添加的唯一入口
  //当为partition执行makeMirror()时，若 remoteLeaderAndEpochs 中不存在该partition则将其加入到 delayedMakeMirrorPartitions 中
  private def delayMakeMirror(partition: TopicPartition): Unit = {
    info(s"Delay to make mirror for remote partition $partition")
    inLock(mirrorFetcherLock) {
      delayedMakeMirrorPartitions.add(partition)
    }
  }

  def removeMirrors(partitionsToRemove: Set[TopicPartition], reason: String): Unit =
    inLock(mirrorFetcherLock) {
      delayedMakeMirrorPartitions --= partitionsToRemove
      mirrorFetcherManager.removeFetcherForPartitions(partitionsToRemove)

      remoteLeaderAndEpochs --= partitionsToRemove
      partitionsToRemove.foreach { topicPartition =>
        stateChangeLogger.trace(s"Stopped mirror fetchers of " +
          s"$reason for remote partition $topicPartition")
      }
    }

  //TODO-ssy 为partition集合创建镜像Partition
  //通过配置管理添加镜像Topic时，若localTopic的某分区的leader副本在本broker上，则为该partition执行makeMirrors()方法。(其中Topic名为镜像Topic名，partitionId为本partitionId)
  def makeMirrors(partitionsToMakeMirrors: Set[TopicPartition],  //其中Topic名为镜像Topic名，partitionId为本地partitionId(leader副本在本broker上)
                  localStateChange: Boolean = true): Unit = {
    //we do not need to check if the leader exists again since this has been done at the beginning of this process
    //mirrorMetadataThread 中循环执行的主方法会将topicPartition的leaderAndEpoch加入到remoteLeaderAndEpochs中
    /** TODO-ssy 1. 获取 topicPartition -> InitialFetchState(包括镜像分区的leader节点、leaderEpoch和HW) 映射集合 */
    val partitionsToMakeMirrorWithLeaderAndOffset = partitionsToMakeMirrors.flatMap { topicPartition =>
      remoteLeaderAndEpochs.get(topicPartition) match {
        case Some(leaderAndEpoch) =>
          //convert to local topic partiton
          //获取TopicPartition在本地的LocalTopic的每个TopicPartition的HW（？）
          val fetchOffset = {
            //对于 __consumer_offsets 主题
            if (topicPartition.topic().equals(Topic.GROUP_METADATA_TOPIC_NAME) )
              // 当远程leader发生切换时，继续使用之前的fetchOffset
              // epoch order will markPartitionFailed and remove from fetchState
              mirrorFetcherManager.latestFetchOffset(topicPartition).getOrElse(0L)
            else {
              /** 对于非 __consumer_offsets 的镜像主题，获取其HW */
              getPartitionOrExceptionByRemoteTopicPartition(topicPartition, expectLeader = true).localLogOrException.highWatermark
            }
          }
          /** 获取镜像分区的leader节点(包括id、host、port等信息) */
          val leader = leaderAndEpoch.leader.get()
          //创建MirrorPartitionLeader
          val remoteLeader = BrokerEndPoint(leader.id(), leader.host(), leader.port())
          //配置bootstrap.servers即集群信息
          remoteLeader.remoteCluster = Option(mirrorTopics(topicPartition.topic).remoteCluster)   //配置镜像分区leader的镜像集群名(即本集群)
          remoteLeader.mirrorConfig = Option(getMirrorConfig(remoteLeader.remoteCluster.get))     //配置镜像分区leader的镜像集群配置(即本集群配置)
          Option(topicPartition -> InitialFetchState(remoteLeader, leaderAndEpoch.epoch.orElse(-1), fetchOffset))
        case None =>
          //若 remoteLeaderAndEpochs 中不存在该partition则将其加入到 delayedMakeMirrorPartitions 中，
          //等待 mirrorMetadataThread 中循环执行的主方法将topicPartition的leaderAndEpoch加入到remoteLeaderAndEpochs中
          delayMakeMirror(topicPartition)
          None
      }
    }.toMap

    inLock(mirrorFetcherLock) {
      //check mirrors, maybe removed during the process of metadata, and must before remove fetchers
      val allMirrorPartitions = mirrorFetcherManager.allPartitions() ++ delayedMakeMirrorPartitions
      //取partitionsToMakeMirrorWithLeaderAndOffset中存在在allMirrorPartitions的部分
      val partitionsToMakeMirrorWithLeaderAndOffsetWithCheck = partitionsToMakeMirrorWithLeaderAndOffset.filter(t => allMirrorPartitions.contains(t._1))
      //first remove fetcher
      /** 2. 取消对partitionsToMakeMirrors的fetcher线程 */
      mirrorFetcherManager.removeFetcherForPartitions(partitionsToMakeMirrors)
      partitionsToMakeMirrors.foreach { topicPartition =>
        stateChangeLogger.trace(s"Stopped mirror fetchers of " +
          s"${if (localStateChange) "local state change to" else "remote state change to"} " +
          s"become Leader for remote partition $topicPartition")
      }
      //add fetcher
      /** TODO-ssy 3. 进行MirrorFetcher，同步镜像TopicPartition的消息 */
      //为本地分区 Leader 副本设置 Fetcher 线程，该线程用于从镜像分区Leader副本处同步消息数据
      mirrorFetcherManager.addFetcherForPartitions(partitionsToMakeMirrorWithLeaderAndOffsetWithCheck)
      delayedMakeMirrorPartitions --= partitionsToMakeMirrorWithLeaderAndOffsetWithCheck.keySet
      partitionsToMakeMirrorWithLeaderAndOffsetWithCheck.foreach { case (partition, initialFetchState) =>
        stateChangeLogger.trace(s"Started mirror fetchers of " +
          s"${if (localStateChange) "local state change to" else "remote state change to"} " +
          s"become leader for remote partition $partition with leader ${initialFetchState.leader} and epoch ${initialFetchState.currentLeaderEpoch}")
      }
    }
  }

  //通过配置管理添加镜像Topic
  def addMirrorTopics(mirrorTopic: MirrorTopic): Unit = {
    info(s"Add a mirror topic $mirrorTopic" )
    /** 添加 镜像Topic名 -> 镜像Topic 的映射(唯一存储入口) */
    mirrorTopics.put(mirrorTopic.remoteTopic, mirrorTopic)
    /** 添加 本地Topic名 -> 镜像Topic 的映射(唯一存储入口)，会被MirrorCoordinator周期性检测并创建或更新本地Topic以与MirrorTopicpar的tition数相同 */
    mirrorTopicsByLocal.put(mirrorTopic.localTopic, mirrorTopic)
    //TODO-ssy 当localTopic中leader副本在本broker上的partition执行makeMirrors()方法。(其中Topic名为镜像Topic名，partitionId为本partitionId)
    makeMirrors(
      nonOfflinePartitionsIterator
        .filter(tp => tp.isLeader && tp.topic.equals(mirrorTopic.localTopic))
        .map(tp => new TopicPartition(mirrorTopic.remoteTopic, tp.partitionId)).toSet,
      localStateChange = false
    )
    //添加镜像Topic时，若该MirrorTopic的localTopic在本地元数据中不存在，则加入 unknownMirrorTopicLocalNames 中
    if (!metadataCache.contains(mirrorTopic.localTopic)) {
      unknownMirrorTopicLocalNames.add(mirrorTopic.localTopic)
    }
  }

  //通过配置管理移除镜像Topic
  def removeMirrorTopics(remoteTopic: String): Unit = {
    info(s"Remove a mirror topic ${mirrorTopics.get(remoteTopic)} by remote topic $remoteTopic")
    removeMirrors(
      (mirrorFetcherManager.allPartitions() ++ delayedMakeMirrorPartitions).filter(
        p => mirrorTopics.contains(p.topic()) && p.topic().equals(remoteTopic)),
      "remote state change to stop mirror"
    )
    mirrorTopics.remove(remoteTopic).foreach {
      mt => {
        mirrorTopicsByLocal.remove(mt.localTopic)
        unknownMirrorTopicLocalNamesNeedMetadata.remove(mt.localTopic)
        unknownMirrorTopicLocalNames.remove(mt.localTopic)
      }
    }
  }

  //本地元数据更新删除未知topic
  private def removeUnknownLocalMirrorTopic(localTopic: String): Unit = {
    info(s"Local mirror topic $localTopic has bean created, remove it from unknown list")
    unknownMirrorTopicLocalNamesNeedMetadata.remove(localTopic)
    unknownMirrorTopicLocalNames.remove(localTopic)
  }

  //循环获取镜像Topic元数据的线程
  private class RemoteMetadataThread(name: String,  //线程名
                                     period: Long,  //循环周期
                                     unit: TimeUnit) extends
    ShutdownableThread(name) {
    def backoff(): Unit = pause(period, unit)

    override def doWork(): Unit = {
      try {
        //获取镜像Topic元数据
        updateRemoteTopicMetadata()
      } catch {
        case e: Throwable =>
          if (isRunning)
            error("Error due to", e)
      } finally {
        backoff()
      }
    }
  }

//  添加MirrorTopic样例：
//  addMirrorTopics(MirrorTopic("lee_remote_topic_0", "9092", "lee_remote_topic_0", syncTopicPartitions =
//    true))
//  addMirrorTopics(MirrorTopic("lee_remote_topic_1", "9092", "lee_remote_topic_1", syncTopicPartitions =
//    true))
//  addMirrorTopics(MirrorTopic("lee_remote_topic_2", "9092", "lee_remote_topic_2", syncTopicPartitions =
//    true))
}

case class MirrorTopic(remoteTopic: String,
                       remoteCluster: String,
                       localTopic: String,
                       syncTopicPartitions: Boolean = false,
                       syncTopicConfigs: Boolean = false,
                       syncTopicAcls: Boolean = false)

/**TODO-ssy Didi-Kafka 灾备 ↑ **/
