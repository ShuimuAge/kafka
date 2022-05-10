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

import java.util.Optional

import kafka.api._
import kafka.cluster.BrokerEndPoint
import kafka.log.LogAppendInfo
import kafka.server.AbstractFetcherThread.ReplicaFetch
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{LogContext, Time}

import scala.collection.JavaConverters._
import scala.collection.{mutable, Map}

class ReplicaFetcherThread(name: String,
                           fetcherId: Int,
                           sourceBroker: BrokerEndPoint,
                           brokerConfig: KafkaConfig,
                           failedPartitions: FailedPartitions,
                           replicaMgr: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           quota: ReplicaQuota,
                           leaderEndpointBlockingSend: Option[BlockingSend] = None)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                failedPartitions,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                replicaMgr.brokerTopicStats) {

  private val replicaId = brokerConfig.brokerId
  private val logContext = new LogContext(s"[ReplicaFetcher replicaId=$replicaId, leaderId=${sourceBroker.id}, " +
    s"fetcherId=$fetcherId] ")
  this.logIdent = logContext.logPrefix

  private val leaderEndpoint = leaderEndpointBlockingSend.getOrElse(
    new ReplicaFetcherBlockingSend(sourceBroker, brokerConfig, metrics, time, fetcherId,
      s"broker-$replicaId-fetcher-$fetcherId", logContext))

  // Visible for testing
  private[server] val fetchRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_3_IV1) 11
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV2) 10
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1) 8
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_1_1_IV0) 7
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV1) 5
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
    else 0

  // Visible for testing
  private[server] val offsetForLeaderEpochRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_3_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV0) 1
    else 0

  // Visible for testing
  private[server] val listOffsetRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_2_IV1) 5
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_1_IV1) 4
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_2_0_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2) 1
    else 0

  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  private val minBytes = brokerConfig.replicaFetchMinBytes
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes
  private val brokerSupportsLeaderEpochRequest = brokerConfig.interBrokerProtocolVersion >= KAFKA_0_11_0_IV2
  val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)

  override protected def latestEpoch(topicPartition: TopicPartition): Option[Int] = {
    replicaMgr.localLogOrException(topicPartition).latestEpoch
  }

  override protected def logStartOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logStartOffset
  }

  override protected def logEndOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logEndOffset
  }

  override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch] = {
    replicaMgr.localLogOrException(topicPartition).endOffsetForEpoch(epoch)
  }

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown) {
      // This is thread-safe, so we don't expect any exceptions, but catch and log any errors
      // to avoid failing the caller, especially during shutdown. We will attempt to close
      // leaderEndpoint after the thread terminates.
      try {
        leaderEndpoint.initiateClose()
      } catch {
        case t: Throwable =>
          error(s"Failed to initiate shutdown of leader endpoint $leaderEndpoint after initiating replica fetcher thread shutdown", t)
      }
    }
    justShutdown
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    // We don't expect any exceptions here, but catch and log any errors to avoid failing the caller,
    // especially during shutdown. It is safe to catch the exception here without causing correctness
    // issue because we are going to shutdown the thread and will not re-use the leaderEndpoint anyway.
    try {
      leaderEndpoint.close()
    } catch {
      case t: Throwable =>
        error(s"Failed to close leader endpoint $leaderEndpoint after shutting down replica fetcher thread", t)
    }
  }

  // process fetched data
  // 用于处理指定分区从Leader副本所在节点返回的响应，
  // 将获取的消息写入本地存储，并返回写入消息的元数据
  //这里有两个个重要的操作：
  //1. 写入消息，更新 Follower 副本的 LEO(对应同步数据的第八步)
  //2. 更新 Follower 副本本地的 HW 值(对应同步数据的第九步)
  override def processPartitionData(topicPartition: TopicPartition,              // 读取哪个分区的数据
                                    fetchOffset: Long,                           // 读取到的最新位移值
                                    partitionData: FetchData                     // 读取到的分区消息数据
                                   ): Option[LogAppendInfo] = {
    val partition = replicaMgr.nonOfflinePartition(topicPartition).get
    val log = partition.localLogOrException
    val records = toMemoryRecords(partitionData.records)

    maybeWarnIfOversizedRecords(records, topicPartition)

    if (fetchOffset != log.logEndOffset)
      throw new IllegalStateException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, log.logEndOffset))

    if (isTraceEnabled)
      trace("Follower has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
        .format(log.logEndOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))

    // Append the leader's messages to the log
    //调用 Partition.appendRecordsToFollowerOrFutureReplica()
    // 方法将消息数据追加到本地，这部分主要是日志文件的写操作，
    // 如有兴趣可参考Kafka 服务端对客户端的 Produce 请求处理，
    // 至此就完成了主从副本消息数据的同步
    val logAppendInfo = partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false)

    if (isTraceEnabled)
      trace("Follower has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(log.logEndOffset, records.sizeInBytes, topicPartition))
    val leaderLogStartOffset = partitionData.logStartOffset

    // For the follower replica, we do not need to keep its segment base offset and physical position.
    // These values will be computed upon becoming leader or handling a preferred read replica fetch.
    // 消息写入完成后，需要把 Fetch 响应中的 HW 取出来，尝试更新本地日志的 HW，
    // 这部分调用 Log.updateHighWatermark() 方法实现
    //TODO-ssy 根据从leader读取到的LEO更新Follower的HW
    val followerHighWatermark = log.updateHighWatermark(partitionData.highWatermark)
    log.maybeIncrementLogStartOffset(leaderLogStartOffset)
    if (isTraceEnabled)
      trace(s"Follower set replica high watermark for partition $topicPartition to $followerHighWatermark")

    // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
    // traffic doesn't exceed quota.
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)

    if (partition.isReassigning && partition.isAddingLocalReplica)
      brokerTopicStats.updateReassignmentBytesIn(records.sizeInBytes)

    brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)

    logAppendInfo
  }

  def maybeWarnIfOversizedRecords(records: MemoryRecords, topicPartition: TopicPartition): Unit = {
    // oversized messages don't cause replication to fail from fetch request version 3 (KIP-74)
    if (fetchRequestVersion <= 2 && records.sizeInBytes > 0 && records.validBytes <= 0)
      error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicPartition. " +
        "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
        "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
        "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
  }


  override protected def fetchFromLeader(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData] = {
    try {
      //给Leader发送FETCH请求，获取响应数据
      val clientResponse = leaderEndpoint.sendRequest(fetchRequest)
      val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse[Records]]
      if (!fetchSessionHandler.handleResponse(fetchResponse)) {
        Map.empty
      } else {
        fetchResponse.responseData.asScala
      }
    } catch {
      case t: Throwable =>
        fetchSessionHandler.handleError(t)
        throw t
    }
  }

  override protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    fetchOffsetFromLeader(topicPartition, currentLeaderEpoch, ListOffsetRequest.EARLIEST_TIMESTAMP)
  }

  override protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    fetchOffsetFromLeader(topicPartition, currentLeaderEpoch, ListOffsetRequest.LATEST_TIMESTAMP)
  }

  private def fetchOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int, earliestOrLatest: Long): Long = {
    val requestPartitionData = new ListOffsetRequest.PartitionData(earliestOrLatest,
      Optional.of[Integer](currentLeaderEpoch))
    val requestPartitions = Map(topicPartition -> requestPartitionData)
    val requestBuilder = ListOffsetRequest.Builder.forReplica(listOffsetRequestVersion, replicaId)
      .setTargetTimes(requestPartitions.asJava)

    val clientResponse = leaderEndpoint.sendRequest(requestBuilder)
    val response = clientResponse.responseBody.asInstanceOf[ListOffsetResponse]

    val responsePartitionData = response.responseData.get(topicPartition)
    responsePartitionData.error match {
      case Errors.NONE =>
        if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2)
          responsePartitionData.offset
        else
          responsePartitionData.offsets.get(0)
      case error => throw error.exception
    }
  }

  //封装 Fetch 请求
  //为指定分区集合构建对应的FetchRequest.Builder 对象，而该对象是构建 FetchRequest 的核心组件
  //这个方法中有一个重要的操作：
  //封装拉取请求时，携带了Follower副本的 LogStartOffset 和 LEO 值(对应同步数据的第三步)
  override def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[ReplicaFetch]] = {
    val partitionsWithError = mutable.Set[TopicPartition]()

    //核心是一个LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val builder = fetchSessionHandler.newBuilder(partitionMap.size, false)
    //遍历提出Fetch请求的副本
    partitionMap.foreach { case (topicPartition, fetchState) =>
      // We will not include a replica in the fetch request if it should be throttled.
      //如果分区的状态是可拉取的，且该分区未对follower限流
      if (fetchState.isReadyForFetch && !shouldFollowerThrottle(quota, fetchState, topicPartition)) {
        try {
          //获取本地Follower副本保存的分区日志的logStartOffset
          val logStartOffset = this.logStartOffset(topicPartition)
          //用Follower副本的 LEO 值(fetchState.fetchOffset) 和 LogStartOffset 等信息封装拉取请求
          builder.add(topicPartition, new FetchRequest.PartitionData(
            fetchState.fetchOffset, logStartOffset, fetchSize, Optional.of(fetchState.currentLeaderEpoch)))
        } catch {
          case _: KafkaStorageException =>
            // The replica has already been marked offline due to log directory failure and the original failure should have already been logged.
            // This partition should be removed from ReplicaFetcherThread soon by ReplicaManager.handleLogDirFailure()
            partitionsWithError += topicPartition
        }
      }
    }

    //fetchData是一个FetchSessionHandler.FetchRequestData
    //主要包括：
    //1. 待传送的发起Fetch请求的副本：          toSend: Map[TopicPartition, FetchRequest.PartitionData]
    //2. 在fetch request session中的所有副本： sessionPartitions: Map[TopicPartition, FetchRequest.PartitionData]
    val fetchData = builder.build()
    val fetchRequestOpt = if (fetchData.sessionPartitions.isEmpty && fetchData.toForget.isEmpty) {
      None
    } else {
      //正常情况下，封装Fetch请求，包括：
      //1. 本brokerId，等到Leader副本所在的broker上会作为本broker上所有副本的partitionId
      //2. minBytes
      //3. 待同步的副本（LinkedHashMap[TopicPartition, FetchRequest.PartitionData]）
      val requestBuilder = FetchRequest.Builder
        .forReplica(fetchRequestVersion, replicaId, maxWait, minBytes, fetchData.toSend)
        .setMaxBytes(maxBytes)
        .toForget(fetchData.toForget)
        .metadata(fetchData.metadata)
      Some(ReplicaFetch(fetchData.sessionPartitions(), requestBuilder))
    }

    ResultWithPartitions(fetchRequestOpt, partitionsWithError)
  }

  /**
   * Truncate the log for each partition's epoch based on leader's returned epoch and offset.
   * The logic for finding the truncation offset is implemented in AbstractFetcherThread.getOffsetTruncationState
   */
  //进行日志截断
  //用于将指定分区的日志截断到指定的偏移量
  //这个方法内部依次调用了：
  // Partition.truncateTo -> LogManager.truncateTo -> Log.truncateTo -> LogSegment.truncateTo 进行日志截断操作
  override def truncate(tp: TopicPartition, offsetTruncationState: OffsetTruncationState): Unit = {
    val partition = replicaMgr.nonOfflinePartition(tp).get
    val log = partition.localLogOrException

    partition.truncateTo(offsetTruncationState.offset, isFuture = false)

    if (offsetTruncationState.offset < log.highWatermark)
      warn(s"Truncating $tp to offset ${offsetTruncationState.offset} below high watermark " +
        s"${log.highWatermark}")

    // mark the future replica for truncation only when we do last truncation
    if (offsetTruncationState.truncationCompleted)
      replicaMgr.replicaAlterLogDirsManager.markPartitionsForTruncation(brokerConfig.brokerId, tp,
        offsetTruncationState.offset)
  }

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.nonOfflinePartition(topicPartition).get
    partition.truncateFullyAndStartAt(offset, isFuture = false)
  }

  override def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {

    if (partitions.isEmpty) {
      debug("Skipping leaderEpoch request since all partitions do not have an epoch")
      return Map.empty
    }

    val epochRequest = OffsetsForLeaderEpochRequest.Builder.forFollower(offsetForLeaderEpochRequestVersion, partitions.asJava, brokerConfig.brokerId)
    debug(s"Sending offset for leader epoch request $epochRequest")

    try {
      val response = leaderEndpoint.sendRequest(epochRequest)
      val responseBody = response.responseBody.asInstanceOf[OffsetsForLeaderEpochResponse]
      debug(s"Received leaderEpoch response $response")
      responseBody.responses.asScala
    } catch {
      case t: Throwable =>
        warn(s"Error when sending leader epoch request for $partitions", t)

        // if we get any unexpected exception, mark all partitions with an error
        val error = Errors.forException(t)
        partitions.map { case (tp, _) =>
          tp -> new EpochEndOffset(error, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
        }
    }
  }

  override def isOffsetForLeaderEpochSupported: Boolean = brokerSupportsLeaderEpochRequest


  /**
   *  To avoid ISR thrashing, we only throttle a replica on the follower if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  private def shouldFollowerThrottle(quota: ReplicaQuota, fetchState: PartitionFetchState, topicPartition: TopicPartition): Boolean = {
    !fetchState.isReplicaInSync && quota.isThrottled(topicPartition) && quota.isQuotaExceeded
  }

}
