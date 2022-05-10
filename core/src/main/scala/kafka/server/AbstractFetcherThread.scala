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

import java.nio.ByteBuffer
import java.util
import java.util.{Optional}
import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.BrokerEndPoint
import kafka.utils.{DelayedItem, Pool, ShutdownableThread}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.requests.EpochEndOffset._
import kafka.common.ClientIdAndBroker
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.protocol.Errors

import scala.collection.{mutable, Map, Set}
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import kafka.log.LogAppendInfo
import kafka.server.AbstractFetcherThread.ReplicaFetch
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import org.apache.kafka.common.{InvalidRecordException, TopicPartition}
import org.apache.kafka.common.internals.PartitionStates
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, Records}
import org.apache.kafka.common.requests._

//import java.util.stream.Collectors
import scala.math._

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(name: String,                           //线程名字
                                     clientId: String,
                                     val sourceBroker: BrokerEndPoint,       //源Broker节点信息。源Broker是指此线程要从哪个Broker上读取数据。它决定 Follower 副本从哪个 Broker 拉取 数据，也就是 Leader 副本所在的 Broker 是哪台
                                     failedPartitions: FailedPartitions,     //线程处理过程报错的分区集合
                                     fetchBackOffMs: Int = 0,                //当获取分区数据出错后的等待重试时间
                                     isInterruptible: Boolean = true,
                                     val brokerTopicStats: BrokerTopicStats) //BrokerTopicStats's lifecycle managed by ReplicaManager ; Broker端主题的各类监控指标，常见的有 MessagesInPerSec、 BytesInPerSec 等
  extends ShutdownableThread(name, isInterruptible) {

  //type 的用法是：给指定的类起一个别名
  //后面就可以用 FetchData 来表示 FetchResponse.PartitionData[Records] 类；EpochData 同理。

  //FetchResponse是封装的FETCH请求的响应类，
  // FetchResponse.PartitionData是一个嵌套类，
  // 表示响应中单个分区的拉取信息，包括:
  //  1.对应Leader副本的高水位:highWatermark，
  //  2.分区日志的起始偏移量:logStartOffset，
  //  3.拉取到的消息集合等:records。
  type FetchData = FetchResponse.PartitionData[Records]
  //包含了Follower副本在本地保存的leader epoch 和从Leader副本获取到的leader epoch
  type EpochData = OffsetsForLeaderEpochRequest.PartitionData

  //PartitionStates 类用轮询的方式来处理要读取的多个分区。
  // 简单来说，就是依靠 LinkedHashMap 数据结构来保存所有主题分区。
  // LinkedHashMap 中的元素有明确的迭代顺序，通常就是元素被插入的顺序(LRU结构)。
  private val partitionStates = new PartitionStates[PartitionFetchState]
  protected val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)

  /* callbacks to be defined in subclass */

  //拉取数据的流程，AbstractFetchThread定义了相关的方法：
  // process fetched data
  //处理返回的响应
  protected def processPartitionData(topicPartition: TopicPartition,    // 读取的哪个分区的数据
                                     fetchOffset: Long,                 // 读取到的最新位移值
                                     partitionData: FetchData           // 读取到的分区消息数据
                                    ): Option[LogAppendInfo]            // 写入已读取消息数据前的元数据

  //进行日志截断
  protected def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit

  protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit

  //封装拉取数据的请求
  //为集合中的分区构造ResultWithPartitions对象，
  //     这里的返回结果有两个对象：
  //     1. fetchRequestOpt：要读取的分区核心信息 + 对应的FetchRequest.Builder 对象。
  //                          FetchRequest.Builder 对象是构建 FetchRequest 的核心组件
  //                         这里的核心信息就是指：要读取哪个分区，从哪个位置开始读，最多读多少字节，等等。
  //     2. partitionsWithError：一组出错的分区
  //这个方法中有一个重要的操作：
  //封装拉取请求时，携带了Follower副本的 LogStartOffset 和 LEO 值(对应同步数据的第三步)
  protected def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[ReplicaFetch]]

  protected def latestEpoch(topicPartition: TopicPartition): Option[Int]

  protected def logStartOffset(topicPartition: TopicPartition): Long

  protected def logEndOffset(topicPartition: TopicPartition): Long

  protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch]

  protected def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset]

  protected def fetchFromLeader(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData]

  protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long

  protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long

  protected def isOffsetForLeaderEpochSupported: Boolean

  override def shutdown(): Unit = {
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  //ReplicaFetcherThread 线程对象被创建后会立即启动，
  // 核心逻辑就是在 while 循环中不断执行 AbstractFetcherThread.doWork()方法
  // 将上面buildFetch()、truncate()、processPartitionData()三个方法串联起来，形成一个闭环，并不断地重复执行。
  //  从而实现从Leader副本所在的节点同步消息
  override def doWork(): Unit = {
    //是在异常恢复时尝试进行日志截断的
    maybeTruncate()
    //maybeFetch()方法尝试从Leader副本拉取数据
    //
    //1. 调用子类实现ReplicaFetcherThread.buildFetch()方法构建 Fetch 请求
    //2. 调用AbstractFetcherThread.processFetchRequest()方法发起 Fetch 请求并处理响应数据
    maybeFetch()
  }

  //尝试从Leader副本拉取数据
  //这是builderFetch()唯一被调用的地方
  //这个方法可以划分为关键的三个步骤:
  private def maybeFetch(): Unit = {
    //获取分区状态集合对应的拉取请求的集合
    val fetchRequestOpt = inLock(partitionMapLock) {
      //构建 Fetch 请求
      //第一步：为集合中的分区构造FetchRequest.builder对象，
      //     这里的返回结果有两个对象：
      //     1. fetchRequestOpt：(在fetch request session中的所有副本sessionPartitions: Map[TopicPartition, FetchRequest.PartitionData] ,
      //                           FetchRequest.Builder 对象)
      //                         Builder对象是核心信息，包括：
      //                              1. follower 的 brokerId(等到Leader副本所在的broker上会作为本broker上所有副本的partitionId)
      //                              2. minBytes
      //                              3. 待同步的副本（LinkedHashMap[TopicPartition, FetchRequest.PartitionData]）
      //                         待同步的副本就是指要读取哪个分区，从哪个位置开始读(LEO)、最多读多少字节maxBytes、follower保存的当前leader的epoch 等等。
      //     2. partitionsWithError：一组出错的分区
      val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = buildFetch(partitionStates.partitionStateMap.asScala)

      //第二步：处理出错的分区，处理方式主要是将这个分区加入到LinkedHashMap的末尾，等待后续重试
      handlePartitionsWithErrors(partitionsWithError, "maybeFetch")

      // 如果当前没有可读取的分区，则等待fetchBackOffMs时间等候后续重试
      if (fetchRequestOpt.isEmpty) {
        trace(s"There are no active partitions. Back off for $fetchBackOffMs ms before sending a fetch request")
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }

      fetchRequestOpt
    }
    //第三步：遍历并发送FETCH请求给Leader副本，并处理Response
    fetchRequestOpt.foreach { case ReplicaFetch(sessionPartitions, fetchRequest) =>
      //发送 Fetch 请求给Leader副本并处理响应数据Response
      processFetchRequest(sessionPartitions, fetchRequest)
    }
  }

  // deal with partitions with errors, potentially due to leadership changes
  //处理出错的分区。处理方式主要是将这个分区加入到LinkedHashMap末尾，等待后续重试
  private def handlePartitionsWithErrors(partitions: Iterable[TopicPartition], methodName: String): Unit = {
    if (partitions.nonEmpty) {
      debug(s"Handling errors in $methodName for partitions $partitions")
      delayPartitions(partitions, fetchBackOffMs)
    }
  }

  /**
   * Builds offset for leader epoch requests for partitions that are in the truncating phase based
   * on latest epochs of the future replicas (the one that is fetching)
   */
  private def fetchTruncatingPartitions(): (Map[TopicPartition, EpochData], Set[TopicPartition]) = inLock(partitionMapLock) {
    val partitionsWithEpochs = mutable.Map.empty[TopicPartition, EpochData]
    val partitionsWithoutEpochs = mutable.Set.empty[TopicPartition]

    partitionStates.partitionStateMap.forEach { (tp, state) =>
      if (state.isTruncating) {
        latestEpoch(tp) match {
          case Some(epoch) if isOffsetForLeaderEpochSupported =>
            partitionsWithEpochs += tp -> new EpochData(Optional.of(state.currentLeaderEpoch), epoch)
          case _ =>
            partitionsWithoutEpochs += tp
        }
      }
    }

    (partitionsWithEpochs, partitionsWithoutEpochs)
  }

  private def maybeTruncate(): Unit = {
    //将所有处于截断中状态的分区依据有无Leader Epoch值进行分组
    val (partitionsWithEpochs, partitionsWithoutEpochs) = fetchTruncatingPartitions()
    if (partitionsWithEpochs.nonEmpty) {
      // 对于有Leader Epoch值的分区，将日志截断到Leader Epoch值对应的位移值处
      truncateToEpochEndOffsets(partitionsWithEpochs)
    }
    if (partitionsWithoutEpochs.nonEmpty) {
      // 对于没有Leader Epoch值的分区，将日志截断到高水位值处
      truncateToHighWatermark(partitionsWithoutEpochs)
    }
  }

  // 执行截断到高水位值操作
  //内部调用了实现类：truncate() 方法
  private def doTruncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Boolean = {
    try {
      truncate(topicPartition, truncationState)
      true
    }
    catch {
      case e: KafkaStorageException =>
        error(s"Failed to truncate $topicPartition at offset ${truncationState.offset}", e)
        markPartitionFailed(topicPartition)
        false
      case t: Throwable =>
        error(s"Unexpected error occurred during truncation for $topicPartition "
          + s"at offset ${truncationState.offset}", t)
        markPartitionFailed(topicPartition)
        false
    }
  }

  /**
    * - Build a leader epoch fetch based on partitions that are in the Truncating phase
    * - Send OffsetsForLeaderEpochRequest, retrieving the latest offset for each partition's
    *   leader epoch. This is the offset the follower should truncate to ensure
    *   accurate log replication.
    * - Finally truncate the logs for partitions in the truncating phase and mark the
    *   truncation complete. Do this within a lock to ensure no leadership changes can
    *   occur during truncation.
    */
  private def truncateToEpochEndOffsets(latestEpochsForPartitions: Map[TopicPartition, EpochData]): Unit = {
    val endOffsets = fetchEpochEndOffsets(latestEpochsForPartitions)
    //Ensure we hold a lock during truncation.
    inLock(partitionMapLock) {
      //Check no leadership and no leader epoch changes happened whilst we were unlocked, fetching epochs
      val epochEndOffsets = endOffsets.filter { case (tp, _) =>
        val curPartitionState = partitionStates.stateValue(tp)
        val partitionEpochRequest = latestEpochsForPartitions.getOrElse(tp, {
          throw new IllegalStateException(
            s"Leader replied with partition $tp not requested in OffsetsForLeaderEpoch request")
        })
        val leaderEpochInRequest = partitionEpochRequest.currentLeaderEpoch.get
        curPartitionState != null && leaderEpochInRequest == curPartitionState.currentLeaderEpoch
      }

      val ResultWithPartitions(fetchOffsets, partitionsWithError) = maybeTruncateToEpochEndOffsets(epochEndOffsets, latestEpochsForPartitions)
      handlePartitionsWithErrors(partitionsWithError, "truncateToEpochEndOffsets")
      updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets)
    }
  }

  // Visible for testing
  // 对于没有Leader Epoch值的分区，将日志截断到高水位值处
  private[server] def truncateToHighWatermark(partitions: Set[TopicPartition]): Unit = inLock(partitionMapLock) {
    val fetchOffsets = mutable.HashMap.empty[TopicPartition, OffsetTruncationState]

    // 遍历每个要执行截断操作的分区对象
    for (tp <- partitions) {
      // 获取分区的分区读取状态
      val partitionState = partitionStates.stateValue(tp)
      if (partitionState != null) {
        // 取出高水位值
        val highWatermark = partitionState.fetchOffset
        //封装截断状态
        val truncationState = OffsetTruncationState(highWatermark, truncationCompleted = true)

        info(s"Truncating partition $tp to local high watermark $highWatermark")
        // 执行截断到高水位值
        if (doTruncate(tp, truncationState))
        //保存分区和对应的截取状态
          fetchOffsets.put(tp, truncationState)
      }
    }

    // 更新这组分区的分区读取状态
    updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets)
  }

  private def maybeTruncateToEpochEndOffsets(fetchedEpochs: Map[TopicPartition, EpochEndOffset],
                                             latestEpochsForPartitions: Map[TopicPartition, EpochData]): ResultWithPartitions[Map[TopicPartition, OffsetTruncationState]] = {
    val fetchOffsets = mutable.HashMap.empty[TopicPartition, OffsetTruncationState]
    val partitionsWithError = mutable.HashSet.empty[TopicPartition]

    fetchedEpochs.foreach { case (tp, leaderEpochOffset) =>
      leaderEpochOffset.error match {
        case Errors.NONE =>
          val offsetTruncationState = getOffsetTruncationState(tp, leaderEpochOffset)
          if(doTruncate(tp, offsetTruncationState))
            fetchOffsets.put(tp, offsetTruncationState)

        case Errors.FENCED_LEADER_EPOCH =>
          if (onPartitionFenced(tp, latestEpochsForPartitions.get(tp).flatMap {
            p =>
              if (p.currentLeaderEpoch.isPresent) Some(p.currentLeaderEpoch.get())
              else None
          })) partitionsWithError += tp

        case error =>
          info(s"Retrying leaderEpoch request for partition $tp as the leader reported an error: $error")
          partitionsWithError += tp
      }
    }

    ResultWithPartitions(fetchOffsets, partitionsWithError)
  }

  /**
   * remove the partition if the partition state is NOT updated. Otherwise, keep the partition active.
   * @return true if the epoch in this thread is updated. otherwise, false
   */
  protected def onPartitionFenced(tp: TopicPartition, requestEpoch: Option[Int]): Boolean = inLock(partitionMapLock) {
    Option(partitionStates.stateValue(tp)).exists { currentFetchState =>
      val currentLeaderEpoch = currentFetchState.currentLeaderEpoch
      if (requestEpoch.contains(currentLeaderEpoch)) {
        info(s"Partition $tp has an older epoch ($currentLeaderEpoch) than the current leader. Will await " +
          s"the new LeaderAndIsr state before resuming fetching.")
        markPartitionFailed(tp)
        false
      } else {
        info(s"Partition $tp has an new epoch ($currentLeaderEpoch) than the current leader. retry the partition later")
        true
      }
    }
  }

  //发起 Fetch 请求并处理响应数据
  private def processFetchRequest(sessionPartitions: util.Map[TopicPartition, FetchRequest.PartitionData],
                                  fetchRequest: FetchRequest.Builder): Unit = {
    //定义出错分区的集合
    val partitionsWithError = mutable.Set[TopicPartition]()
    //定义接收响应数据的集合
    var responseData: Map[TopicPartition, FetchData] = Map.empty

    try {
      trace(s"Sending fetch request $fetchRequest")
      //1、调用子类fetchFromLeader()给Leader发送FETCH请求，获取响应数据
      //      这部分往下分析涉及到底层网络组件 NetworkClient， 如有兴趣可参考消费者组协调器定位了解其工作机制，这里不再赘述
      //返回一个 Map[TopicPartition, FetchData]
      //其中FetchData表示响应中单个分区的拉取信息，包括:
      //  1.对应Leader副本的高水位:highWatermark，
      //  2.分区日志的起始偏移量:logStartOffset，
      //  3.拉取到的消息集合等:records。
      responseData = fetchFromLeader(fetchRequest)
    } catch {
      case t: Throwable =>
        if (isRunning) {
          warn(s"Error in response for fetch request $fetchRequest", t)
          inLock(partitionMapLock) {
            partitionsWithError ++= partitionStates.partitionSet.asScala
            // there is an error occurred while fetching partitions, sleep a while
            // note that `ReplicaFetcherThread.handlePartitionsWithError` will also introduce the same delay for every
            // partition with error effectively doubling the delay. It would be good to improve this.
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    //更新请求发送速率指标
    fetcherStats.requestRate.mark()

    //如果 Fetch 响应中有消息数据，即接收到了响应
    // 则调用子类实现方法 processPartitionData() 将消息追加到本地日志
    if (responseData.nonEmpty) {
      // process fetched data
      inLock(partitionMapLock) {
        //遍历响应结果中的分区和分区对应的数据
        responseData.foreach { case (topicPartition, partitionData) =>
          //TODO 为什么要取partitionStates.stateValue(topicPartition)).foreach
          Option(partitionStates.stateValue(topicPartition)).foreach { currentFetchState =>
            // It's possible that a partition is removed and re-added or truncated when there is a pending fetch request.
            // In this case, we only want to process the fetch response if the partition state is ready for fetch and
            // the current offset is the same as the offset requested.
            //获取分区对应的拉取状态
            val fetchPartitionData = sessionPartitions.get(topicPartition)
            // 处理Response的条件：
            //    1. 获取的消息集合的起始偏移量和之前已保存的下一条待写入偏移量相等
            //    2. 当前分区处于可获取状态
            if (fetchPartitionData != null && fetchPartitionData.fetchOffset == currentFetchState.fetchOffset && currentFetchState.isReadyForFetch) {
              //获取请求中携带的Follower副本保存的 leader epoch 值
              val requestEpoch = if (fetchPartitionData.currentLeaderEpoch.isPresent) Some(fetchPartitionData.currentLeaderEpoch.get().toInt) else None
              partitionData.error match {
                // 如果没有错误
                case Errors.NONE =>
                  try {
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                    // 交由子类完成Response的处理
                    // 将消息追加到本地日志
                    val logAppendInfoOpt = processPartitionData(topicPartition, currentFetchState.fetchOffset,
                      partitionData)

                    logAppendInfoOpt.foreach { logAppendInfo =>
                      val validBytes = logAppendInfo.validBytes
                      val nextOffset = if (validBytes > 0) logAppendInfo.lastOffset + 1 else currentFetchState.fetchOffset
                      val lag = Math.max(0L, partitionData.highWatermark - nextOffset)
                      fetcherLagStats.getAndMaybePut(topicPartition).lag = lag

                      // ReplicaDirAlterThread may have removed topicPartition from the partitionStates after processing the partition data
                      if (validBytes > 0 && partitionStates.contains(topicPartition)) {
                        // Update partitionStates only if there is no exception during processPartitionData
                        val newFetchState = PartitionFetchState(nextOffset, Some(lag), currentFetchState.currentLeaderEpoch, state = Fetching)
                        // 将该分区放置在有序Map读取顺序的末尾，保证公平性
                        partitionStates.updateAndMoveToEnd(topicPartition, newFetchState)
                        fetcherStats.byteRate.mark(validBytes)
                      }
                    }
                  } catch {
                    case ime@( _: CorruptRecordException | _: InvalidRecordException) =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread
                      //    down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes
                      //    can cause this), we simply continue and should get fixed in the subsequent fetches
                      error(s"Found invalid messages during fetch for partition $topicPartition " +
                        s"offset ${currentFetchState.fetchOffset}", ime)
                      partitionsWithError += topicPartition
                    case e: KafkaStorageException =>
                      error(s"Error while processing data for partition $topicPartition " +
                        s"at offset ${currentFetchState.fetchOffset}", e)
                      markPartitionFailed(topicPartition)
                    case t: Throwable =>
                      // stop monitoring this partition and add it to the set of failed partitions
                      error(s"Unexpected error occurred while processing data for partition $topicPartition " +
                        s"at offset ${currentFetchState.fetchOffset}", t)
                      markPartitionFailed(topicPartition)
                  }
                // 如果读取位移值越界，通常是因为Leader发生变更
                case Errors.OFFSET_OUT_OF_RANGE =>
                  //调整越界，主要办法是做截断
                  if (handleOutOfRangeError(topicPartition, currentFetchState, requestEpoch))
                    //如果依然不能成功，将该分区添加到出错分区集合
                    partitionsWithError += topicPartition

                //如果Follower本地保存的Leader Epoch值比Leader所在Broker上的Epoch值要新
                case Errors.UNKNOWN_LEADER_EPOCH =>
                  debug(s"Remote broker has a smaller leader epoch for partition $topicPartition than " +
                    s"this replica's current leader epoch of ${currentFetchState.currentLeaderEpoch}.")
                  // 加入到出错分区集合
                  partitionsWithError += topicPartition

                // 如果Follower本地保存的Leader Epoch值比Leader所在Broker上的Epoch值要旧
                case Errors.FENCED_LEADER_EPOCH =>
                  //将该分区标记为失效，从分区拉取状态集合中移除，并加入到失效分区集合
                  if (onPartitionFenced(topicPartition, requestEpoch)) partitionsWithError += topicPartition

                // 如果Leader发生变更
                case Errors.NOT_LEADER_FOR_PARTITION =>
                  debug(s"Remote broker is not the leader for partition $topicPartition, which could indicate " +
                    "that the partition is being moved")
                  // 加入到出错分区列表
                  partitionsWithError += topicPartition

                // 其他错误
                case _ =>
                  error(s"Error for partition $topicPartition at offset ${currentFetchState.fetchOffset}",
                    partitionData.error.exception)
                  // 加入到出错分区集合
                  partitionsWithError += topicPartition
              }
            }
          }
        }
      }
    }

    // 处理出错分区集合，主要就是将该分区放到LinkedHashMap数据结构的末尾
    if (partitionsWithError.nonEmpty) {
      handlePartitionsWithErrors(partitionsWithError, "processFetchRequest")
    }
  }

  def markPartitionsForTruncation(topicPartition: TopicPartition, truncationOffset: Long): Unit = {
    partitionMapLock.lockInterruptibly()
    try {
      Option(partitionStates.stateValue(topicPartition)).foreach { state =>
        val newState = PartitionFetchState(math.min(truncationOffset, state.fetchOffset),
          state.lag, state.currentLeaderEpoch, state.delay, state = Truncating)
        partitionStates.updateAndMoveToEnd(topicPartition, newState)
        partitionMapCond.signalAll()
      }
    } finally partitionMapLock.unlock()
  }

  private def markPartitionFailed(topicPartition: TopicPartition): Unit = {
    partitionMapLock.lock()
    try {
      failedPartitions.add(topicPartition)
      removePartitions(Set(topicPartition))
    } finally partitionMapLock.unlock()
    warn(s"Partition $topicPartition marked as failed")
  }

  def addPartitions(initialFetchStates: Map[TopicPartition, OffsetAndEpoch]): Set[TopicPartition] = {
    partitionMapLock.lockInterruptibly()
    try {
      failedPartitions.removeAll(initialFetchStates.keySet)

      initialFetchStates.foreach { case (tp, initialFetchState) =>
        // We can skip the truncation step iff the leader epoch matches the existing epoch
        val currentState = partitionStates.stateValue(tp)
        val updatedState = if (currentState != null && currentState.currentLeaderEpoch == initialFetchState.leaderEpoch) {
          currentState
        } else if (initialFetchState.offset < 0) {
          fetchOffsetAndTruncate(tp, initialFetchState.leaderEpoch)
        } else {
          PartitionFetchState(initialFetchState.offset, None, initialFetchState.leaderEpoch, state = Truncating)
        }
        partitionStates.updateAndMoveToEnd(tp, updatedState)
      }

      partitionMapCond.signalAll()
      initialFetchStates.keySet
    } finally partitionMapLock.unlock()
  }

  /**
    * Loop through all partitions, updating their fetch offset and maybe marking them as
    * truncation completed if their offsetTruncationState indicates truncation completed
    *
    * @param fetchOffsets the partitions to update fetch offset and maybe mark truncation complete
    */
  private def updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets: Map[TopicPartition, OffsetTruncationState]): Unit = {
    val newStates: Map[TopicPartition, PartitionFetchState] = partitionStates.partitionStateMap.asScala
      .map { case (topicPartition, currentFetchState) =>
        val maybeTruncationComplete = fetchOffsets.get(topicPartition) match {
          case Some(offsetTruncationState) =>
            val state = if (offsetTruncationState.truncationCompleted) Fetching else Truncating
            PartitionFetchState(offsetTruncationState.offset, currentFetchState.lag,
              currentFetchState.currentLeaderEpoch, currentFetchState.delay, state)
          case None => currentFetchState
        }
        (topicPartition, maybeTruncationComplete)
      }
    partitionStates.set(newStates.asJava)
  }

  /**
   * Called from ReplicaFetcherThread and ReplicaAlterLogDirsThread maybeTruncate for each topic
   * partition. Returns truncation offset and whether this is the final offset to truncate to
   *
   * For each topic partition, the offset to truncate to is calculated based on leader's returned
   * epoch and offset:
   *  -- If the leader replied with undefined epoch offset, we must use the high watermark. This can
   *  happen if 1) the leader is still using message format older than KAFKA_0_11_0; 2) the follower
   *  requested leader epoch < the first leader epoch known to the leader.
   *  -- If the leader replied with the valid offset but undefined leader epoch, we truncate to
   *  leader's offset if it is lower than follower's Log End Offset. This may happen if the
   *  leader is on the inter-broker protocol version < KAFKA_2_0_IV0
   *  -- If the leader replied with leader epoch not known to the follower, we truncate to the
   *  end offset of the largest epoch that is smaller than the epoch the leader replied with, and
   *  send OffsetsForLeaderEpochRequest with that leader epoch. In a more rare case, where the
   *  follower was not tracking epochs smaller than the epoch the leader replied with, we
   *  truncate the leader's offset (and do not send any more leader epoch requests).
   *  -- Otherwise, truncate to min(leader's offset, end offset on the follower for epoch that
   *  leader replied with, follower's Log End Offset).
   *
   * @param tp                    Topic partition
   * @param leaderEpochOffset     Epoch end offset received from the leader for this topic partition
   */
  private def getOffsetTruncationState(tp: TopicPartition,
                                       leaderEpochOffset: EpochEndOffset): OffsetTruncationState = inLock(partitionMapLock) {
    if (leaderEpochOffset.endOffset == UNDEFINED_EPOCH_OFFSET) {
      // truncate to initial offset which is the high watermark for follower replica. For
      // future replica, it is either high watermark of the future replica or current
      // replica's truncation offset (when the current replica truncates, it forces future
      // replica's partition state to 'truncating' and sets initial offset to its truncation offset)
      warn(s"Based on replica's leader epoch, leader replied with an unknown offset in $tp. " +
           s"The initial fetch offset ${partitionStates.stateValue(tp).fetchOffset} will be used for truncation.")
      OffsetTruncationState(partitionStates.stateValue(tp).fetchOffset, truncationCompleted = true)
    } else if (leaderEpochOffset.leaderEpoch == UNDEFINED_EPOCH) {
      // either leader or follower or both use inter-broker protocol version < KAFKA_2_0_IV0
      // (version 0 of OffsetForLeaderEpoch request/response)
      warn(s"Leader or replica is on protocol version where leader epoch is not considered in the OffsetsForLeaderEpoch response. " +
           s"The leader's offset ${leaderEpochOffset.endOffset} will be used for truncation in $tp.")
      OffsetTruncationState(min(leaderEpochOffset.endOffset, logEndOffset(tp)), truncationCompleted = true)
    } else {
      val replicaEndOffset = logEndOffset(tp)

      // get (leader epoch, end offset) pair that corresponds to the largest leader epoch
      // less than or equal to the requested epoch.
      endOffsetForEpoch(tp, leaderEpochOffset.leaderEpoch) match {
        case Some(OffsetAndEpoch(followerEndOffset, followerEpoch)) =>
          if (followerEpoch != leaderEpochOffset.leaderEpoch) {
            // the follower does not know about the epoch that leader replied with
            // we truncate to the end offset of the largest epoch that is smaller than the
            // epoch the leader replied with, and send another offset for leader epoch request
            val intermediateOffsetToTruncateTo = min(followerEndOffset, replicaEndOffset)
            info(s"Based on replica's leader epoch, leader replied with epoch ${leaderEpochOffset.leaderEpoch} " +
              s"unknown to the replica for $tp. " +
              s"Will truncate to $intermediateOffsetToTruncateTo and send another leader epoch request to the leader.")
            OffsetTruncationState(intermediateOffsetToTruncateTo, truncationCompleted = false)
          } else {
            val offsetToTruncateTo = min(followerEndOffset, leaderEpochOffset.endOffset)
            OffsetTruncationState(min(offsetToTruncateTo, replicaEndOffset), truncationCompleted = true)
          }
        case None =>
          // This can happen if the follower was not tracking leader epochs at that point (before the
          // upgrade, or if this broker is new). Since the leader replied with epoch <
          // requested epoch from follower, so should be safe to truncate to leader's
          // offset (this is the same behavior as post-KIP-101 and pre-KIP-279)
          warn(s"Based on replica's leader epoch, leader replied with epoch ${leaderEpochOffset.leaderEpoch} " +
            s"below any replica's tracked epochs for $tp. " +
            s"The leader's offset only ${leaderEpochOffset.endOffset} will be used for truncation.")
          OffsetTruncationState(min(leaderEpochOffset.endOffset, replicaEndOffset), truncationCompleted = true)
      }
    }
  }

  /**
   * Handle the out of range error. Return false if
   * 1) the request succeeded or
   * 2) was fenced and this thread haven't received new epoch,
   * which means we need not backoff and retry. True if there was a retriable error.
   */
  private def handleOutOfRangeError(topicPartition: TopicPartition,
                                    fetchState: PartitionFetchState,
                                    requestEpoch: Option[Int]): Boolean = {
    try {
      val newFetchState = fetchOffsetAndTruncate(topicPartition, fetchState.currentLeaderEpoch)
      partitionStates.updateAndMoveToEnd(topicPartition, newFetchState)
      info(s"Current offset ${fetchState.fetchOffset} for partition $topicPartition is " +
        s"out of range, which typically implies a leader change. Reset fetch offset to ${newFetchState.fetchOffset}")
      false
    } catch {
      case _: FencedLeaderEpochException =>
        onPartitionFenced(topicPartition, requestEpoch)

      case e @ (_ : UnknownTopicOrPartitionException |
                _ : UnknownLeaderEpochException |
                _ : NotLeaderForPartitionException) =>
        info(s"Could not fetch offset for $topicPartition due to error: ${e.getMessage}")
        true

      case e: Throwable =>
        error(s"Error getting offset for partition $topicPartition", e)
        true
    }
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
   */
  protected def fetchOffsetAndTruncate(topicPartition: TopicPartition, currentLeaderEpoch: Int): PartitionFetchState = {
    val replicaEndOffset = logEndOffset(topicPartition)

    /**
     * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
     * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
     * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
     * and it may discover that the current leader's end offset is behind its own end offset.
     *
     * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
     *
     * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
     */
    val leaderEndOffset = fetchLatestOffsetFromLeader(topicPartition, currentLeaderEpoch)
    if (leaderEndOffset < replicaEndOffset) {
      warn(s"Reset fetch offset for partition $topicPartition from $replicaEndOffset to current " +
        s"leader's latest offset $leaderEndOffset")
      truncate(topicPartition, OffsetTruncationState(leaderEndOffset, truncationCompleted = true))

      fetcherLagStats.getAndMaybePut(topicPartition).lag = 0
      PartitionFetchState(leaderEndOffset, Some(0), currentLeaderEpoch, state = Fetching)
    } else {
      /**
       * If the leader's log end offset is greater than the follower's log end offset, there are two possibilities:
       * 1. The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
       * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
       * 2. When unclean leader election occurs, it is possible that the old leader's high watermark is greater than
       * the new leader's log end offset. So when the old leader truncates its offset to its high watermark and starts
       * to fetch from the new leader, an OffsetOutOfRangeException will be thrown. After that some more messages are
       * produced to the new leader. While the old leader is trying to handle the OffsetOutOfRangeException and query
       * the log end offset of the new leader, the new leader's log end offset becomes higher than the follower's log end offset.
       *
       * In the first case, the follower's current log end offset is smaller than the leader's log start offset. So the
       * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log
       * start offset.
       * In the second case, the follower should just keep the current log segments and retry the fetch. In the second
       * case, there will be some inconsistency of data between old and new leader. We are not solving it here.
       * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both
       * brokers and producers.
       *
       * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
       * and the current leader's log start offset.
       */
      val leaderStartOffset = fetchEarliestOffsetFromLeader(topicPartition, currentLeaderEpoch)
      warn(s"Reset fetch offset for partition $topicPartition from $replicaEndOffset to current " +
        s"leader's start offset $leaderStartOffset")
      val offsetToFetch = Math.max(leaderStartOffset, replicaEndOffset)
      // Only truncate log when current leader's log start offset is greater than follower's log end offset.
      if (leaderStartOffset > replicaEndOffset)
        truncateFullyAndStartAt(topicPartition, leaderStartOffset)

      val initialLag = leaderEndOffset - offsetToFetch
      fetcherLagStats.getAndMaybePut(topicPartition).lag = initialLag
      PartitionFetchState(offsetToFetch, Some(initialLag), currentLeaderEpoch, state = Fetching)
    }
  }

  //将分区加入到LinkedHashMap末尾，等待后续重试
  def delayPartitions(partitions: Iterable[TopicPartition], delay: Long): Unit = {
    partitionMapLock.lockInterruptibly()
    try {
      for (partition <- partitions) {
        Option(partitionStates.stateValue(partition)).foreach { currentFetchState =>
          if (!currentFetchState.isDelayed) {
            //把给定的分区从数据结构的头部移除，然后放到尾部，从而达到轮询的目的
            partitionStates.updateAndMoveToEnd(partition, PartitionFetchState(currentFetchState.fetchOffset,
              currentFetchState.lag, currentFetchState.currentLeaderEpoch, Some(new DelayedItem(delay)), currentFetchState.state))
          }
        }
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def removePartitions(topicPartitions: Set[TopicPartition]): Unit = {
    partitionMapLock.lockInterruptibly()
    try {
      topicPartitions.foreach { topicPartition =>
        partitionStates.remove(topicPartition)
        fetcherLagStats.unregister(topicPartition)
      }
    } finally partitionMapLock.unlock()
  }

  def partitionCount(): Int = {
    partitionMapLock.lockInterruptibly()
    try partitionStates.size
    finally partitionMapLock.unlock()
  }

  /*** Didi-Kafka 灾备 ↓ ***/
  def allPartitions(): Set[TopicPartition] = inLock(partitionMapLock) {
    partitionStates.partitionStateMap().asScala
      .map { case (topicPartition, _) =>
        topicPartition
      }.toSet
  }

  def delayedPartitions(): Set[TopicPartition] = inLock(partitionMapLock) {
    partitionStates.partitionStateMap().asScala.filter { case (_, currentFetchState) =>
      currentFetchState.isDelayed
    }.map { case (topicPartition, _) =>
      topicPartition
    }.toSet
  }
  /*** Didi-Kafka 灾备 ↑ ***/

  // Visible for testing
  private[server] def fetchState(topicPartition: TopicPartition): Option[PartitionFetchState] = inLock(partitionMapLock) {
    Option(partitionStates.stateValue(topicPartition))
  }

  private[server] def partitionsAndOffsets: Map[TopicPartition, InitialFetchState] = inLock(partitionMapLock) {
    partitionStates.partitionStateMap.asScala.map { case (topicPartition, currentFetchState) =>
      val initialFetchState = InitialFetchState(sourceBroker,
        currentLeaderEpoch = currentFetchState.currentLeaderEpoch,
        initOffset = currentFetchState.fetchOffset)
      topicPartition -> initialFetchState
    }
  }

  protected def toMemoryRecords(records: Records): MemoryRecords = {
    records match {
      case r: MemoryRecords => r
      case r: FileRecords =>
        val buffer = ByteBuffer.allocate(r.sizeInBytes)
        r.readInto(buffer, 0)
        MemoryRecords.readableRecords(buffer)
    }
  }

}

object AbstractFetcherThread {

  case class ReplicaFetch(partitionData: util.Map[TopicPartition, FetchRequest.PartitionData], fetchRequest: FetchRequest.Builder)
  case class ResultWithPartitions[R](result: R, partitionsWithError: Set[TopicPartition])

}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

class FetcherLagMetrics(metricId: ClientIdTopicPartition) extends KafkaMetricsGroup {

  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topicPartition.topic,
    "partition" -> metricId.topicPartition.partition.toString)

  newGauge(FetcherMetrics.ConsumerLag, () => lagVal.get, tags)

  def lag_=(newLag: Long): Unit = {
    lagVal.set(newLag)
  }

  def lag = lagVal.get

  def unregister(): Unit = {
    removeMetric(FetcherMetrics.ConsumerLag, tags)
  }
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: TopicPartition) => new FetcherLagMetrics(ClientIdTopicPartition(metricId.clientId, k))
  val stats = new Pool[TopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getAndMaybePut(topicPartition: TopicPartition): FetcherLagMetrics = {
    stats.getAndMaybePut(topicPartition)
  }

  def unregister(topicPartition: TopicPartition): Unit = {
    val lagMetrics = stats.remove(topicPartition)
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister(): Unit = {
    stats.keys.toBuffer.foreach { key: TopicPartition =>
      unregister(key)
    }
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister(): Unit = {
    removeMetric(FetcherMetrics.RequestsPerSec, tags)
    removeMetric(FetcherMetrics.BytesPerSec, tags)
  }

}

case class ClientIdTopicPartition(clientId: String, topicPartition: TopicPartition) {
  override def toString: String = s"$clientId-$topicPartition"
}

//用来表征副本读取状态
sealed trait ReplicaState
//截断中
case object Truncating extends ReplicaState
//拉取中
case object Fetching extends ReplicaState

object PartitionFetchState {
  def apply(offset: Long, lag: Option[Long], currentLeaderEpoch: Int, state: ReplicaState): PartitionFetchState = {
    PartitionFetchState(offset, lag, currentLeaderEpoch, None, state)
  }
}


/**
 * case class to keep partition offset and its state(truncatingLog, delayed)
 * This represents a partition as being either:
 * (1) Truncating its log, for example having recently become a follower
 * (2) Delayed, for example due to an error, where we subsequently back off a bit
 * (3) ReadyForFetch, the is the active state where the thread is actively fetching data.
 */
//样例类，用来表征分区的读取状态及
//将从leader的哪个offset开始获取数据
// 包含:
// 1. 已拉取的偏移量: fetchOffset，
// 2. 当前leader的epoch: currentLeaderEpoch，
// 3. 副本读取状态: state
// 等
case class PartitionFetchState(fetchOffset: Long,           //已拉取的偏移量，即Follower副本LEO
                               lag: Option[Long],
                               currentLeaderEpoch: Int,     //当前leader的epoch
                               delay: Option[DelayedItem],
                               state: ReplicaState) {       //副本读取状态

  //分区读取状态分为三种：
  //1. 可拉取，表明副本获取线程当前能够读取数据。判断条件是：副本处于Fetching且未被推迟执行
  def isReadyForFetch: Boolean = state == Fetching && !isDelayed

  //副本出于ISR， 条件：没有lag。副本限流，出镜率不高
  def isReplicaInSync: Boolean = lag.isDefined && lag.get <= 0

  //2. 截断中，表明分区副本正在执行截断操作(比如该副本刚刚成为 Follower 副本)。
  //          判断条件是：副本处于Truncating且未被推迟执行
  def isTruncating: Boolean = state == Truncating && !isDelayed

  //3. 被推迟，表明副本获取线程获取数据时出现错误，需要等待一段时间后重试。
  //          判断条件是：存在未过期的延迟任务
  def isDelayed: Boolean = delay.exists(_.getDelay(TimeUnit.MILLISECONDS) > 0)

  override def toString: String = {
    s"FetchState(fetchOffset=$fetchOffset" +
      s", currentLeaderEpoch=$currentLeaderEpoch" +
      s", state=$state" +
      s", lag=$lag" +
      s", delay=${delay.map(_.delayMs).getOrElse(0)}ms" +
      s")"
  }
}

case class OffsetTruncationState(offset: Long, truncationCompleted: Boolean) {

  def this(offset: Long) = this(offset, true)

  override def toString: String = "offset:%d-truncationCompleted:%b".format(offset, truncationCompleted)
}

case class OffsetAndEpoch(offset: Long, leaderEpoch: Int) {
  override def toString: String = {
    s"(offset=$offset, leaderEpoch=$leaderEpoch)"
  }
}
