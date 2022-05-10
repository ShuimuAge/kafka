package kafka.server;

import kafka.api.ApiVersion
import kafka.cluster.BrokerEndPoint
import kafka.coordinator.group.GroupMetadataManager
import kafka.log.LogAppendInfo
import kafka.ssy.datachannel.HAClusterConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

import scala.collection.mutable

/**
 * Didi-Kafka 灾备
 * @author leewei
 * @date 2021/11/23
 *
 * 使用MirrorFetcher拉取源集群__consumer_offsets数据
 * 源集群的__consumer_offsets和目标集群的分区数可能会存在不一致，对于需要同步的组信息以目标集群为主，源集群可能会存在同步多个分区的数据
 * 为了防止group offset出现环路，需要在消息上打入tag信息
 *
 * 1. 获取组offset信息
 * 2. 更新目标组offset信息
 * 3. 以副本方式拉取offset数据
 */
class MirrorGroupThread(name: String,
                        fetcherId: Int,
                        sourceBroker: BrokerEndPoint,
                        config: HAClusterConfig,
                        failedPartitions: FailedPartitions,
                        partitionLatestFetchOffsets: mutable.HashMap[TopicPartition, Long],
                        replicaMgr: ReplicaManager,
                        groupManager: GroupMetadataManager,
                        metrics: Metrics,
                        time: Time,
                        quota: ReplicaQuota,
                        leaderEndpointBlockingSend: Option[BlockingSend] = None,
                        interBrokerProtocolVersion: ApiVersion,
                        brokerId: Int,
                        clusterId: String)
  extends MirrorFetcherThread(name,
    fetcherId,
    sourceBroker,
    config,
    failedPartitions,
    partitionLatestFetchOffsets,
    replicaMgr,
    groupManager,
    metrics,
    time,
    quota,
    leaderEndpointBlockingSend,
    interBrokerProtocolVersion,
    brokerId,
    clusterId) {

  // not use epoch to truncate data, see isOffsetForLeaderEpochSupported
  override protected def latestEpoch(topicPartition: TopicPartition): Option[Int] = {
    Option.empty
  }

  override protected def logStartOffset(topicPartition: TopicPartition): Long = {
    0L
  }

  override protected def logEndOffset(topicPartition: TopicPartition): Long = {
    fetchState(topicPartition).map(_.fetchOffset).getOrElse(0L)
  }

  // not use epoch to truncate data, see isOffsetForLeaderEpochSupported
  override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int):
  Option[OffsetAndEpoch] = {
    Option.empty
  }

  override def processPartitionData(topicPartition: TopicPartition,
                                    fetchOffset: Long,
                                    partitionData: FetchData): Option[LogAppendInfo] = {
    groupManager.doMirrorGroupsAndOffsets(topicPartition, fetchOffset, partitionData, clusterId,
      sourceBroker.remoteCluster.get)
  }

  override def truncate(topicPartition: TopicPartition, offsetTruncationState: OffsetTruncationState): Unit = {
    // do nothing
  }

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    // do nothing
  }
}

