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
 * @author leewei
 * @date 2021/11/23
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

