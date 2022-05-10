package kafka.server

import kafka.cluster.BrokerEndPoint
import kafka.coordinator.group.GroupMetadataManager
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

import scala.collection.{Set, mutable}

/**
 * TODO Didi-Kafka 灾备
 * @author leewei
 * @date 2021/9/28
 *
 * 管理MirrorFetcherThread及Topic同步任务
 */
class MirrorFetcherManager(brokerConfig: KafkaConfig,
                           replicaManager: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           threadNamePrefix: Option[String] = None,
                           quotaManager: ReplicationQuotaManager)
  extends AbstractFetcherManager[MirrorFetcherThread](
    name = "MirrorFetcherManager on broker " + brokerConfig.brokerId,
    clientId = "Mirror",
    numFetchers = brokerConfig.numReplicaFetchers) {

  private val partitionLatestFetchOffsets = new mutable.HashMap[TopicPartition, Long]

  var groupManagerOpt: Option[GroupMetadataManager] = None

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): MirrorFetcherThread = {
    val prefix = threadNamePrefix.map(tp => s"$tp: ").getOrElse("")
    val threadName = s"${prefix}MirrorFetcherThread-$fetcherId-remote-cluster-${sourceBroker.remoteCluster.get}-${sourceBroker.id}"
    val config = sourceBroker.mirrorConfig.getOrElse(throw new ConfigException(s"Can't get remote cluster $sourceBroker configs"))
    new MirrorFetcherThread(threadName,
      fetcherId,
      sourceBroker,
      config,
      failedPartitions,
      partitionLatestFetchOffsets,
      replicaManager,
      groupManagerOpt.getOrElse(throw new ConfigException("Can't get group metadata manager")),
      metrics,
      time,
      quotaManager,
      None,
      brokerConfig.interBrokerProtocolVersion,
      brokerConfig.brokerId,
      brokerConfig.clusterId + "")
  }

  def setGroupMetadataManager(groupManager: GroupMetadataManager): Unit = {
    this.groupManagerOpt = Option(groupManager)
  }

  /** 所有通过MirrorFetcherThread处理的partitions */
  def allFetchedPartitions(): Set[TopicPartition] = {
    fetcherThreadMap.values.flatMap(_.allPartitions()).toSet
  }

  /** 所有通过MirrorFetcherThread延迟处理的partitions */
  def allDelayedPartitions(): Set[TopicPartition] = {
    fetcherThreadMap.values.flatMap(_.delayedPartitions()).toSet
  }

  /** 所有fetch失败的partitions */
  def allFailedPartitions(): Set[TopicPartition] = {
    failedPartitions.listAll()
  }

  /** 所有已通过MirrorFetcherThread处理的partitions */
  def allPartitions(): Set[TopicPartition] = {
    allFetchedPartitions() ++ allFailedPartitions()
  }

  def latestFetchOffset(topicPartition: TopicPartition): Option[Long] = {
    partitionLatestFetchOffsets.get(topicPartition)
  }

  def shutdown(): Unit = {
    info(" shutting down")
    closeAllFetchers()
    info(" shutdown completed")
  }
}