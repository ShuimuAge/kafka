package kafka.coordinator.mirror

import kafka.metrics.KafkaMetricsGroup
import kafka.server.{AdminManager, KafkaConfig, ReplicaManager}
import kafka.utils.CoreUtils.inLock
import kafka.utils.{Logging, ShutdownableThread}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.common.utils.{Time, Utils}

import java.util.Collections
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import scala.collection.{Map, mutable}
//import scala.collection.JavaConverters._

/**
 * @author leewei
 * @date 2021/10/21
 */
class MirrorCoordinator(config: KafkaConfig,
                        metrics: Metrics,
                        time: Time,
                        replicaManager: ReplicaManager,
                        adminManager: AdminManager,
                        zkClient: KafkaZkClient) extends Logging with KafkaMetricsGroup {

  private val maxWaitMs = 60 * 1000

  private val fetcherManager = replicaManager.mirrorFetcherManager

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* number of partitions for the mirror state topic */
  private val mirrorTopicPartitionCount = getMirrorTopicPartitionCount

  /* partitions of consumer groups that are assigned, using the same loading partition lock */
  private val ownedPartitions = mutable.Set[Int]()

  private val inconsistentTopics = mutable.Set[String]()

  private val isActive = new AtomicBoolean(false)

  private var syncTopicPartitionsThread: PeriodSyncThread = null

  newGauge("InconsistentTopicCount", () => inconsistentTopics.size)

  private class PeriodSyncThread(name: String, period: Long, unit: TimeUnit, doSync: () => Unit) extends
    ShutdownableThread(name) {

    def backoff(): Unit = pause(period, unit)

    override def doWork(): Unit = {
      try {
        doSync()
      } catch {
        case e: Throwable =>
          if (isRunning)
            error("Error due to", e)
      } finally {
        backoff()
      }
    }
  }

  /**
   * Startup logically executed at the same time when the server starts up.
   */
  def startup(): Unit = {
    info("Starting up.")
    // TODO get period by coniig
    syncTopicPartitionsThread = new PeriodSyncThread(" sync-topic-partitions-thread" ,
      10 * 1000, TimeUnit.MILLISECONDS, syncTopicPartitions)
    syncTopicPartitionsThread.start()
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logically executed at the same time when server shuts down .
   * Ordering of actions should be reversed from the startup process .
   */
  def shutdown(): Unit = {
    info("Shutting down.")
    isActive.set(false)
    if (syncTopicPartitionsThread != null)
      syncTopicPartitionsThread.shutdown()
    info("Shutdown complete.")
  }

  def partitionFor(resourceId: String): Int = Utils.abs(resourceId.hashCode) % mirrorTopicPartitionCount

  def isPartitionOwned(partition: Int): Boolean = inLock(partitionLock) {
    ownedPartitions.contains(partition)
  }

  def isLocal( resourceId: String): Boolean = isPartitionOwned(partitionFor(resourceId))
  //def isLocal(resourceId: String): Boolean = true

  /**
   * Load cached state from the given pa rtition.
   *
   * @param mirrorTopicPartitionId The partition we are now leading
   */
  def onElection(mirrorTopicPartitionId: Int): Unit = {
    inLock(partitionLock) {
      ownedPartitions.add(mirrorTopicPartitionId)
    }
  }

  /**
   * Unload cached state for the given partition.
   *
   * @param mirrorTopicPartitionId The partition we are no longer leading
   */
  def onResignation(mirrorTopicPartitionId: Int): Unit = {
    inLock(partitionLock) {
      ownedPartitions.remove(mirrorTopicPartitionId)
    }
  }

  /**
   * Gets the partition count of the mirror state topic from ZooKeeper.
   * If the topic does not exist, the default partition count is returned.
   */
  private def getMirrorTopicPartitionCount: Int = {
    // TODO config.mirrorStateNumPartitions
    zkClient.getTopicPartitionCount(Topic.MIRROR_STATE_TOPIC_NAME).getOrElse(50)
  }

  def syncTopicPartitions(): Unit = {
      val needSyncTopics = mutable.Set[String]()
      // 1.Topic的0分区所在leader负责同步分区
      //获取id为0的partition的leader副本所在的broker就行topic同步
      fetcherManager.allPartitions()
        .filter(_.partition() == 0)
        .map(replicaManager.getPartitionOrExceptionByRemoteTopicPartition(_, expectLeader = true))
        .filter(_.isLeader)
        .foreach(p => needSyncTopics.add(p.topic))

      info( s"UnknownMirrorTopicLocalNames: ${replicaManager.getUnknownMirrorTopicLocalNames()}")
      //2.刷新未知topic, 请求元数据
      replicaManager.getUnknownMirrorTopicLocalNames()
        .filter(topic => isLocal(topic))
        . diff(replicaManager.unknownMirrorTopicLocalNamesNeedMetadata)
        .foreach(replicaManager.unknownMirrorTopicLocalNamesNeedMetadata.add)
      needSyncTopics ++= replicaManager.unknownMirrorTopicLocalNamesNeedMetadata

      val mirrorTopicsByLocal = replicaManager.allMirrorTopicsByLocal()
      val newPartitions = mutable.Set[CreatePartitionsTopic]()
      val newTopics = mutable.HashMap[String, CreatableTopic]()
    //遍历待同步的Topics，用每个本地topic对应的MirrorTopic取代本地topic，再遍历每个MirrorTopic：
      needSyncTopics
        .flatMap(topic => mirrorTopicsByLocal.get(topic))
        .foreach {
          // 若其定义了syncTopicPartitions这个配置
          case mirrorTopic => {
            //取出其对应的mirrorTopic在remoteCluster中的List<PartitionInfo>
            val remotePartitions = replicaManager.remoteClusterMetadata(mirrorTopic.remoteCluster)
              .map(_.partitionsForTopic(mirrorTopic.remoteTopic))
              .getOrElse(Collections.emptyList)
            //该mirrorTopic的partition数
            val remotePartitionCount = remotePartitions.size()
            //本地Topic的partition数
            val partitionCount = replicaManager.metadataCache.getNumberOfPartition(mirrorTopic.localTopic)._2
            debug(s"remote topic ${mirrorTopic.remoteTopic} partition count $remotePartitionCount, " +
            s"local topic ${mirrorTopic.localTopic} partition count $partitionCount ")
            if (remotePartitionCount > partitionCount) {
              inconsistentTopics += mirrorTopic.localTopic
              if (mirrorTopic.syncTopicPartitions) {
                if (partitionCount > 0) {
                  newPartitions += new CreatePartitionsTopic().setName(mirrorTopic.localTopic)
                    .setCount(remotePartitionCount).setAssignments(null)
                } else {
                  //通过源集群获取副本数
                  val replicationFactor = remotePartitions.get(0).replicas( ).length.shortValue()
                  newTopics += mirrorTopic.localTopic -> new CreatableTopic().setName(mirrorTopic.localTopic)
                    .setNumPartitions(remotePartitionCount).setReplicationFactor(replicationFactor)
                }
              } else {
                warn(s"Topic partition inconsistent, remote topic ${mirrorTopic.remoteTopic} partition count $remotePartitionCount, " +
                  s"local topic ${mirrorTopic.localTopic} partition count $partitionCount ")
                }
            }
          }
          case _ => None
        }

    // auto create internal topic __mirror_state
    if (!replicaManager.metadataCache.contains(Topic.MIRROR_STATE_TOPIC_NAME)) {
    val aliveBrokers = replicaManager.metadataCache.getAliveBrokers
    if (aliveBrokers.size < config.mirrorStateTopicReplicationFactor) {
      error(s"Number of alive brokers '${aliveBrokers.size}' does not meet the required replication factor " +
        s"'${config.mirrorStateTopicReplicationFactor}' for the ${Topic.MIRROR_STATE_TOPIC_NAME} topic(configured via " +
        s"'${KafkaConfig.DiDiMirrorStateTopiReplicationFactorProp}'). This error can be ignored if the cluster is starting up " +
        s"and not all brokers are up yet.")
    } else {
          newTopics += Topic.MIRROR_STATE_TOPIC_NAME -> new
              CreatableTopic().setName(Topic.MIRROR_STATE_TOPIC_NAME)
        .setNumPartitions(config.mirrorStateTopicPartitions).setReplicationFactor(config.mirrorStateTopicReplicationFactor)
        }
  }
  
      info(s"to create topic partitions $newPartitions, to create topics $newTopics")
      // 3.create topics
      val includeConfigsAndMetadata = mutable.HashMap[String, CreatableTopicResult]()
      newTopics.keys.foreach { topic =>
        includeConfigsAndMetadata += topic -> new CreatableTopicResult().setName(topic)
      }
      adminManager.createTopics(30000, validateOnly = false, newTopics, includeConfigsAndMetadata, result => {
        result.foreach {
          case (topic, error) =>
            info(s"topic $topic create topic result $error")
        }
      })
      // 4.create partitions
      adminManager.createPartitions(30000, newPartitions.toSeq, validateOnly = false, null, result => {
        result.foreach {
          case (topic, error) => info(s"topic $topic create partition result $error")
        }
      })
    }

}