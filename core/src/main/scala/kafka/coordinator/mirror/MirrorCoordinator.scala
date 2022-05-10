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
 * Didi-Kafka 灾备 1
 *
 * @author leewei
 * @date 2021/10/21
 *
 * 定时同步topic配置、分区数量、ACL信息 (过滤掉写权限，只能由MirrorFetcher写入数据)
 * leader负责同步topic、topic配置和acl
 * 1. 远程Topic元数据管理
 * 2. 处理远程Topic leader发生切换
 * 3. 内部Topic __ mirror_ state实现(处理任务分配)
 * 4. 同步Topic配置信息、同步Topic ACL权限信息、同步Topic分区变化
 *
 */
class MirrorCoordinator(config: KafkaConfig,
                        metrics: Metrics,
                        time: Time,
                        replicaManager: ReplicaManager,
                        adminManager: AdminManager,
                        zkClient: KafkaZkClient) extends Logging with KafkaMetricsGroup {

  private val maxWaitMs = 60 * 1000

  //mirror fetcher线程管理器
  private val fetcherManager = replicaManager.mirrorFetcherManager

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* number of partitions for the mirror state topic */
  // __mirror_state 的分区数，默认为50
  private val mirrorTopicPartitionCount = getMirrorTopicPartitionCount

  /* partitions of consumer groups that are assigned, using the same loading partition lock */
  //分配给消费组消费的partitions集合
  /**
   * 在处理 LeaderAndIsrRequest 请求时，对于Topic为 __mirror_state 的分区:
   * 1. 该分区副本成为leader时，将该分区副本添加到 ownedPartitions 中
   * 2. 该分区副本成为follower时，将该分区副本从 ownedPartitions 中移除
   */
  private val ownedPartitions = mutable.Set[Int]()

  private val inconsistentTopics = mutable.Set[String]()

  /** 在MirrorCoordinator.startup()中设置为true，在MirrorCoordinator.shutdown()中设置为false */
  private val isActive = new AtomicBoolean(false)

  /** 在MirrorCoordinator启动时初始化并启动，名为"sync-topic-partitions-thread"，循环执行 syncTopicPartitions() 方法 */
  private var syncTopicPartitionsThread: PeriodSyncThread = null

  newGauge("InconsistentTopicCount", () => inconsistentTopics.size)

  private class PeriodSyncThread(name: String, //线程名
                                 period: Long, //线程循环周期
                                 unit: TimeUnit, //线程循环周期单位
                                 doSync: () => Unit //方法主体，循环调用该方法来进行同步
                                ) extends ShutdownableThread(name) {

    def backoff(): Unit = pause(period, unit)

    override def doWork(): Unit = {
      try {
        doSync()
      } catch {
        case e: Throwable =>
          if (isRunning)
            error("Error due to", e)
      } finally {
        //暂停一个周期单位
        backoff()
      }
    }
  }

  /**
   * Startup logically executed at the same time when the server starts up.
   */
  //启动MirrorCoordinator
  //1. 循环执行syncTopicPartitions()方法
  //2. 将isActive设置为true
  def startup(): Unit = {
    info("Starting up.")
    // TODO get period by config 应该是要做成根据动态配置做循环周期手动修改
    syncTopicPartitionsThread = new PeriodSyncThread("sync-topic-partitions-thread",
      10 * 1000, TimeUnit.MILLISECONDS, syncTopicPartitions)
    /** 以周期period循环执行syncTopicPartitions()方法 **/
    /** 当本broker上存在镜像集群某Topic配置的MirrorTopic的0分区的leader副本时，保证自己这边的Topic存在且partitions数和对方相等 */
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

  //获取该topic在 __mirror_state 中分配的分区：hash(topic) % 50
  def partitionFor(resourceId: String): Int = Utils.abs(resourceId.hashCode) % mirrorTopicPartitionCount

  /** __mirror_state 中 partitionId 分区的leader副本是否由本broker维护 */
  def isPartitionOwned(partition: Int): Boolean = inLock(partitionLock) {
    ownedPartitions.contains(partition)
  }

  /** 该topic在 __mirror_state 中被分配的分区的leader副本是否由本broker维持 */
  def isLocal(resourceId: String): Boolean = isPartitionOwned(partitionFor(resourceId))
  //def isLocal(resourceId: String): Boolean = true

  /**
   * Load cached state from the given partition.
   *
   * @param mirrorTopicPartitionId The partition we are now leading
   */
  /** TODO-ssy 当 __mirror_state 某分区副本在本broker上成为leader副本时，将其放入ownedPartitions中 */
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
  /** TODO-ssy 当 __mirror_state 某分区副本在本broker上成为leader副本时，将分区从 mirrorCoordinator 的 ownedPartitions 中移除 */
  def onResignation(mirrorTopicPartitionId: Int): Unit = {
    inLock(partitionLock) {
      ownedPartitions.remove(mirrorTopicPartitionId)
    }
  }

  /**
   * Gets the partition count of the mirror state topic from ZooKeeper.
   * If the topic does not exist, the default partition count is returned.
   */
  //获取内部主题 __mirror_state 的分区数
  private def getMirrorTopicPartitionCount: Int = {
    // TODO config.mirrorStateNumPartitions
    zkClient.getTopicPartitionCount(Topic.MIRROR_STATE_TOPIC_NAME).getOrElse(50)
  }

  /**
   * TODO-ssy 每一台KafkaServer中MirrorCoordinator循环执行的主方法
   *  当本broker上存在镜像集群某Topic配置的MirrorTopic的0分区leader副本时，保证自己的Topic存在且partitions数和对方相等
   *
   *  1. 若不存在内部主题 __mirror_state 则新建，分区数和副本数都是来自配置信息
   *  2. TODO-ssy 创建本地topic或为本地Topic创建partition来保证和镜像topic对齐
   */
  def syncTopicPartitions(): Unit = {
    //待同步的Topic集合
    val needSyncTopics = mutable.Set[String]()
    //TODO-ssy 1.所有已通过MirrorFetcherThread处理的Topic中，选择0分区的leader副本所在的broker负责对齐该Topic和其镜像Topic的分区数量(其实是作为镜像集群中对应Topic配置的MirrorTopic，需要保证对方的partitions数一致)
    fetcherManager.allPartitions()
      .filter(_.partition() == 0)
      .map(replicaManager.getPartitionOrExceptionByRemoteTopicPartition(_, expectLeader = true))
      .filter(_.isLeader)
      .foreach(p => needSyncTopics.add(p.topic))

    info(s"UnknownMirrorTopicLocalNames: ${replicaManager.getUnknownMirrorTopicLocalNames()}")
    /** TODO-ssy 2.刷新未知topic, 请求元数据 **/
    /**
     * 将 添加镜像Topic时，对在本地元数据中不存在的localTopic 中：
     *  1. 被__mirror_state分配给本broker
     *  2. 不在 unknownMirrorTopicLocalNamesNeedMetadata 中(即未曾被本机处理过)
     * 的localTopic加入到 unknownMirrorTopicLocalNamesNeedMetadata 中，准备进行和镜像Topic的Topic结构对齐
    */
    replicaManager.getUnknownMirrorTopicLocalNames()
      .filter(topic => isLocal(topic))
      .diff(replicaManager.unknownMirrorTopicLocalNamesNeedMetadata)
      .foreach(replicaManager.unknownMirrorTopicLocalNamesNeedMetadata.add)
    //将 unknownMirrorTopicLocalNamesNeedMetadata 中的所有topic添加到待同步topic集合中
    needSyncTopics ++= replicaManager.unknownMirrorTopicLocalNamesNeedMetadata


    // 本地Topic名 -> 镜像Topic 的映射
    val mirrorTopicsByLocal = replicaManager.allMirrorTopicsByLocal()
    // 存储待创建分区
    val newPartitions = mutable.Set[CreatePartitionsTopic]()
    // 存储待创建Topic (Topic名 -> topic详细信息) 映射
    val newTopics = mutable.HashMap[String, CreatableTopic]()
    /** TODO-ssy 遍历待同步的Topics对应的MirrorTopic对象，记录待创建的本地topic或本地Topic的待创建partition来保证和镜像topic对齐 */
    needSyncTopics
      .flatMap(topic => mirrorTopicsByLocal.get(topic)) //获取镜像Topic对象
      //遍历待同步Topic集合中对应的镜像Topic对象
      .foreach {
        case mirrorTopic => {
          //取出其对应的mirrorTopic在remoteCluster中的List<PartitionInfo> (镜像topic-partition的元数据信息列表)
          val remotePartitions = replicaManager.remoteClusterMetadata(mirrorTopic.remoteCluster)
            .map(_.partitionsForTopic(mirrorTopic.remoteTopic))
            .getOrElse(Collections.emptyList)
          //该镜像Topic的partition数
          val remotePartitionCount = remotePartitions.size()
          //本地Topic的partition数
          val partitionCount = replicaManager.metadataCache.getNumberOfPartition(mirrorTopic.localTopic)._2
          debug(s"remote topic ${mirrorTopic.remoteTopic} partition count $remotePartitionCount, " +
            s"local topic ${mirrorTopic.localTopic} partition count $partitionCount ")
          //TODO-ssy 若镜像Topic的partition数 > 本地Topic的partition数  (创建本地topic或为本地Topic创建partition来保证和镜像topic对齐)
          if (remotePartitionCount > partitionCount) {
            //将该镜像Topic的本地Topic名添加到 inconsistentTopics 中
            //inconsistentTopics添加的唯一入口
            inconsistentTopics += mirrorTopic.localTopic
            //若该MirrorTopic对象配置了 syncTopicPartitions 分区同步
            if (mirrorTopic.syncTopicPartitions) {
              //i 若本地Topic的分区数不为空(不知道为-1是什么情况)，则
              if (partitionCount > 0) {
                // 创建一个待创建Partition对象，初始化Topic名(localTopic名)和分区数(该镜像Topic分区数)，加入 newPartitions
                newPartitions += new CreatePartitionsTopic().setName(mirrorTopic.localTopic)
                  .setCount(remotePartitionCount).setAssignments(null)                        //TODO-ssy 为啥是null？
              }
              //ii 若本地Topic的分区数为空则创建新Topic
              else {
                //获取镜像Topic的0分区的副本数(即该topic的分区副本数)
                val replicationFactor = remotePartitions.get(0).replicas().length.shortValue()
                // 创建一个待创建Topic对象，初始化Topic名、分区数及分区副本数，加入 newPartitions
                //TODO-ssy 什么情况下会需要创建Topic呀？
                newTopics += mirrorTopic.localTopic -> new CreatableTopic().setName(mirrorTopic.localTopic)
                  .setNumPartitions(remotePartitionCount).setReplicationFactor(replicationFactor)
              }
            }
            //若该MirrorTopic对象没有配置 syncTopicPartitions 分区同步，返回
            else {
              warn(s"Topic partition inconsistent, remote topic ${mirrorTopic.remoteTopic} partition count $remotePartitionCount, " +
                s"local topic ${mirrorTopic.localTopic} partition count $partitionCount ")
            }
          }
        }
        //没获取到，返回None
        case _ => None
      }

    // auto create internal topic __mirror_state
    /** 若本地replicaManager中缓存的元数据中不存在内部分区 __mirror_state 的元数据则创建内部主题 __mirror_state */
    if (!replicaManager.metadataCache.contains(Topic.MIRROR_STATE_TOPIC_NAME)) {
      // 获取alive brokers
      val aliveBrokers = replicaManager.metadataCache.getAliveBrokers
      //若alive brokers数小于给主题 __mirror_state 配置的副本数，报异常
      if (aliveBrokers.size < config.mirrorStateTopicReplicationFactor) {
        error(s"Number of alive brokers '${aliveBrokers.size}' does not meet the required replication factor " +
          s"'${config.mirrorStateTopicReplicationFactor}' for the ${Topic.MIRROR_STATE_TOPIC_NAME} topic(configured via " +
          s"'${KafkaConfig.DiDiMirrorStateTopicReplicationFactorProp}'). This error can be ignored if the cluster is starting up " +
          s"and not all brokers are up yet.")
      }
      //若无异常
      else {
        // 创建内部主题 __mirror_state ，分区数和副本数都是来自配置信息
        newTopics += Topic.MIRROR_STATE_TOPIC_NAME -> new CreatableTopic().setName(Topic.MIRROR_STATE_TOPIC_NAME)
          .setNumPartitions(config.mirrorStateTopicPartitions).setReplicationFactor(config.mirrorStateTopicReplicationFactor)
      }
    }

    /** TODO-ssy 创建本地topic或为本地Topic创建partition来保证和镜像topic对齐 */
    info(s"to create topic partitions $newPartitions, to create topics $newTopics")
    /** TODO-ssy 3.create topics */
    val includeConfigsAndMetadata = mutable.HashMap[String, CreatableTopicResult]()
    newTopics.keys.foreach { topic =>
      includeConfigsAndMetadata += topic -> new CreatableTopicResult().setName(topic)
    }
    //对newTopics里的所有Topic进行创建
    adminManager.createTopics(30000, validateOnly = false, newTopics, includeConfigsAndMetadata, result => {
      result.foreach {
        case (topic, error) =>
          info(s"topic $topic create topic result $error")
      }
    })
    /** TODO-ssy 4.create partitions */
    /** 对镜像Topic中Partition数大于自己的本地Topic，补加Partition以和remoteTopic对齐 */
    adminManager.createPartitions(30000, newPartitions.toSeq, validateOnly = false, null, result => {
      result.foreach {
        case (topic, error) => info(s"topic $topic create partition result $error")
      }
    })
  }

}