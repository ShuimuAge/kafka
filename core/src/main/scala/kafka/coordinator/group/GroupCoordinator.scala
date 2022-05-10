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
package kafka.coordinator.group

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.OffsetAndMetadata
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, immutable}
import scala.math.max

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 * <p>
 * <b>Delayed operation locking notes:</b>
 * Delayed operations in GroupCoordinator use `group` as the delayed operation
 * lock. ReplicaManager.appendRecords may be invoked while holding the group lock
 * used by its callback.  The delayed callback may acquire the group lock
 * since the delayed operation is completed only if the group lock can be acquired.
 */
//kafka服务端中管理消费组的组件，负责与客户端通信，每个 Broker 都会启动一个该服务
//消费者组会通过 __consumer_offsets的分区数量取模的方式确定选择哪个Broker的GroupCoordinator
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time,
                       metrics: Metrics) extends Logging {
  import GroupCoordinator._

  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = SyncGroupResult => Unit

  /* setup metrics */
  val offsetDeletionSensor = metrics.sensor("OffsetDeletions")

  offsetDeletionSensor.add(new Meter(
    metrics.metricName("offset-deletion-rate",
      "group-coordinator-metrics",
      "The rate of administrative deleted offsets"),
    metrics.metricName("offset-deletion-count",
      "group-coordinator-metrics",
      "The total number of administrative deleted offsets")))

  val groupCompletedRebalanceSensor = metrics.sensor("CompletedRebalances")

  groupCompletedRebalanceSensor.add(new Meter(
    metrics.metricName("group-completed-rebalance-rate",
      "group-coordinator-metrics",
      "The rate of completed rebalance"),
    metrics.metricName("group-completed-rebalance-count",
      "group-coordinator-metrics",
      "The total number of completed rebalance")))

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  //启动 groupCoordinator
  //实际上只是把一个标志变量值 isActive 设置为 true，
  // 并且启动了一个后台线程来删除过期的 group metadata
  def startup(enableMetadataExpiration: Boolean = true): Unit = {
    info("Starting up.")
    //启动了一个后台线程来删除过期的 group metadata
    groupManager.startup(enableMetadataExpiration)
    //把标志变量值 isActive 设置为 true
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown(): Unit = {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  //JoinGroupRequest主要处理逻辑
  def handleJoinGroup(groupId: String,                            //消费者组名称
                      memberId: String,                           //成员id
                      groupInstanceId: Option[String],            //组实例ID，用于标识静态成员
                      requireKnownMemberId: Boolean,              //是否要求成员ID不为空
                      clientId: String,                           //client.id值
                      clientHost: String,                         //消费者程序主机名
                      rebalanceTimeoutMs: Int,                    //rebalance超时时间
                      sessionTimeoutMs: Int,                      //会话超时时间
                      protocolType: String,                       //协议类型，普通消费者固定是consumer
                      protocols: List[(String, Array[Byte])],     //分区分配策略集合
                      responseCallback: JoinCallback              //回调函数
                     ): Unit = {
    //验证组状态的合法性
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      responseCallback(JoinGroupResult(memberId, error))
      return
    }

    //若会话超时时间如果设置不合适，即<6秒 或者 > 1800秒，则返回一个INVALID_SESSION_TIMEOUT异常响应
    if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
      sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(JoinGroupResult(memberId, Errors.INVALID_SESSION_TIMEOUT))
    }
    //TODO-ssy 主要动作入口
    else {
      //判断请求中是否存在memberId (如果是新创建的消费者，发送请求时是没有memberId的)
      val isUnknownMember = memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID
      //根据消费者组id，获取组元数据对象
      //判断内存中是否存在group
      groupManager.getGroup(groupId) match {
        //如果内存中没有获取到group元数据对象
        case None =>
          // only try to create the group if the group is UNKNOWN AND
          // the member id is UNKNOWN, if member is specified but group does not
          // exist we should reject the request.
          //当group和memberId都不存在，表示这是该消费组中第一个Consumer第一次进行joinGroup，
          // 此时需要添加group及初始化group的状态等信息
          if (isUnknownMember) {
            //则会根据消费者组id创建一个元数据对象，并交给GroupMetadataManager管理
            val group = groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
            //为空memberId成员执行加入组操作
            //生成memberId：memberId = clientId + "-" + UUID.randomUUID().toString
            doUnknownJoinGroup(group, groupInstanceId, requireKnownMemberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          } else {
            //如果group不存在但消费者不是首次加入，则封装UNKNOWN_MEMBER_ID异常并调用回调函数返回
            responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
          }
        //如果内存中存在group元数据对象
        case Some(group) =>
          group.inLock {
            //如果满足以下条件之一，则将该消费者从组中移除，返回GROUP_MAX_SIZE_REACHED异常信息
            // 1.该消费者组已超额，且组中包含该消费者成员信息，且该成员不是正在等待加入组(由于消费组已超额，需要阻挡还没加入的消费者进入)
            //   消费者数量由group.max.size参数配置，默认为Int.MaxValue
            // 2.是新加入的消费者，且消费者组已满员
            if ((groupIsOverCapacity(group)
                  && group.has(memberId) && !group.get(memberId).isAwaitingJoin) // oversized group, need to shed members that haven't joined yet
                || (isUnknownMember && group.size >= groupConfig.groupMaxSize)) {
              //将该消费者从组中移除
              group.remove(memberId)
              group.removeStaticMember(groupInstanceId)
              //返回GROUP_MAX_SIZE_REACHED错误
              responseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.GROUP_MAX_SIZE_REACHED))
            }
            //若group存在，但memberId不存在
            else if (isUnknownMember) {
              //为空memberId成员执行加入组操作
              //生成memberId：memberId = clientId + "-" + UUID.randomUUID().toString
              //主要工作：
              //   如果申请加入组的成员 memberId 为空，服务端会先生成一个 memberId，
              //   然后将该请求 "打回去" ，携带生成的 memberId 和 MEMBER_ID_REQUIRED 异常信息。
              //   当客户端收到包含该异常信息的响应，会根据返回的 memberId 更新自身的信息，
              //   然后携带 memberId ，重新发送 JoinGroupRequest，之后就会调用 doJoinGroup 方法了
              doUnknownJoinGroup(group, groupInstanceId, requireKnownMemberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            }
            //若group和mamberId都存在
            else {
              //为非空memberId成员执行加入组操作
              doJoinGroup(group, memberId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            }

            // attempt to complete JoinGroup
            //如果消费者组正处于PreparingRebalance状态
            if (group.is(PreparingRebalance)) {
              //尝试执行延时任务
              joinPurgatory.checkAndComplete(GroupKey(group.groupId))
            }
          }
        }
      }
    }

  //为空memberId成员执行加入组操作
  private def doUnknownJoinGroup(group: GroupMetadata,                    //消费者组元数据
                                 groupInstanceId: Option[String],
                                 requireKnownMemberId: Boolean,
                                 clientId: String,                       //client.id
                                 clientHost: String,                     //消费者程序主机名
                                 rebalanceTimeoutMs: Int,                //Rebalance超时时间
                                 sessionTimeoutMs: Int,                  //会话超时时间
                                 protocolType: String,                   //协议类型
                                 protocols: List[(String, Array[Byte])], //分配分配策略列表
                                 responseCallback: JoinCallback          //回调函数
                                ): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        //如果消费者组状态为Dead，封装COORDINATOR_NOT_AVAILABLE异常并调用回调函数返回
        responseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.COORDINATOR_NOT_AVAILABLE))
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {
        //如果成员配置的协议类型/分区消费分配策略与消费者组的不匹配，封装INCONSISTENT_GROUP_PROTOCOL异常并调用回调函数返回
        //这里需要注意一点：新加入成员的设置的分区分配策略，必须至少有一个策略是组内所有成员都支持的，因为消费者组选举分区分配策略时
        //第一步就是要获取所有成员都支持的分区分配策略，否则无法选举
        responseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.INCONSISTENT_GROUP_PROTOCOL))
      }
      //TODO-ssy 主要动作入口
      //首先生成memberId,判断缓存中有没有groupInstanceId，如果没有的话则返回首先生成memberId，如果存在groupInstanceId则
      // 直接调用addMemberAndRebalance
      else {
        //给消费者成员生成一个memberId：memberId = clientId + "-" + UUID.randomUUID().toString
        val newMemberId = group.generateMemberId(clientId, groupInstanceId)
        //如果是已注册的静态成员（暂时不考虑）
        if (group.hasStaticMember(groupInstanceId)) {
          updateStaticMemberAndRebalance(group, newMemberId, groupInstanceId, protocols, responseCallback)
        }
        //若没有配置静态成员
        //如果要求memberId(默认为true)
        //会返回带memberId的response
        else if (requireKnownMemberId) {
            // If member id required (dynamic membership), register the member in the pending member list
            // and send back a response to call for another join group request with allocated member id.
          debug(s"Dynamic member with unknown member id joins group ${group.groupId} in " +
              s"${group.currentState} state. Created a new member id $newMemberId and request the member to rejoin with this id.")
          //将该成员加入到待决成员列表
          group.addPendingMember(newMemberId)
          addPendingMemberExpiration(group, newMemberId, sessionTimeoutMs)
          //封装MEMBER_ID_REQUIRED异常并携带新生成的memberId，调用回调函数返回
          responseCallback(JoinGroupResult(newMemberId, Errors.MEMBER_ID_REQUIRED))
        }
        //如果不要求需要memberId
        else {
          info(s"${if (groupInstanceId.isDefined) "Static" else "Dynamic"} Member with unknown member id joins group ${group.groupId} in " +
            s"${group.currentState} state. Created a new member id $newMemberId for this member and add to the group.")
          //向内存中添加member，进行rebalance处理
          addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, newMemberId, groupInstanceId,
            clientId, clientHost, protocolType, protocols, group, responseCallback)
        }
      }
    }
  }

  //为配置了 memberId 的成员执行加入组逻辑
  //该方法可以分为两个阶段：
  // 1.验证组信息及成员信息，处理待决定成员的入组
  // 2.处理非待决成员的入组
  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          groupInstanceId: Option[String],             //静态成员Id，是用户指定的一个consumer成员ID。每个消费者组下这些ID必须是唯一的。一旦设置了该ID，该消费者就会被视为是一个静态成员
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback): Unit = {
    group.inLock {
      //第一阶段: 验证组信息及成员信息，处理待决定成员的入组
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        //如果消费者组状态为Dead，封装COORDINATOR_NOT_AVAILABLE异常并调用回调函数返回
        responseCallback(JoinGroupResult(memberId, Errors.COORDINATOR_NOT_AVAILABLE))
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {
        //如果成员配置的协议类型/分区消费分配策略与消费者组的不匹配，封装INCONSISTENT_GROUP_PROTOCOL异常并调用回调函数返回
        responseCallback(JoinGroupResult(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
      }
      //如果是待决成员，TODO-ssy 即当前member在进行rejoin处理中
      else if (group.isPendingMember(memberId)) {
        // A rejoining pending member will be accepted. Note that pending member will never be a static member.
        //如果是待决成员，由于这次分配了成员ID，故允许加入组
        //是pending状态的member，表示其为动态member，
        // 而此类型的member不允许设置groupInstanceId，只有静态成员才能设置
        if (groupInstanceId.isDefined) {
          //如果定义了group.instance.id参数，报告异常
          throw new IllegalStateException(s"the static member $groupInstanceId was not expected to be assigned " +
            s"into pending member bucket with member id $memberId")
        }
        //如果没有定义group.instance.id参数
        //向内存中添加member信息并进行rebalance
        else {
          debug(s"Dynamic Member with specific member id $memberId joins group ${group.groupId} in " +
            s"${group.currentState} state. Adding to the group now.")
          //添加成员入组，如果还未选出Leader成员，则设置当前成员为Leader
          //进行rebalance
          addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, memberId, groupInstanceId,
            clientId, clientHost, protocolType, protocols, group, responseCallback)
        }
      //第二阶段： 处理非待决成员的入组
      } else {
        val groupInstanceIdNotFound = groupInstanceId.isDefined && !group.hasStaticMember(groupInstanceId)
        //若该member是静态member但存储的memberId与当前的memberId不一致
        if (group.isStaticMemberFenced(memberId, groupInstanceId, "join-group")) {
          // given member id doesn't match with the groupInstanceId. Inform duplicate instance to shut down immediately.
          responseCallback(JoinGroupResult(memberId, Errors.FENCED_INSTANCE_ID))
        }
        //若group中不存在该memberID或为静态成员但无groupInstanceId
        else if (!group.has(memberId) || groupInstanceIdNotFound) {
            // If the dynamic member trying to register with an unrecognized id, or
            // the static member joins with unknown group instance id, send the response to let
            // it reset its member id and retry.
          //返回UNKNOWN_MEMBER_ID错误
          responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
        } else {
          //根据成员id获取成员元数据对象
          val member = group.get(memberId)

          //根据消费者组当前的状态分别执行不同的操作(判断是否更新内存中member信息)
          group.currentState match {
            //如果是PreparingRebalance状态(表示当前所有Consumer都在进行Join处理)：
            // 调用 updateMemberAndRebalance 更新成员信息并准备 Rebalance
            case PreparingRebalance =>
              //更新成员信息并开始准备Rebalance
            updateMemberAndRebalance(group, member, protocols, responseCallback)

            //如果是CompletingRebalance状态(表示当前所有Consumer已经join完成，正准备Sync处理)：
            //判断请求中的分区分配策略是否和内存中的一致
            //1. 如果一致说明该成员以前申请过加入组，GroupCoordinator也同意了，但该成员没有收到同意的信息；
            //   同时也说明该成员的元数据信息未发生改变，可以直接返回当前的组信息
            //2. 如果不一致则说明该成员的元数据信息发生了改变，调用 updateMemberAndRebalance 更新成员信息并准备 Rebalance
            case CompletingRebalance =>
              //判断请求中的分区分配策略是否和内存中的一致:
              //1. 如果一致说明该成员以前申请过加入组，GroupCoordinator也同意了，但该成员没有收到同意的信息；
              //   同时也说明该成员的元数据信息未发生改变，可以直接返回当前的组信息
              if (member.matches(protocols)) {
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                //直接返回当前组信息
                responseCallback(JoinGroupResult(
                  members = if (group.isLeader(memberId)) {
                    group.currentMemberMetadata
                  } else {
                    List.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  protocolType = group.protocolType,
                  protocolName = group.protocolName,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              }
              //2. 如果不一致则说明该成员的元数据信息发生了改变，调用 updateMemberAndRebalance 更新成员信息并准备 Rebalance
              else {
                // member has changed metadata, so force a rebalance
                //如果分配策略和内存中不一致，说明该成员的元数据发生了变更，那么更新成员信息并开始准备Rebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }

            //如果是Stable状态(表示当前所有Consumer已经sync完成，
            // 即已经rebalance完成，所有Consumer已经可以正常消费消息了)：
            //判断该成员是否是 Leader 成员，或者该成员的元数据发生了变更
            //1. 如果满足上面条件之一，调用 updateMemberAndRebalance 更新成员信息并准备 Rebalance
            //2. 如果均不满足，说明该成员不是 Leader 成员，且元数据信息未发生改变，可以直接返回当前的组信息
            case Stable =>
              val member = group.get(memberId)
              //如果该成员是Leader成员，或者该成员的元数据发生了变更
              if (group.isLeader(memberId) || !member.matches(protocols)) {
                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                // The latter allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                //更新成员信息并开始准备Rebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }
              //如果不是Leader消费者，且元数据未发生变更
              else {
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                //直接返回当前member的信息
                responseCallback(JoinGroupResult(
                  members = List.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  protocolType = group.protocolType,
                  protocolName = group.protocolName,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              }

            //如果是其它状态(Empty或Dead)，调用回调函数返回UNKNOWN_MEMBER_ID异常
            case Empty | Dead =>
              // Group reaches unexpected state. Let the joining member reset their generation and rejoin.
              warn(s"Attempt to add rejoining member $memberId of group ${group.groupId} in " +
                s"unexpected group state ${group.currentState}")
              responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
          }
        }
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      protocolType: Option[String],
                      protocolName: Option[String],
                      groupInstanceId: Option[String],
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback): Unit = {
    //验证消费者组状态及合法性
    validateGroupStatus(groupId, ApiKeys.SYNC_GROUP) match {
      //若消费者组的元数据信息正在被加载，说明是从位移主题中读取消息并填充缓存中的消费者组元数据，
      // 那么当前 Rebalance 过程中各个消费者成员的元数据信息就丢失了，这时需要让消费者组重新从加入组开始。
      //所以封装 REBALANCE_IN_PROGRESS 异常，然后调用回调函数返回。一旦消费者组成员接收到此异常，就会重新开启 Rebalance
      case Some(error) if error == Errors.COORDINATOR_LOAD_IN_PROGRESS =>
        // The coordinator is loading, which means we've lost the state of the active rebalance and the
        // group will need to start over at JoinGroup. By returning rebalance in progress, the consumer
        // will attempt to rejoin without needing to rediscover the coordinator. Note that we cannot
        // return COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect the error.
        responseCallback(SyncGroupResult(Errors.REBALANCE_IN_PROGRESS))

      //对于其他不合法情况则封装对应异常信息并调用回调函数返回
      case Some(error) => responseCallback(SyncGroupResult(error))
      //如果验证通过
      case None =>
        //通过组 id 获取组元数据对象
        groupManager.getGroup(groupId) match {
          //如果没有获取到，则封装Errors.UNKNOWN_MEMBER_ID异常并调用回调函数返回
          case None => responseCallback(SyncGroupResult(Errors.UNKNOWN_MEMBER_ID))
          //TODO-ssy 如果获取到了，执行doSyncGroup方法进行同步组操作
          case Some(group) => doSyncGroup(group, generation, memberId, protocolType, protocolName,
            groupInstanceId, groupAssignment, responseCallback)
        }
    }
  }

  //执行同步组操作
  //代码前面主要是校验各种数据及组的状态，组状态为CompletingRebalance的时候才会处理，
  // 为Stable时只会返回分配信息。
  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          protocolType: Option[String],
                          protocolName: Option[String],
                          groupInstanceId: Option[String],
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback): Unit = {
    group.inLock {
      /**
       * 进行各种合法性验证，不合法则封装对应错误响应并调用回调函数返回
       */
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(SyncGroupResult(Errors.COORDINATOR_NOT_AVAILABLE))
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "sync-group")) {
        responseCallback(SyncGroupResult(Errors.FENCED_INSTANCE_ID))
      }
      //若memberId对应的成员不属于该消费者组
      else if (!group.has(memberId)) {
        responseCallback(SyncGroupResult(Errors.UNKNOWN_MEMBER_ID))
      }
      //若该成员的generationId和消费组的不一致
      else if (generationId != group.generationId) {
        responseCallback(SyncGroupResult(Errors.ILLEGAL_GENERATION))
      } else if (protocolType.isDefined && !group.protocolType.contains(protocolType.get)) {
        responseCallback(SyncGroupResult(Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else if (protocolName.isDefined && !group.protocolName.contains(protocolName.get)) {
        responseCallback(SyncGroupResult(Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else {
        /**
         * 如果通过合法性验证，则根据当前的组状态执行对应的操作
         */
        //组状态为CompletingRebalance的时候才会处理， 为Stable时只会返回分配信息。
        group.currentState match {
          case Empty =>
            responseCallback(SyncGroupResult(Errors.UNKNOWN_MEMBER_ID))

          case PreparingRebalance =>
            responseCallback(SyncGroupResult(Errors.REBALANCE_IN_PROGRESS))

          //此状态表明当前正在进行分区分配
          case CompletingRebalance =>
            //为当前消费者组成员设置组同步回调函数
            group.get(memberId).awaitingSyncCallback = responseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            //若当前member为leader，则需要获取leader中的分配结果；否则不进行处理
            if (group.isLeader(memberId)) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              //对于没有被分配任何消费方案的成员，则创建一个空的方案给它
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              if (missing.nonEmpty) {
                warn(s"Setting empty assignments for members $missing of ${group.groupId} for generation ${group.generationId}")
              }

              //把消费者组信息保存在消费者组元数据中，并且将其写入到内部位移主题
              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the CompletingRebalance state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  //如果组状态是CompletingRebalance且成员和组的generationId相同
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    //如果有错误
                    if (error != Errors.NONE) {
                      //清空分配方案并发送给所有成员
                      resetAndPropagateAssignmentError(group, error)
                      //准备开启新一轮的Rebalance
                      maybePrepareRebalance(group, s"error when storing group assignment during SyncGroup (member: $memberId)")
                    } else {
                      //如果没有错误
                      //在消费者组元数据中为每个消费者成员保存分配方案并发送给所有成员
                      setAndPropagateAssignment(group, assignment)
                      //将组状态转换为Stable，之后就可以正常提供服务了
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
              groupCompletedRebalanceSensor.record()
            }

          //如果是Stable状态，从组元数据中获取并返回分配信息
          case Stable =>
            // if the group is stable, we just return the current assignment
            //获取组元数据对象
            val memberMetadata = group.get(memberId)
            //封装同步结果，包含该，member自己的消费分区分配方案和Errors.NONE表示无异常，返回调用回调函数返回
            responseCallback(SyncGroupResult(group.protocolType, group.protocolName, memberMetadata.assignment, Errors.NONE))
            //设定成员下次心跳时间
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))

          case Dead =>
            throw new IllegalStateException(s"Reached unexpected condition for Dead group ${group.groupId}")
        }
      }
    }
  }

  def handleLeaveGroup(groupId: String,
                       leavingMembers: List[MemberIdentity],
                       responseCallback: LeaveGroupResult => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.LEAVE_GROUP) match {
      case Some(error) =>
        responseCallback(leaveError(error, List.empty))
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            responseCallback(leaveError(Errors.NONE, leavingMembers.map {leavingMember =>
              memberLeaveError(leavingMember, Errors.UNKNOWN_MEMBER_ID)
            }))
          case Some(group) =>
            group.inLock {
              if (group.is(Dead)) {
                responseCallback(leaveError(Errors.COORDINATOR_NOT_AVAILABLE, List.empty))
              } else {
                val memberErrors = leavingMembers.map { leavingMember =>
                  val memberId = leavingMember.memberId
                  val groupInstanceId = Option(leavingMember.groupInstanceId)
                  if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID
                    && group.isStaticMemberFenced(memberId, groupInstanceId, "leave-group")) {
                    memberLeaveError(leavingMember, Errors.FENCED_INSTANCE_ID)
                  } else if (group.isPendingMember(memberId)) {
                    if (groupInstanceId.isDefined) {
                      throw new IllegalStateException(s"the static member $groupInstanceId was not expected to be leaving " +
                        s"from pending member bucket with member id $memberId")
                    } else {
                      // if a pending member is leaving, it needs to be removed from the pending list, heartbeat cancelled
                      // and if necessary, prompt a JoinGroup completion.
                      info(s"Pending member $memberId is leaving group ${group.groupId}.")
                      removePendingMemberAndUpdateGroup(group, memberId)
                      heartbeatPurgatory.checkAndComplete(MemberKey(group.groupId, memberId))
                      memberLeaveError(leavingMember, Errors.NONE)
                    }
                  } else if (!group.has(memberId) && !group.hasStaticMember(groupInstanceId)) {
                    memberLeaveError(leavingMember, Errors.UNKNOWN_MEMBER_ID)
                  } else {
                    val member = if (group.hasStaticMember(groupInstanceId))
                      group.get(group.getStaticMemberId(groupInstanceId))
                    else
                      group.get(memberId)
                    removeHeartbeatForLeavingMember(group, member)
                    info(s"Member[group.instance.id ${member.groupInstanceId}, member.id ${member.memberId}] " +
                      s"in group ${group.groupId} has left, removing it from the group")
                    removeMemberAndUpdateGroup(group, member, s"removing member $memberId on LeaveGroup")
                    memberLeaveError(leavingMember, Errors.NONE)
                  }
                }
                responseCallback(leaveError(Errors.NONE, memberErrors))
              }
            }
        }
    }
  }

  def handleDeleteGroups(groupIds: Set[String]): Map[String, Errors] = {
    var groupErrors: Map[String, Errors] = Map()
    var groupsEligibleForDeletion: Seq[GroupMetadata] = Seq()

    groupIds.foreach { groupId =>
      validateGroupStatus(groupId, ApiKeys.DELETE_GROUPS) match {
        case Some(error) =>
          groupErrors += groupId -> error

        case None =>
          groupManager.getGroup(groupId) match {
            case None =>
              groupErrors += groupId ->
                (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
            case Some(group) =>
              group.inLock {
                group.currentState match {
                  case Dead =>
                    groupErrors += groupId ->
                      (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
                  case Empty =>
                    group.transitionTo(Dead)
                    groupsEligibleForDeletion :+= group
                  case Stable | PreparingRebalance | CompletingRebalance =>
                    groupErrors += groupId -> Errors.NON_EMPTY_GROUP
                }
              }
          }
      }
    }

    if (groupsEligibleForDeletion.nonEmpty) {
      val offsetsRemoved = groupManager.cleanupGroupMetadata(groupsEligibleForDeletion, _.removeAllOffsets())
      groupErrors ++= groupsEligibleForDeletion.map(_.groupId -> Errors.NONE).toMap
      info(s"The following groups were deleted: ${groupsEligibleForDeletion.map(_.groupId).mkString(", ")}. " +
        s"A total of $offsetsRemoved offsets were removed.")
    }

    groupErrors
  }

  def handleDeleteOffsets(groupId: String, partitions: Seq[TopicPartition]): (Errors, Map[TopicPartition, Errors]) = {
    var groupError: Errors = Errors.NONE
    var partitionErrors: Map[TopicPartition, Errors] = Map()
    var partitionsEligibleForDeletion: Seq[TopicPartition] = Seq()

    validateGroupStatus(groupId, ApiKeys.OFFSET_DELETE) match {
      case Some(error) =>
        groupError = error

      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            groupError = if (groupManager.groupNotExists(groupId))
              Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR

          case Some(group) =>
            group.inLock {
              group.currentState match {
                case Dead =>
                  groupError = if (groupManager.groupNotExists(groupId))
                    Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR

                case Empty =>
                  partitionsEligibleForDeletion = partitions

                case PreparingRebalance | CompletingRebalance | Stable if group.isConsumerGroup =>
                  val (consumed, notConsumed) =
                    partitions.partition(tp => group.isSubscribedToTopic(tp.topic()))

                  partitionsEligibleForDeletion = notConsumed
                  partitionErrors = consumed.map(_ -> Errors.GROUP_SUBSCRIBED_TO_TOPIC).toMap

                case _ =>
                  groupError = Errors.NON_EMPTY_GROUP
              }
            }

            if (partitionsEligibleForDeletion.nonEmpty) {
              val offsetsRemoved = groupManager.cleanupGroupMetadata(Seq(group), group => {
                group.removeOffsets(partitionsEligibleForDeletion)
              })

              partitionErrors ++= partitionsEligibleForDeletion.map(_ -> Errors.NONE).toMap

              offsetDeletionSensor.record(offsetsRemoved)

              info(s"The following offsets of the group $groupId were deleted: ${partitionsEligibleForDeletion.mkString(", ")}. " +
                s"A total of $offsetsRemoved offsets were removed.")
            }
        }
    }

    // If there is a group error, the partition errors is empty
    groupError -> partitionErrors
  }

  //Server 端处理心跳请求
  def handleHeartbeat(groupId: String,
                      memberId: String,
                      groupInstanceId: Option[String],
                      generationId: Int,
                      responseCallback: Errors => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.HEARTBEAT).foreach { error =>
      if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS)
        // the group is still loading, so respond just blindly
        responseCallback(Errors.NONE)
      else
        responseCallback(error)
      return
    }

    groupManager.getGroup(groupId) match {
      //当前 GroupCoordinator 不包含这个 group
      case None =>
        responseCallback(Errors.UNKNOWN_MEMBER_ID)

      //当前 GroupCoordinator 包含这个 group
      case Some(group) => group.inLock {
        if (group.is(Dead)) {
          // if the group is marked as dead, it means some other thread has just removed the group
          // from the coordinator metadata; this is likely that the group has migrated to some other
          // coordinator OR the group is in a transient unstable phase. Let the member retry
          // finding the correct coordinator and rejoin.
          responseCallback(Errors.COORDINATOR_NOT_AVAILABLE)
        } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "heartbeat")) {
          responseCallback(Errors.FENCED_INSTANCE_ID)
        } else if (!group.has(memberId)) {
          responseCallback(Errors.UNKNOWN_MEMBER_ID)
        } else if (generationId != group.generationId) {
          responseCallback(Errors.ILLEGAL_GENERATION)
        } else {
          group.currentState match {
            case Empty =>
              //group 的状态为 Empty, 意味着 group 的成员为空,返回 UNKNOWN_MEMBER_ID 错误
              responseCallback(Errors.UNKNOWN_MEMBER_ID)

            case CompletingRebalance =>
                responseCallback(Errors.REBALANCE_IN_PROGRESS)

            //group 状态为 PreparingRebalance
            case PreparingRebalance =>
                val member = group.get(memberId)
                completeAndScheduleNextHeartbeatExpiration(group, member)
                responseCallback(Errors.REBALANCE_IN_PROGRESS)

            case Stable =>
                val member = group.get(memberId)
                // 更新心跳时间,认为心跳完成,并监控下次的调度情况（超时的话,会把这个 member 从 group 中移除）
                completeAndScheduleNextHeartbeatExpiration(group, member)
                responseCallback(Errors.NONE)

            case Dead =>
              //group 的状态已经变为 dead,意味着 group 的 meta 已经被清除,返回 UNKNOWN_MEMBER_ID 错误
              throw new IllegalStateException(s"Reached unexpected condition for Dead group $groupId")
          }
        }
      }
    }
  }

  def handleTxnCommitOffsets(groupId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             memberId: String,
                             groupInstanceId: Option[String],
                             generationId: Int,
                             offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                             responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.TXN_OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.map { case (k, _) => k -> error })
      case None =>
        val group = groupManager.getGroup(groupId).getOrElse {
          groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
        }
        doTxnCommitOffsets(group, memberId, groupInstanceId, generationId, producerId, producerEpoch, offsetMetadata, responseCallback)
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          groupInstanceId: Option[String],
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.map { case (k, _) => k -> error })
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            if (generationId < 0) {
              // the group is not relying on Kafka for group management, so allow the commit
              val group = groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
              doCommitOffsets(group, memberId, groupInstanceId, generationId, offsetMetadata, responseCallback)
            } else {
              // or this is a request coming from an older generation. either way, reject the commit
              responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
            }

          case Some(group) =>
            doCommitOffsets(group, memberId, groupInstanceId, generationId, offsetMetadata, responseCallback)
        }
    }
  }

  def scheduleHandleTxnCompletion(producerId: Long,
                                  offsetsPartitions: Iterable[TopicPartition],
                                  transactionResult: TransactionResult): Unit = {
    require(offsetsPartitions.forall(_.topic == Topic.GROUP_METADATA_TOPIC_NAME))
    val isCommit = transactionResult == TransactionResult.COMMIT
    groupManager.scheduleHandleTxnCompletion(producerId, offsetsPartitions.map(_.partition).toSet, isCommit)
  }

  private def doTxnCommitOffsets(group: GroupMetadata,
                                 memberId: String,
                                 groupInstanceId: Option[String],
                                 generationId: Int,
                                 producerId: Long,
                                 producerEpoch: Short,
                                 offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                                 responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.COORDINATOR_NOT_AVAILABLE })
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "txn-commit-offsets")) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.FENCED_INSTANCE_ID })
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // Enforce member id when it is set.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.UNKNOWN_MEMBER_ID })
      } else if (generationId >= 0 && generationId != group.generationId) {
        // Enforce generation check when it is set.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
      } else {
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback, producerId, producerEpoch)
      }
    }
  }

  //
  private def doCommitOffsets(group: GroupMetadata,
                              memberId: String,
                              groupInstanceId: Option[String],
                              generationId: Int,
                              offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                              responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.COORDINATOR_NOT_AVAILABLE })
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "commit-offsets")) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.FENCED_INSTANCE_ID })
      } else if (generationId < 0 && group.is(Empty)) {
        //来自 assign 的情况
        // The group is only using Kafka to store offsets.
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.UNKNOWN_MEMBER_ID })
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
      } else {
        group.currentState match {
          case Stable | PreparingRebalance =>
            // During PreparingRebalance phase, we still allow a commit request since we rely
            // on heartbeat response to eventually notify the rebalance in progress signal to the consumer
            val member = group.get(memberId)
            //更新下次需要的心跳时间
            completeAndScheduleNextHeartbeatExpiration(group, member)
            groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)

          case CompletingRebalance =>
            // We should not receive a commit request if the group has not completed rebalance;
            // but since the consumer's member.id and generation is valid, it means it has received
            // the latest group generation information from the JoinResponse.
            // So let's return a REBALANCE_IN_PROGRESS to let consumer handle it gracefully.
            responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.REBALANCE_IN_PROGRESS })

          case _ =>
            throw new RuntimeException(s"Logic error: unexpected group state ${group.currentState}")
        }
      }
    }
  }

  def handleFetchOffsets(groupId: String, requireStable: Boolean, partitions: Option[Seq[TopicPartition]] = None):
  (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {

    validateGroupStatus(groupId, ApiKeys.OFFSET_FETCH) match {
      case Some(error) => error -> Map.empty
      case None =>
        // return offsets blindly regardless the current group state since the group may be using
        // Kafka commit storage without automatic group management
        (Errors.NONE, groupManager.getOffsets(groupId, requireStable, partitions))
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading) Errors.COORDINATOR_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  //DESCRIBE_GROUPS 请求处理实现如下，主要是返回 group 中各个 member 的详细信息，
  // 包含的变量信息为 memberId, clientId, clientHost, metadata(protocol), assignment
  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    validateGroupStatus(groupId, ApiKeys.DESCRIBE_GROUPS) match {
      case Some(error) => (error, GroupCoordinator.EmptyGroup)
      case None =>
        groupManager.getGroup(groupId) match {
          //返回 group 详细信息,主要是 member 的详细信息
          case None => (Errors.NONE, GroupCoordinator.DeadGroup)
          case Some(group) =>
            group.inLock {
              (Errors.NONE, group.summary)
            }
        }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]): Unit = {
    val offsetsRemoved = groupManager.cleanupGroupMetadata(groupManager.currentGroups, group => {
      group.removeOffsets(topicPartitions)
    })
    info(s"Removed $offsetsRemoved offsets associated with deleted partitions: ${topicPartitions.mkString(", ")}.")
  }

  private def isValidGroupId(groupId: String, api: ApiKeys): Boolean = {
    api match {
      case ApiKeys.OFFSET_COMMIT | ApiKeys.OFFSET_FETCH | ApiKeys.DESCRIBE_GROUPS | ApiKeys.DELETE_GROUPS =>
        // For backwards compatibility, we support the offset commit APIs for the empty groupId, and also
        // in DescribeGroups and DeleteGroups so that users can view and delete state of all groups.
        groupId != null
      case _ =>
        // The remaining APIs are groups using Kafka for group coordination and must have a non-empty groupId
        groupId != null && !groupId.isEmpty
    }
  }

  /**
   * Check that the groupId is valid, assigned to this coordinator and that the group has been loaded.
   */

  /**
   * 验证消费者组状态的合法性，这里的合法性包括：
   * 1. 消费者组id是否合法，即不为空；
   * 2. GroupCoordinator是否为Active状态；
   * 3. 消费者组的元数据信息是否正在被加载；
   *    如果正在被加载，说明是从位移主题中读取消息并填充缓存中的消费者组元数据，
   *    那么当前 Rebalance 过程中各个消费者成员的元数据信息就丢失了，这时需要让消费者组重新从加入组开始。
   *    这时会封装 REBALANCE_IN_PROGRESS 异常，然后调用回调函数返回。一旦消费者组成员接收到此异常，就会重新开启 Rebalance
   * 4. 当前节点的 GroupCoordinator 是否为管理该消费者组的 GroupCoordinator。
   */
  private def validateGroupStatus(groupId: String, api: ApiKeys): Option[Errors] = {
    if (!isValidGroupId(groupId, api))
      Some(Errors.INVALID_GROUP_ID)
    else if (!isActive.get)
      Some(Errors.COORDINATOR_NOT_AVAILABLE)
    else if (isCoordinatorLoadInProgress(groupId))
      Some(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    else if (!isCoordinatorForGroup(groupId))
      Some(Errors.NOT_COORDINATOR)
    else
      None
  }

  private def onGroupUnloaded(group: GroupMetadata): Unit = {
    group.inLock {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      group.transitionTo(Dead)

      previousState match {
        case Empty | Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            group.maybeInvokeJoinCallback(member, JoinGroupResult(member.memberId, Errors.NOT_COORDINATOR))
          }

          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | CompletingRebalance =>
          for (member <- group.allMemberMetadata) {
            group.maybeInvokeSyncCallback(member, SyncGroupResult(Errors.NOT_COORDINATOR))
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  private def onGroupLoaded(group: GroupMetadata): Unit = {
    group.inLock {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable) || group.is(Empty))
      if (groupIsOverCapacity(group)) {
        prepareRebalance(group, s"Freshly-loaded group is over capacity ($groupConfig.groupMaxSize). Rebalacing in order to give a chance for consumers to commit offsets")
      }

      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  /**
   * Load cached state from the given partition and begin handling requests for groups which map to it.
   *
   * @param offsetTopicPartitionId The partition we are now leading
   */
  def onElection(offsetTopicPartitionId: Int): Unit = {
    groupManager.scheduleLoadGroupAndOffsets(offsetTopicPartitionId, onGroupLoaded)
  }

  /**
   * Unload cached state for the given partition and stop handling requests for groups which map to it.
   *
   * @param offsetTopicPartitionId The partition we are no longer leading
   */
  def onResignation(offsetTopicPartitionId: Int): Unit = {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]): Unit = {
    assert(group.is(CompletingRebalance))
    //更新每个member对应的partition分配方案
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    //TODO-ssy 遍历组成员，调用该成员的回调函数将分配方案发送给每个消费者，，调用完成后将其回调函数清除
    propagateAssignment(group, Errors.NONE)
  }

  //清空分配方案并发送给所有成员
  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors): Unit = {
    assert(group.is(CompletingRebalance))
    group.allMemberMetadata.foreach(_.assignment = Array.empty)
    propagateAssignment(group, error)
  }

  /**
   * 将分配方案发送给每个消费者时，调用了 propagateAssignment 方法。其主要做了两件事：
   * 1. 遍历所有的消费者成员，调用回调函数将属于该成员的消费分区分配方案返回
   * 2. 如果回调函数执行成功，完成心跳并设置下一次心跳的超时时间
   *
   * (从该方法中可以知道，每个消费者只接收到了属于自己的消费分区分配方案，而不知道其它消费者的分配方案)
   */
  private def propagateAssignment(group: GroupMetadata, error: Errors): Unit = {
    val (protocolType, protocolName) = if (error == Errors.NONE)
      (group.protocolType, group.protocolName)
    else
      (None, None)
    //TODO-ssy 遍历组成员，调用该成员的回调函数，调用完成后将其回调函数清除
    for (member <- group.allMemberMetadata) {
      if (member.assignment.isEmpty && error == Errors.NONE) {
        warn(s"Sending empty assignment to member ${member.memberId} of ${group.groupId} for generation ${group.generationId} with no errors")
      }

      //TODO-ssy 调用回调函数，，调用完成后将其回调函数清除，每个消费者只收到了属于自己的分配方案
      //处理成员的response，返回分配信息，并重置心跳。
      if (group.maybeInvokeSyncCallback(member, SyncGroupResult(protocolType, protocolName, member.assignment, error))) {
        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        //如果返回true，则设置下次心跳的时间
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  //设定成员member的下次心跳时间
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata): Unit = {
    completeAndScheduleNextExpiration(group, member, member.sessionTimeoutMs)
  }

  private def completeAndScheduleNextExpiration(group: GroupMetadata, member: MemberMetadata, timeoutMs: Long): Unit = {
    val memberKey = MemberKey(member.groupId, member.memberId)

    // complete current heartbeat expectation
    member.heartbeatSatisfied = true
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    member.heartbeatSatisfied = false
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member.memberId, isPending = false, timeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  /**
    * Add pending member expiration to heartbeat purgatory
    */
  private def addPendingMemberExpiration(group: GroupMetadata, pendingMemberId: String, timeoutMs: Long): Unit = {
    val pendingMemberKey = MemberKey(group.groupId, pendingMemberId)
    val delayedHeartbeat = new DelayedHeartbeat(this, group, pendingMemberId, isPending = true, timeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(pendingMemberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata): Unit = {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  /**
   *在没有memberId且没有groupInstanceId的情况下，会生成memberId并返回，
   * 在那个时候就会将生成的memberId放入pendingMembers中，表示待加入的member。
   * 也就是说在这种情况下，客户端第二次带上memberId请求服务端的时候会走走addMemberAndRebalance的逻辑。
   */
  //向内存中添加member信息并进行rebalance处理
  //主要是多一步：确定消费组的 Leader 成员。逻辑很简单：就是第一个加入组的成员
  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    memberId: String,
                                    groupInstanceId: Option[String],
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback): Unit = {
    //1. 根据给定的元数据信息初始化成员元数据对象
    val member = new MemberMetadata(memberId, group.groupId, groupInstanceId,
      clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)
    //2. 标记该成员是新加入组的
    member.isNew = true

    // update the newMemberAdded flag to indicate that the join group can be further delayed
    //3. 如果组状态为PreparingRebalance，且generationId == 0，说明是第一次进行Rebalance
    if (group.is(PreparingRebalance) && group.generationId == 0)
    //设置newMemberAdded = true
      group.newMemberAdded = true

    //4. 将成员信息添加到group的元数据对象members中，如果还没有选出Leader成员，则设置当前成员为Leader
    group.add(member, callback)

    // The session timeout does not affect new members since they do not have their memberId and
    // cannot send heartbeats. Furthermore, we cannot detect disconnects because sockets are muted
    // while the JoinGroup is in purgatory. If the client does disconnect (e.g. because of a request
    // timeout during a long rebalance), they may simply retry which will lead to a lot of defunct
    // members in the rebalance. To prevent this going on indefinitely, we timeout JoinGroup requests
    // for new members. If the new member is still there, we expect it to retry.
    // 5. 完成心跳并设置下次心跳超时的时间
    completeAndScheduleNextExpiration(group, member, NewMemberJoinTimeoutMs)

    //若该member是静态成员
    if (member.isStaticMember) {
      info(s"Adding new static member $groupInstanceId to group ${group.groupId} with member id $memberId.")
      //添加到静态成员列表
      group.addStaticMember(groupInstanceId, memberId)
    } else {
      //6. 从待决成员列表中移除，表示member添加成功
      group.removePendingMember(memberId)
    }
    //7. 准备开启Rebalance，进行rebalance的延迟任务处理
    maybePrepareRebalance(group, s"Adding new member $memberId with group instance id $groupInstanceId")
  }

  //1. 替换memberId
  //2. 储存group信息
  //3. rebalance处理
  private def updateStaticMemberAndRebalance(group: GroupMetadata,
                                             newMemberId: String,
                                             groupInstanceId: Option[String],
                                             protocols: List[(String, Array[Byte])],
                                             responseCallback: JoinCallback): Unit = {
    val oldMemberId = group.getStaticMemberId(groupInstanceId)
    info(s"Static member $groupInstanceId of group ${group.groupId} with unknown member id rejoins, assigning new member id $newMemberId, while " +
      s"old member id $oldMemberId will be removed.")

    val currentLeader = group.leaderOrNull
    //替换静态member中的memberId
    val member = group.replaceGroupInstance(oldMemberId, newMemberId, groupInstanceId)
    // Heartbeat of old member id will expire without effect since the group no longer contains that member id.
    // New heartbeat shall be scheduled with new member id.
    completeAndScheduleNextHeartbeatExpiration(group, member)

    val knownStaticMember = group.get(newMemberId)
    //更新静态member信息
    group.updateMember(knownStaticMember, protocols, responseCallback)
    val oldProtocols = knownStaticMember.supportedProtocols

    //判断当前group的状态
    group.currentState match {
      case Stable =>
        // check if group's selectedProtocol of next generation will change, if not, simply store group to persist the
        // updated static member, if yes, rebalance should be triggered to let the group's assignment and selectProtocol consistent
        val selectedProtocolOfNextGeneration = group.selectProtocol
        if (group.protocolName.contains(selectedProtocolOfNextGeneration)) {
          info(s"Static member which joins during Stable stage and doesn't affect selectProtocol will not trigger rebalance.")
          val groupAssignment: Map[String, Array[Byte]] = group.allMemberMetadata.map(member => member.memberId -> member.assignment).toMap
          groupManager.storeGroup(group, groupAssignment, error => {
            if (error != Errors.NONE) {
              warn(s"Failed to persist metadata for group ${group.groupId}: ${error.message}")

              // Failed to persist member.id of the given static member, revert the update of the static member in the group.
              group.updateMember(knownStaticMember, oldProtocols, null)
              val oldMember = group.replaceGroupInstance(newMemberId, oldMemberId, groupInstanceId)
              completeAndScheduleNextHeartbeatExpiration(group, oldMember)
              responseCallback(JoinGroupResult(
                List.empty,
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                generationId = group.generationId,
                protocolType = group.protocolType,
                protocolName = group.protocolName,
                leaderId = currentLeader,
                error = error
              ))
            } else {
              group.maybeInvokeJoinCallback(member, JoinGroupResult(
                members = List.empty,
                memberId = newMemberId,
                generationId = group.generationId,
                protocolType = group.protocolType,
                protocolName = group.protocolName,
                // We want to avoid current leader performing trivial assignment while the group
                // is in stable stage, because the new assignment in leader's next sync call
                // won't be broadcast by a stable group. This could be guaranteed by
                // always returning the old leader id so that the current leader won't assume itself
                // as a leader based on the returned message, since the new member.id won't match
                // returned leader id, therefore no assignment will be performed.
                leaderId = currentLeader,
                error = Errors.NONE))
            }
          })
        } else {
          maybePrepareRebalance(group, s"Group's selectedProtocol will change because static member ${member.memberId} with instance id $groupInstanceId joined with change of protocol")
        }
      case CompletingRebalance =>
        // if the group is in after-sync stage, upon getting a new join-group of a known static member
        // we should still trigger a new rebalance, since the old member may already be sent to the leader
        // for assignment, and hence when the assignment gets back there would be a mismatch of the old member id
        // with the new replaced member id. As a result the new member id would not get any assignment.
        prepareRebalance(group, s"Updating metadata for static member ${member.memberId} with instance id $groupInstanceId")
      case Empty | Dead =>
        //抛出异常
        throw new IllegalStateException(s"Group ${group.groupId} was not supposed to be " +
          s"in the state ${group.currentState} when the unknown static member $groupInstanceId rejoins.")
      //对于PreparingRebalance不做处理
      case PreparingRebalance =>
    }
  }

  /**
   * 调用场景：
   1. 在group状态为PreparingRebalance时，即触发了joinGroup，在等待其他组成员加入的状态，这里会直接调用updateMemberAndRebalance
   2. 在group状态为CompletingRebalance时，即组成员已全部加入，并选举出consumeLeader，等待同步分配方案的状态，这时会匹配传入的协议信息，如果不匹配则调用updateMemberAndRebalance，会再次触发rebalance
   3. 在group状态为Stable时，即完成了重分配，处于稳定态，如果是consumeleader或者不匹配协议信息，则调用updateMemberAndRebalance
   */
  //更新member信息并准备 Rebalance
  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback): Unit = {
    //根据新加入成员的元数据信息，更新消费者组元数据
    group.updateMember(member, protocols, callback)
    //尝试准备进行Rebalance
    maybePrepareRebalance(group, s"Updating metadata for member ${member.memberId}")
  }

  //尝试准备进行Rebalance操作
  //如果是Stable
  //     CompletingRebalance
  //     Empty
  //     三种状态之一才可调用rebalance方法
  private def maybePrepareRebalance(group: GroupMetadata, reason: String): Unit = {
    group.inLock {
      //canRebalance 方法，就是看当前的组状态是否为 PreparingRebalance 的前置状态，
      // 满足条件的有三种：Stable, CompletingRebalance, Empty
      if (group.canRebalance) {
        //调用 prepareRebalance 方法，准备进行 Rebalance
        prepareRebalance(group, reason)
      }
    }
  }

  //准备进行 Rebalance
  private def prepareRebalance(group: GroupMetadata, reason: String): Unit = {
    // if any members are awaiting sync, cancel their request and have them rejoin
    //如果是CompletingRebalance状态(gourp的所有成员已经join完成，但还未sync完成)
    if (group.is(CompletingRebalance))
      //则所有Consumer返回REBALANCE_IN_PROGRESS，等待重新join
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    //如果是Empty状态，则初始化InitialDelayedJoin对象
    val delayedRebalance = if (group.is(Empty))
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    else
      //如果是Stable状态，则初始化DelayedJoin对象
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)

    //TODO-ssy 将组状态转为PreparingRebalance
    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} in state ${group.currentState} with old generation " +
      s"${group.generationId} (${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)}) (reason: $reason)")

    val groupKey = GroupKey(group.groupId)
    //将延时任务添加到joinPurgatory，等待超时或全部member都join完成
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def removeMemberAndUpdateGroup(group: GroupMetadata, member: MemberMetadata, reason: String): Unit = {
    // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
    // to invoke the callback before removing the member. We return UNKNOWN_MEMBER_ID so that the consumer
    // will retry the JoinGroup request if is still active.
    group.maybeInvokeJoinCallback(member, JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID))

    group.remove(member.memberId)
    group.removeStaticMember(member.groupInstanceId)

    group.currentState match {
      case Dead | Empty =>
      case Stable | CompletingRebalance => maybePrepareRebalance(group, reason)
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  private def removePendingMemberAndUpdateGroup(group: GroupMetadata, memberId: String): Unit = {
    group.removePendingMember(memberId)

    if (group.is(PreparingRebalance)) {
      joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  //处理JoinGroup的延时任务
  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group.inLock {
      //如果所有成员都已经加入(gorup中所有组员状态为AwaitingJoin，并且pendingMembers为空)
      if (group.hasAllMembersJoined)
        forceComplete()
      else false
    }
  }

  def onExpireJoin(): Unit = {
    // TODO: add metrics for restabilize timeouts
  }

  //当所有申请加入的成员都已经在组中创建了元数据对象时，会立刻执行加入组的操作
  def onCompleteJoin(group: GroupMetadata): Unit = {
    group.inLock {
      // remove dynamic members who haven't joined the group yet
      //移除GroupMetadata中的notYetRejoinedMembers的成员，即移除还未Join成功的成员，但静态成员除外
      group.notYetRejoinedMembers.filterNot(_.isStaticMember) foreach { failedMember =>
        removeHeartbeatForLeavingMember(group, failedMember)
        group.remove(failedMember.memberId)
        group.removeStaticMember(failedMember.groupInstanceId)
        // TODO: cut the socket connection to the client
      }

      if (group.is(Dead)) {
        info(s"Group ${group.groupId} is dead, skipping rebalance stage")
      }
      //如果组成员不为空且还未选出Leader成员？继续等待，直到rebalanceTimeoutMs超时
      else if (!group.maybeElectNewJoinedLeader() && group.allMembers.nonEmpty) {
        // If all members are not rejoining, we will postpone the completion
        // of rebalance preparing stage, and send out another delayed operation
        // until session timeout removes all the non-responsive members.
        error(s"Group ${group.groupId} could not complete rebalance because no members rejoined")
        joinPurgatory.tryCompleteElseWatch(
          new DelayedJoin(this, group, group.rebalanceTimeoutMs),
          Seq(GroupKey(group.groupId)))
      }
      //如果组成员为空或已选出Leader成员
      else {
        //TODO-ssy 选举分区分配策略、更新group的generationId、同时转换group状态到CompletingRebalance
        group.initNextGeneration()
        //如果组状态为空，即member为空
        if (group.is(Empty)) {
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          //向位移主题写入消费者组元数据，存储及缓存组信息
          groupManager.storeGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        }
        //如果组不为空
        else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId} " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          // trigger the awaiting join group response callback for all the members after rebalancing
          //遍历所有组成员,返回对应的JoinGroupRequest应答，对组leader返回对应的所有组成员，用于后续分区分配
          for (member <- group.allMemberMetadata) {
            //封装回调结果
            val joinResult = JoinGroupResult(
              //TODO-ssy 如果是Leader成员，该members变量是组内的所有成员
              //如果不是Leader成员，该members变量为空
              members = if (group.isLeader(member.memberId)) {
                group.currentMemberMetadata
              } else {
                List.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              //选举出的分区分配策略，是唯一的
              protocolType = group.protocolType,
              protocolName = group.protocolName,
              leaderId = group.leaderOrNull,
              //响应结果中的 error 类型为 Errors.NONE，客户端会根据这个类型执行相应的操作
              error = Errors.NONE)

            //调用回调函数返回
            group.maybeInvokeJoinCallback(member, joinResult)
            //完成当前心跳任务并设置下一个心跳的超时时间
            completeAndScheduleNextHeartbeatExpiration(group, member)
            //标记该成员为非新成员
            member.isNew = false
          }
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata,
                           memberId: String,
                           isPending: Boolean,
                           forceComplete: () => Boolean): Boolean = {
    group.inLock {
      // The group has been unloaded and invalid, we should complete the heartbeat.
      if (group.is(Dead)) {
        forceComplete()
      } else if (isPending) {
        // complete the heartbeat if the member has joined the group
        if (group.has(memberId)) {
          forceComplete()
        } else false
      } else if (shouldCompleteNonPendingHeartbeat(group, memberId)) {
        forceComplete()
      } else false
    }
  }

  def shouldCompleteNonPendingHeartbeat(group: GroupMetadata, memberId: String): Boolean = {
    if (group.has(memberId)) {
      val member = group.get(memberId)
      member.hasSatisfiedHeartbeat || member.isLeaving
    } else {
      info(s"Member id $memberId was not found in ${group.groupId} during heartbeat completion check")
      true
    }
  }

  def onExpireHeartbeat(group: GroupMetadata, memberId: String, isPending: Boolean): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        info(s"Received notification of heartbeat expiration for member $memberId after group ${group.groupId} had already been unloaded or deleted.")
      } else if (isPending) {
        info(s"Pending member $memberId in group ${group.groupId} has been removed after session timeout expiration.")
        removePendingMemberAndUpdateGroup(group, memberId)
      } else if (!group.has(memberId)) {
        debug(s"Member $memberId has already been removed from the group.")
      } else {
        val member = group.get(memberId)
        if (!member.hasSatisfiedHeartbeat) {
          info(s"Member ${member.memberId} in group ${group.groupId} has failed, removing it from the group")
          removeMemberAndUpdateGroup(group, member, s"removing member ${member.memberId} on heartbeat expiration")
        }
      }
    }
  }

  def onCompleteHeartbeat(): Unit = {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  //该消费者组成员已超额
  private def groupIsOverCapacity(group: GroupMetadata): Boolean = {
    group.size > groupConfig.groupMaxSize
  }

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoGeneration = -1
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)
  val NewMemberJoinTimeoutMs: Int = 5 * 60 * 1000

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            time: Time,
            metrics: Metrics): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkClient, replicaManager, heartbeatPurgatory, joinPurgatory, time, metrics)
  }

  private[group] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time,
            metrics: Metrics): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs,
      groupMaxSize = config.groupMaxSize,
      groupInitialRebalanceDelayMs = config.groupInitialRebalanceDelay)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkClient, time, metrics)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time, metrics)
  }

  private def memberLeaveError(memberIdentity: MemberIdentity,
                               error: Errors): LeaveMemberResponse = {
    LeaveMemberResponse(
      memberId = memberIdentity.memberId,
      groupInstanceId = Option(memberIdentity.groupInstanceId),
      error = error)
  }

  private def leaveError(topLevelError: Errors,
                         memberResponses: List[LeaveMemberResponse]): LeaveGroupResult = {
    LeaveGroupResult(
      topLevelError = topLevelError,
      memberResponses = memberResponses)
  }
}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int,
                       groupMaxSize: Int,                 //消费组最大成员数
                       groupInitialRebalanceDelayMs: Int)

case class JoinGroupResult(members: List[JoinGroupResponseMember],
                           memberId: String,
                           generationId: Int,
                           protocolType: Option[String],
                           protocolName: Option[String],
                           leaderId: String,
                           error: Errors)

object JoinGroupResult {
  def apply(memberId: String, error: Errors): JoinGroupResult = {
    JoinGroupResult(
      members = List.empty,
      memberId = memberId,
      generationId = GroupCoordinator.NoGeneration,
      protocolType = None,
      protocolName = None,
      leaderId = GroupCoordinator.NoLeader,
      error = error)
  }
}

case class SyncGroupResult(protocolType: Option[String],
                           protocolName: Option[String],
                           memberAssignment: Array[Byte],
                           error: Errors)

object SyncGroupResult {
  def apply(error: Errors): SyncGroupResult = {
    SyncGroupResult(None, None, Array.empty, error)
  }
}

case class LeaveMemberResponse(memberId: String,
                               groupInstanceId: Option[String],
                               error: Errors)

case class LeaveGroupResult(topLevelError: Errors,
                            memberResponses : List[LeaveMemberResponse])
