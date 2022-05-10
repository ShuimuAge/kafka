/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.controller

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}
import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._
import scala.collection.JavaConverters._

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

trait ControllerEventProcessor {
  def process(event: ControllerEvent): Unit
  def preempt(event: ControllerEvent): Unit
}

//KafkaController使用单线程基于事件队列的模型，将每个事件都做一层封装
//主要是记录了下入队时间，并且提供了事件需要调用的方法
class QueuedEvent(val event: ControllerEvent,
                  val enqueueTimeMs: Long) {
  val processingStarted = new CountDownLatch(1)
  val spent = new AtomicBoolean(false)

  def process(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processingStarted.countDown()
    processor.process(event)
  }

  def preempt(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processor.preempt(event)
  }

  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

//zk事件管理器
class ControllerEventManager(controllerId: Int,
                             processor: ControllerEventProcessor,
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, KafkaTimer]) extends KafkaMetricsGroup {
  import ControllerEventManager._

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  /**
   * 事件队列
   * Kafka的控制器使用单线程基于事件队列的模型，将每个事件都做一层封装，
   * 然后按照事件发生的先后顺序暂存到LinkedBlockingQueue中，
   * 最后使用一个专用的线程（ControllerEventThread）按照FIFO的原则顺序处理各个事件，
   * 这样就可以不需要锁机制就可以在多线程间维护线程安全
   */
  private val queue = new LinkedBlockingQueue[QueuedEvent]
  // Visible for test
  private[controller] val thread = new ControllerEventThread(ControllerEventThreadName)

  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  newGauge(EventQueueSizeMetricName, () => queue.size)

  def state: ControllerState = _state

  //启动eventManager的ControllerEventThread线程来处理event queue中的任务ControllerEvent
  def start(): Unit = thread.start()

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      clearAndPut(ShutdownEventThread)
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }
  }

  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    queue.put(queuedEvent)
    queuedEvent
  }

  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    queue.asScala.foreach(_.preempt(processor))
    queue.clear()
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      val dequeued = queue.take()
      dequeued.event match {
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          _state = controllerEvent.state

          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            def process(): Unit = dequeued.process(processor)

            rateAndTimeMetrics.get(state) match {
              case Some(timer) => timer.time { process() }
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

}
