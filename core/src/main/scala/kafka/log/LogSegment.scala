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
package kafka.log

import java.io.{File, IOException}
import java.nio.file.{Files, NoSuchFileException}
import java.nio.file.attribute.FileTime
import java.util.concurrent.TimeUnit

import kafka.common.LogSegmentOffsetOverflowException
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{FetchDataInfo, LogOffsetMetadata}
import kafka.utils._
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.errors.CorruptRecordException
import org.apache.kafka.common.record.FileRecords.{LogOffsetPosition, TimestampAndOffset}
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.math._

/**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileRecords containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 *
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
 *
 * @param log The file records containing log entries
 * @param lazyOffsetIndex The offset index
 * @param lazyTimeIndex The timestamp index
 * @param txnIndex The transaction index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param rollJitterMs The maximum random jitter subtracted from the scheduled segment roll time
 * @param time The time instance
 */
@nonthreadsafe
//日志段数据结构，其内部保存了实际保存消息数据的文件对象
//主要包括:
//  1.日志文件，实际消息存储文件 FileRecords，
//  2.偏移量索引文件OffsetIndex、
//  3.时间戳索引文件TimeIndex 等
//日志文件和两个索引文件都是根据基准偏移量(baseOffset)命名的，名称固定为20位，例如：
//00000000000000000000.log/00000000000000000000.index/00000000000000000000.timeindex
class LogSegment private[log] (val log: FileRecords,                         //日志文件，实际保存 Kafka 消息的对象，是实际消息存储文件(以".log"结尾)
                               val lazyOffsetIndex: LazyIndex[OffsetIndex],  //偏移量索引文件OffsetIndex(以".index"结尾)
                               val lazyTimeIndex: LazyIndex[TimeIndex],      //时间戳索引文件TimeIndex(以".timeindex"结尾)
                               val txnIndex: TransactionIndex,               //已中止事务索引文件
                               val baseOffset: Long,                         //每个LogSegment对象都有一个起始偏移量baseOffset，表示第一条消息的offset值，每个LogSegment对象实例一旦被创建，它的起始位移就是固定的了，不能再被更改，64位long
                               val indexIntervalBytes: Int,                  //索引项之间间隔的最小字节数，就是 Broker 端参数log.index.interval.bytes 值，它控制了日志段对象新增索引项的频率。每当写入这么多消息时，索引量和偏移量文件分别增加一个偏移量和时间戳索引项，默认4KB
                               val rollJitterMs: Long,                       // log.segment.ms,以时间为维度切分segment。那配置了这个参数之后如果有很多很多分区，然后因为这个参数是全局的，因此同一时刻需要做很多文件的切分，这磁盘IO就顶不住了，因此需要设置个rollJitterMs，来岔开它们
                               val time: Time) extends Logging {

  //偏移量索引文件OffsetIndex
  def offsetIndex: OffsetIndex = lazyOffsetIndex.get

  //时间戳索引文件TimeIndex
  def timeIndex: TimeIndex = lazyTimeIndex.get

  // 判断是否需要roll一个新的segments段
  def shouldRoll(rollParams: RollParams): Boolean = {
    val reachedRollMs = timeWaitedForRoll(rollParams.now, rollParams.maxTimestampInMessages) > rollParams.maxSegmentMs - rollJitterMs
    size > rollParams.maxSegmentBytes - rollParams.messagesSize ||                                                // 当前 activeSegment 在追加本次消息之后，长度超过 LogSegment 允许的最大值
      (size > 0 && reachedRollMs) ||                                                                              // 当前 activeSegment 的存活时间超过了允许的最大时间
      offsetIndex.isFull || timeIndex.isFull ||                                                                   // 索引文件满了
      // canConvertToRelativeOffset 判断新append的message是否能放进去，判断是否能转化为相对offset
      // 即判断val relativeOffset = rollParams.maxOffsetInMessages - baseOffset
      !canConvertToRelativeOffset(rollParams.maxOffsetInMessages)
  }

  def resizeIndexes(size: Int): Unit = {
    offsetIndex.resize(size)
    timeIndex.resize(size)
  }

  def sanityCheck(timeIndexFileNewlyCreated: Boolean): Unit = {
    if (lazyOffsetIndex.file.exists) {
      // Resize the time index file to 0 if it is newly created.
      if (timeIndexFileNewlyCreated)
        timeIndex.resize(0)
      // Sanity checks for time index and offset index are skipped because
      // we will recover the segments above the recovery point in recoverLog()
      // in any case so sanity checking them here is redundant.
      txnIndex.sanityCheck()
    }
    else throw new NoSuchFileException(s"Offset index file ${lazyOffsetIndex.file.getAbsolutePath} does not exist")
  }

  //LogSegment的创建时间，当调用truncateTo方法吧整个日志文件清空的时候，此字段会重置为当前时间
  private var created = time.milliseconds

  /* the number of bytes since we last added an entry in the offset index */
  /** 自上次添加索引项后，在 log 文件中累计加入的消息字节数 */
  private var bytesSinceLastIndexEntry = 0

  // The timestamp we used for time based log rolling and for ensuring max compaction delay
  // volatile for LogCleaner to see the update
  // roll, 一个新的segments的时间戳
  @volatile private var rollingBasedTimestamp: Option[Long] = None

  /* The maximum timestamp we see so far */
  /** 已追加消息的最大时间戳 */
  @volatile private var _maxTimestampSoFar: Option[Long] = None
  def maxTimestampSoFar_=(timestamp: Long): Unit = _maxTimestampSoFar = Some(timestamp)
  def maxTimestampSoFar: Long = {
    if (_maxTimestampSoFar.isEmpty)
      _maxTimestampSoFar = Some(timeIndex.lastEntry.timestamp)
    _maxTimestampSoFar.get
  }

  @volatile private var _offsetOfMaxTimestampSoFar: Option[Long] = None
  def offsetOfMaxTimestampSoFar_=(offset: Long): Unit = _offsetOfMaxTimestampSoFar = Some(offset)
  def offsetOfMaxTimestampSoFar: Long = {
    if (_offsetOfMaxTimestampSoFar.isEmpty)
      _offsetOfMaxTimestampSoFar = Some(timeIndex.lastEntry.offset)
    _offsetOfMaxTimestampSoFar.get
  }

  /* Return the size in bytes of this log segment */
  /* log文件的大小 */
  def size: Int = log.sizeInBytes()

  /**
   * checks that the argument offset can be represented as an integer offset relative to the baseOffset.
   */
  def canConvertToRelativeOffset(offset: Long): Boolean = {
    offsetIndex.canAppendOffset(offset)
  }

  /**
   * Append the given messages starting with the given offset. Add
   * an entry to the index if needed.
   *
   * It is assumed this method is being called from within a lock.
   *
   * @param largestOffset The last offset in the message set
   * @param largestTimestamp The largest timestamp in the message set.
   * @param shallowOffsetOfMaxTimestamp The offset of the message that has the largest timestamp in the messages to append.
   * @param records The log entries to append.
   * @return the physical position in the file of the appended records
   * @throws LogSegmentOffsetOverflowException if the largest offset causes index offset overflow
   */
  /**
   * 从给定的起始offset加消息。如需要的话添加index文件
   * 线程不安全类，默认调用此方法前加了锁
   **/
  @nonthreadsafe
  def append(largestOffset: Long,                  //待写入消息的最大位移值
             largestTimestamp: Long,               //待写入消息的最大时间戳
             shallowOffsetOfMaxTimestamp: Long,    //最大时间戳对应消息的位移
             records: MemoryRecords): Unit = {     //待写入的实际消息集合
    // 判断是否日志段是否为空
    if (records.sizeInBytes > 0) {
      trace(s"Inserting ${records.sizeInBytes} bytes at end offset $largestOffset at position ${log.sizeInBytes} " +
            s"with largest timestamp $largestTimestamp at shallow offset $shallowOffsetOfMaxTimestamp")
      // 获取当前要写入的log日志段文件大小
      val physicalPosition = log.sizeInBytes()
      // 判断要写入的日志段log文件是否是空的，如果是空的，就一定要记录写入消息集合的最大时间戳，并将其作为后面新增日志段倒计时的依据
      if (physicalPosition == 0)
        rollingBasedTimestamp = Some(largestTimestamp)

      // 确保输入参数最大位移值是合法的,
      // 具体是判断(largestOffset - baseOffset < 0 || largestOffset - baseOffset > Int.MaxValue)
      ensureOffsetInRange(largestOffset)

      // append the messages
      // log.append执行真正的写入日志文件
      // 正式写入log文件，具体在FileRecords类中，在之后接受Log类时会分析log.append，将内存中的消息对象写入到操作系统的页缓存
      val appendedBytes = log.append(records)
      trace(s"Appended $appendedBytes to ${log.file} at end offset $largestOffset")
      // Update the in memory max timestamp and corresponding offset.
      // 更新最大时间戳和最大时间戳所属消息的位移。
      // 用作消息老化机制
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampSoFar = largestTimestamp
        offsetOfMaxTimestampSoFar = shallowOffsetOfMaxTimestamp
      }
      // append an entry to the index (if needed)
      // 更新索引项和写入的字节数，志段每写入 4KB 数据就要写入一个索引项。
      // 当已写入字节数超过了 4KB 之后，append 方法会调用索引对象的 append 方法新增索引项，同时清空已写入字节数，以备下次重新累积计算。
      if (bytesSinceLastIndexEntry > indexIntervalBytes) {
        offsetIndex.append(largestOffset, physicalPosition)
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
        bytesSinceLastIndexEntry = 0
      }
      //更新bytesSinceLastIndexEntry
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }

  private def ensureOffsetInRange(offset: Long): Unit = {
    if (!canConvertToRelativeOffset(offset))
      throw new LogSegmentOffsetOverflowException(this, offset)
  }

  private def appendChunkFromFile(records: FileRecords, position: Int, bufferSupplier: BufferSupplier): Int = {
    var bytesToAppend = 0
    var maxTimestamp = Long.MinValue
    var offsetOfMaxTimestamp = Long.MinValue
    var maxOffset = Long.MinValue
    var readBuffer = bufferSupplier.get(1024 * 1024)

    def canAppend(batch: RecordBatch) =
      canConvertToRelativeOffset(batch.lastOffset) &&
        (bytesToAppend == 0 || bytesToAppend + batch.sizeInBytes < readBuffer.capacity)

    // find all batches that are valid to be appended to the current log segment and
    // determine the maximum offset and timestamp
    val nextBatches = records.batchesFrom(position).asScala.iterator
    for (batch <- nextBatches.takeWhile(canAppend)) {
      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = batch.lastOffset
      }
      maxOffset = batch.lastOffset
      bytesToAppend += batch.sizeInBytes
    }

    if (bytesToAppend > 0) {
      // Grow buffer if needed to ensure we copy at least one batch
      if (readBuffer.capacity < bytesToAppend)
        readBuffer = bufferSupplier.get(bytesToAppend)

      readBuffer.limit(bytesToAppend)
      records.readInto(readBuffer, position)

      append(maxOffset, maxTimestamp, offsetOfMaxTimestamp, MemoryRecords.readableRecords(readBuffer))
    }

    bufferSupplier.release(readBuffer)
    bytesToAppend
  }

  /**
   * Append records from a file beginning at the given position until either the end of the file
   * is reached or an offset is found which is too large to convert to a relative offset for the indexes.
   *
   * @return the number of bytes appended to the log (may be less than the size of the input if an
   *         offset is encountered which would overflow this segment)
   */
  def appendFromFile(records: FileRecords, start: Int): Int = {
    var position = start
    val bufferSupplier: BufferSupplier = new BufferSupplier.GrowableBufferSupplier
    while (position < start + records.sizeInBytes) {
      val bytesAppended = appendChunkFromFile(records, position, bufferSupplier)
      if (bytesAppended == 0)
        return position - start
      position += bytesAppended
    }
    position - start
  }

  @nonthreadsafe
  def updateTxnIndex(completedTxn: CompletedTxn, lastStableOffset: Long): Unit = {
    if (completedTxn.isAborted) {
      trace(s"Writing aborted transaction $completedTxn to transaction index, last stable offset is $lastStableOffset")
      txnIndex.append(new AbortedTxn(completedTxn, lastStableOffset))
    }
  }

  private def updateProducerState(producerStateManager: ProducerStateManager, batch: RecordBatch): Unit = {
    if (batch.hasProducerId) {
      val producerId = batch.producerId
      val appendInfo = producerStateManager.prepareUpdate(producerId, origin = AppendOrigin.Replication)
      val maybeCompletedTxn = appendInfo.append(batch, firstOffsetMetadataOpt = None)
      producerStateManager.update(appendInfo)
      maybeCompletedTxn.foreach { completedTxn =>
        val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
        updateTxnIndex(completedTxn, lastStableOffset)
        producerStateManager.completeTxn(completedTxn)
      }
    }
    producerStateManager.updateMapEndOffset(batch.lastOffset + 1)
  }

  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   *
   * The startingFilePosition argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   *
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * @return The position in the log storing the message with the least offset >= the requested offset and the size of the
    *        message or null if no message meets this criteria.
   */
  // 将位移索引转换成物理文件位置索引
  //这个是根据索引信息找到对应的物理文件位置才开始读取消息，查询是基于二分查找发
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogOffsetPosition = {
    // 基于二分查找获取小于等于参数 offset 的最大 offset，返回 offset 与对应的物理地址
    val mapping = offsetIndex.lookup(offset)
    // 查找对应的物理地址 position，找到map中对应的batch，后面会用到这个batch的起始位置
    log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
  }

  /**
   * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
   * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read
   * @param maxSize The maximum number of bytes to include in the message set we read
   * @param maxPosition The maximum position in the log segment that should be exposed for read
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxSize` (if one exists)
   *
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   */
  /**从第一个比入参startOffset的值大的offset值开始读取，读取的最大大小为maxSize，最大条数为maxOffset
    *read方法读取log中的数据，分为三步：
    1. 查找索引确定读取物理文件位置
    2. 计算要读取的总字节数
    3. 读取消息
   */
  @threadsafe
  def read(startOffset: Long,                     //要读取的第一条消息的位移
           maxSize: Int,                          //能读取的最大字节数
           maxPosition: Long = size,              //能读到的最大文件位置
           minOneMessage: Boolean = false         //是否允许在消息体过大时至少返回第一条消息
          ): FetchDataInfo = {                    //返回对象包括：消息集合、第一条offset值大于入参startOffset的消息的metadata对象。如果入参startOffset大于log文件的最大offset，返回空。
    if (maxSize < 0)
      throw new IllegalArgumentException(s"Invalid max size $maxSize for log read from segment $log")

    // 将位移索引转换成物理文件位置索引
    // 调用 translateOffset()定位要读取的起始文件位置
    // startOffset 仅仅是位移值，Kafka 需要根据索引信息找到对应的物理文件位置才能开始读取消息，调用translateOffset方法获取
    val startOffsetAndSize = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    if (startOffsetAndSize == null)
      return null

    val startPosition = startOffsetAndSize.position
    // offsetMetadata对象，里面主要成员为：消息offset、消息所在的segment的baseOffset、所在的segment的物理位置
    val offsetMetadata = LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    // 根据minOneMessage参数，调整此次读取的最大size
    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    // return a log segment but with zero size in the case below
    if (adjustedMaxSize == 0)
      return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // calculate the length of the message set to read based on whether or not they gave us a maxOffset
    // 计算fetch的最大size,即要读取的总字节数
    val fetchSize: Int = min((maxPosition - startPosition).toInt, adjustedMaxSize)

    // 用log.slice方法从指定位置读取指定大小的消息集合后，封装成FetchDataInfo返回。
    // firstEntryIncomplete为是否整条消息都读完，可能是maxSize小于第一条消息的大小
    FetchDataInfo(offsetMetadata, log.slice(startPosition, fetchSize),
      firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
  }

   def fetchUpperBoundOffset(startOffsetPosition: OffsetPosition, fetchSize: Int): Option[Long] =
     offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize).map(_.offset)

  /**
   * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes
   * from the end of the log and index.
   *
   * @param producerStateManager Producer state corresponding to the segment's base offset. This is needed to recover
   *                             the transaction index.
   * @param leaderEpochCache Optionally a cache for updating the leader epoch during recovery.
   * @return The number of bytes truncated from the log
   * @throws LogSegmentOffsetOverflowException if the log segment contains an offset that causes the index offset to overflow
   */
  /**恢复日志段，Broker 在启动时会从磁盘上加载所有日志段信息到内存中，并创建相应的 LogSegment 对象实例。
    1. 情况索引文件
    2. 遍历日志段中所有的消息集合：
      A. 检验消息集合
      B. 保存最大时间戳和所属消息位移
      C. 更新索引项
      D. 更新总消息字节数
      E. 更细leaderEpoche
    3. 执行消息索引文件截断。
  */
  //根据日志文件重新索引文件
  @nonthreadsafe
  def recover(producerStateManager: ProducerStateManager, leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = {
    offsetIndex.reset()
    timeIndex.reset()
    txnIndex.reset()
    //记录了已经通过验证的字节数
    var validBytes = 0
    //最后一个索引项对应的物理地址
    var lastIndexEntry = 0
    maxTimestampSoFar = RecordBatch.NO_TIMESTAMP
    try {
      // 遍历每个log中的每个batches
      for (batch <- log.batches.asScala) {
        // 该集合中的消息必须要符合 Kafka 定义的二进制格式
        // 消息头字段大小必须保证 >= 14
        batch.ensureValid()
        // 该集合中最后一条消息的位移值不能越界，即它与日志段起始位移的差值必须是一个正整数值
        ensureOffsetInRange(batch.lastOffset)

        // The max timestamp is exposed at the batch level, so no need to iterate the records
        // 更新当前的最大时间戳以及所属消息的位移值
        if (batch.maxTimestamp > maxTimestampSoFar) {
          maxTimestampSoFar = batch.maxTimestamp
          offsetOfMaxTimestampSoFar = batch.lastOffset
        }

        // Build offset index
        // 建立索引项，如果当前的所有读取的byte 减去 上次建索引文件的bytes 大于 indexIntervalBytes(默认配置)，建立新索引
        if (validBytes - lastIndexEntry > indexIntervalBytes) {
          //添加索引项
          offsetIndex.append(batch.lastOffset, validBytes)
          timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar)
          lastIndexEntry = validBytes
        }
        //累加validBytes，更新总消息字节数
        validBytes += batch.sizeInBytes()

        // 更新Porducer状态和Leader Epoch缓存
        // 1. magic是消息格式版本，总有3个版本。V2是最新版本
        // 2. leader epoch是controller分配给分区leader副本的版本号。
        //    每个消息批次都要有对应的leader epoch。Kafka会记录每个分区leader不同epoch对应的首条消息的位移。
        //    比如leader epoch=0时producer写入了100条消息，那么cache会记录<0, 0>，之后leader变更，epoch增加到1，之后producer又写入了200条消息，那么cache会记录<1, 100>。
        //    epoch主要用于做日志截断时保证一致性用的，单纯依赖HW值可能出现各种不一致的情况。这是社区对于HW值的一个修正机制
        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
          leaderEpochCache.foreach { cache =>
            if (batch.partitionLeaderEpoch > 0 && cache.latestEpoch.forall(batch.partitionLeaderEpoch > _))
              cache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
          }
          updateProducerState(producerStateManager, batch)
        }
      }
    } catch {
      case e@ (_: CorruptRecordException | _: InvalidRecordException) =>
        warn("Found invalid messages in log segment %s at byte offset %d: %s. %s"
          .format(log.file.getAbsolutePath, validBytes, e.getMessage, e.getCause))
    }
    //对日志文件进行截断，抛弃后面验证失败的message
    // Kafka 会将日志段当前总字节数(直接从.log文件获取)，和刚刚累加的已读取字节数进行比较
    // 如果发现前者比后者大，说明日志段写入了一些非法消息，需要执行截断操作
    // 日志文件写入了消息的部分字节然后broker宕机。磁盘是块设备，它可不能保证消息的全部字节要么全部写入，要么全都不写入。
    // 因此Kafka必须有机制应对这种情况，即校验+truncate。
    // ps：truncate是强行截取log文件的指定大小，从末尾处直接截取。如果是因为网络抖动导致中间某些字节丢失或写入错误字节，
    // 会出现消息集合CRC校验值发生变更，这个检查不是在log这一层级执行的，而是在底层的消息集合或消息批次这个level执行的。
    val truncated = log.sizeInBytes - validBytes
    if (truncated > 0)
      debug(s"Truncated $truncated invalid bytes at the end of segment ${log.file.getAbsoluteFile} during recovery")

    //执行日志截断操作
    log.truncateTo(validBytes)
    // 同时， 必须相应地调整索引文件的大小
    offsetIndex.trimToValidSize()
    // A normally closed segment always appends the biggest timestamp ever seen into log segment, we do this as well.
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true)
    timeIndex.trimToValidSize()
    truncated
  }

  private def loadLargestTimestamp(): Unit = {
    // Get the last time index entry. If the time index is empty, it will return (-1, baseOffset)
    val lastTimeIndexEntry = timeIndex.lastEntry
    maxTimestampSoFar = lastTimeIndexEntry.timestamp
    offsetOfMaxTimestampSoFar = lastTimeIndexEntry.offset

    val offsetPosition = offsetIndex.lookup(lastTimeIndexEntry.offset)
    // Scan the rest of the messages to see if there is a larger timestamp after the last time index entry.
    val maxTimestampOffsetAfterLastEntry = log.largestTimestampAfter(offsetPosition.position)
    if (maxTimestampOffsetAfterLastEntry.timestamp > lastTimeIndexEntry.timestamp) {
      maxTimestampSoFar = maxTimestampOffsetAfterLastEntry.timestamp
      offsetOfMaxTimestampSoFar = maxTimestampOffsetAfterLastEntry.offset
    }
  }

  /**
   * Check whether the last offset of the last batch in this segment overflows the indexes.
   */
  def hasOverflow: Boolean = {
    val nextOffset = readNextOffset
    nextOffset > baseOffset && !canConvertToRelativeOffset(nextOffset - 1)
  }

  def collectAbortedTxns(fetchOffset: Long, upperBoundOffset: Long): TxnIndexSearchResult =
    txnIndex.collectAbortedTxns(fetchOffset, upperBoundOffset)

  override def toString: String = "LogSegment(baseOffset=" + baseOffset +
    ", size=" + size +
    ", lastModifiedTime=" + lastModified +
    ", largestTime=" + largestTimestamp +
    ")"

  /**
   * Truncate off all index and log entries with offsets >= the given offset.
   * If the given offset is larger than the largest message in this segment, do nothing.
   *
   * @param offset The offset to truncate to
   * @return The number of log bytes truncated
   */
  //将日志段中的数据强制截断到指定的位移处
  @nonthreadsafe
  def truncateTo(offset: Long): Int = {
    // Do offset translation before truncating the index to avoid needless scanning
    // in case we truncate the full index
    // 将位置值转换成物理文件位置
    val mapping = translateOffset(offset)
    // 移动索引到指定位置:
    //偏移量索引文件OffsetIndex
    offsetIndex.truncateTo(offset)
    //时间戳索引文件TimeIndex
    timeIndex.truncateTo(offset)
    txnIndex.truncateTo(offset)

    // After truncation, reset and allocate more space for the (new currently active) index
    // 因为位置变了，为了节省内存，做一次resize操作
    offsetIndex.resize(offsetIndex.maxIndexSize)
    timeIndex.resize(timeIndex.maxIndexSize)

    val bytesTruncated = if (mapping == null) 0 else log.truncateTo(mapping.position)
    // 如果调整到初始位置，那么重新记录一下创建时间
    if (log.sizeInBytes == 0) {
      created = time.milliseconds
      rollingBasedTimestamp = None
    }

    //调整索引项
    bytesSinceLastIndexEntry = 0
    //调整最大的索引位置
    if (maxTimestampSoFar >= 0)
      loadLargestTimestamp()
    bytesTruncated
  }

  /**
   * Calculate the offset that would be used for the next message to be append to this segment.
   * Note that this is expensive.
   */
  @threadsafe
  def readNextOffset: Long = {
    val fetchData = read(offsetIndex.lastOffset, log.sizeInBytes)
    if (fetchData == null)
      baseOffset
    else
      fetchData.records.batches.asScala.lastOption
        .map(_.nextOffset)
        .getOrElse(baseOffset)
  }

  /**
   * Flush this log segment to disk
   */
  @threadsafe
  def flush(): Unit = {
    LogFlushStats.logFlushTimer.time {
      log.flush()
      offsetIndex.flush()
      timeIndex.flush()
      txnIndex.flush()
    }
  }

  /**
   * Update the directory reference for the log and indices in this segment. This would typically be called after a
   * directory is renamed.
   */
  def updateDir(dir: File): Unit = {
    log.setFile(new File(dir, log.file.getName))
    lazyOffsetIndex.file = new File(dir, lazyOffsetIndex.file.getName)
    lazyTimeIndex.file = new File(dir, lazyTimeIndex.file.getName)
    txnIndex.file = new File(dir, txnIndex.file.getName)
  }

  /**
   * Change the suffix for the index and log file for this log segment
   * IOException from this method should be handled by the caller
   */
  def changeFileSuffixes(oldSuffix: String, newSuffix: String): Unit = {
    log.renameTo(new File(CoreUtils.replaceSuffix(log.file.getPath, oldSuffix, newSuffix)))
    offsetIndex.renameTo(new File(CoreUtils.replaceSuffix(lazyOffsetIndex.file.getPath, oldSuffix, newSuffix)))
    timeIndex.renameTo(new File(CoreUtils.replaceSuffix(lazyTimeIndex.file.getPath, oldSuffix, newSuffix)))
    txnIndex.renameTo(new File(CoreUtils.replaceSuffix(txnIndex.file.getPath, oldSuffix, newSuffix)))
  }

  /**
   * Append the largest time index entry to the time index and trim the log and indexes.
   *
   * The time index entry appended will be used to decide when to delete the segment.
   */
  def onBecomeInactiveSegment(): Unit = {
    timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true)
    offsetIndex.trimToValidSize()
    timeIndex.trimToValidSize()
    log.trim()
  }

  /**
    * If not previously loaded,
    * load the timestamp of the first message into memory.
    */
  private def loadFirstBatchTimestamp(): Unit = {
    if (rollingBasedTimestamp.isEmpty) {
      val iter = log.batches.iterator()
      if (iter.hasNext)
        rollingBasedTimestamp = Some(iter.next().maxTimestamp)
    }
  }

  /**
   * The time this segment has waited to be rolled.
   * If the first message batch has a timestamp we use its timestamp to determine when to roll a segment. A segment
   * is rolled if the difference between the new batch's timestamp and the first batch's timestamp exceeds the
   * segment rolling time.
   * If the first batch does not have a timestamp, we use the wall clock time to determine when to roll a segment. A
   * segment is rolled if the difference between the current wall clock time and the segment create time exceeds the
   * segment rolling time.
   */
  /**判断此segment是否需要rolled
   * 1. 如果此segment的第一个消息的时间戳存在，就用当前的新的batch的时间戳，减去此segment第一条消息的的时间戳判断是否已经超过segments.ms
   * 2. 如果此segments的第一个消息的时间戳不存在，就用系统时间与此segment创建的时间差判断。
   */
  def timeWaitedForRoll(now: Long, messageTimestamp: Long) : Long = {
    // Load the timestamp of the first message into memory
    // loadFirstBatchTimestamp如果此segment的第一个消息的时间戳存在，rollingBasedTimestamp置为此时间戳。
    // 如果不存在，rollingBasedTimestamp本身就是此segment创建的时间，不用操作。
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => messageTimestamp - t
      case _ => now - created
    }
  }

  /**
    * @return the first batch timestamp if the timestamp is available. Otherwise return Long.MaxValue
    */
  def getFirstBatchTimestamp() : Long = {
    loadFirstBatchTimestamp()
    rollingBasedTimestamp match {
      case Some(t) if t >= 0 => t
      case _ => Long.MaxValue
    }
  }

  /**
   * Search the message offset based on timestamp and offset.
   *
   * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
   *
   * - If all the messages in the segment have smaller offsets, return None
   * - If all the messages in the segment have smaller timestamps, return None
   * - If all the messages in the segment have larger timestamps, or no message in the segment has a timestamp
   *   the returned the offset will be max(the base offset of the segment, startingOffset) and the timestamp will be Message.NoTimestamp.
   * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
   *   is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
   *
   * This methods only returns None when 1) all messages' offset < startOffing or 2) the log is not empty but we did not
   * see any message when scanning the log from the indexed position. The latter could happen if the log is truncated
   * after we get the indexed position but before we scan the log from there. In this case we simply return None and the
   * caller will need to check on the truncated log and maybe retry or even do the search on another log segment.
   *
   * @param timestamp The timestamp to search for.
   * @param startingOffset The starting offset to search.
   * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there is no such message.
   */
  def findOffsetByTimestamp(timestamp: Long, startingOffset: Long = baseOffset): Option[TimestampAndOffset] = {
    // Get the index entry with a timestamp less than or equal to the target timestamp
    val timestampOffset = timeIndex.lookup(timestamp)
    val position = offsetIndex.lookup(math.max(timestampOffset.offset, startingOffset)).position

    // Search the timestamp
    Option(log.searchForTimestamp(timestamp, position, startingOffset))
  }

  /**
   * Close this log segment
   */
  def close(): Unit = {
    CoreUtils.swallow(timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestampSoFar, skipFullCheck = true), this)
    CoreUtils.swallow(offsetIndex.close(), this)
    CoreUtils.swallow(timeIndex.close(), this)
    CoreUtils.swallow(log.close(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
    * Close file handlers used by the log segment but don't write to disk. This is used when the disk may have failed
    */
  def closeHandlers(): Unit = {
    CoreUtils.swallow(offsetIndex.closeHandler(), this)
    CoreUtils.swallow(timeIndex.closeHandler(), this)
    CoreUtils.swallow(log.closeHandlers(), this)
    CoreUtils.swallow(txnIndex.close(), this)
  }

  /**
   * Delete this log segment from the filesystem.
   */
  def deleteIfExists(): Unit = {
    def delete(delete: () => Boolean, fileType: String, file: File, logIfMissing: Boolean): Unit = {
      try {
        if (delete())
          info(s"Deleted $fileType ${file.getAbsolutePath}.")
        else if (logIfMissing)
          info(s"Failed to delete $fileType ${file.getAbsolutePath} because it does not exist.")
      }
      catch {
        case e: IOException => throw new IOException(s"Delete of $fileType ${file.getAbsolutePath} failed.", e)
      }
    }

    CoreUtils.tryAll(Seq(
      () => delete(log.deleteIfExists _, "log", log.file, logIfMissing = true),
      () => delete(offsetIndex.deleteIfExists _, "offset index", lazyOffsetIndex.file, logIfMissing = true),
      () => delete(timeIndex.deleteIfExists _, "time index", lazyTimeIndex.file, logIfMissing = true),
      () => delete(txnIndex.deleteIfExists _, "transaction index", txnIndex.file, logIfMissing = false)
    ))
  }

  /**
   * The last modified time of this log segment as a unix time stamp
   */
  def lastModified = log.file.lastModified

  /**
   * The largest timestamp this segment contains.
   */
  def largestTimestamp = if (maxTimestampSoFar >= 0) maxTimestampSoFar else lastModified

  /**
   * Change the last modified time for this log segment
   */
  def lastModified_=(ms: Long) = {
    val fileTime = FileTime.fromMillis(ms)
    Files.setLastModifiedTime(log.file.toPath, fileTime)
    Files.setLastModifiedTime(lazyOffsetIndex.file.toPath, fileTime)
    Files.setLastModifiedTime(lazyTimeIndex.file.toPath, fileTime)
  }

}

object LogSegment {

  def open(dir: File, baseOffset: Long, config: LogConfig, time: Time, fileAlreadyExists: Boolean = false,
           initFileSize: Int = 0, preallocate: Boolean = false, fileSuffix: String = ""): LogSegment = {
    val maxIndexSize = config.maxIndexSize
    new LogSegment(
      FileRecords.open(Log.logFile(dir, baseOffset, fileSuffix), fileAlreadyExists, initFileSize, preallocate),
      LazyIndex.forOffset(Log.offsetIndexFile(dir, baseOffset, fileSuffix), baseOffset = baseOffset, maxIndexSize = maxIndexSize),
      LazyIndex.forTime(Log.timeIndexFile(dir, baseOffset, fileSuffix), baseOffset = baseOffset, maxIndexSize = maxIndexSize),
      new TransactionIndex(baseOffset, Log.transactionIndexFile(dir, baseOffset, fileSuffix)),
      baseOffset,
      indexIntervalBytes = config.indexInterval,
      rollJitterMs = config.randomSegmentJitter,
      time)
  }

  def deleteIfExists(dir: File, baseOffset: Long, fileSuffix: String = ""): Unit = {
    Log.deleteFileIfExists(Log.offsetIndexFile(dir, baseOffset, fileSuffix))
    Log.deleteFileIfExists(Log.timeIndexFile(dir, baseOffset, fileSuffix))
    Log.deleteFileIfExists(Log.transactionIndexFile(dir, baseOffset, fileSuffix))
    Log.deleteFileIfExists(Log.logFile(dir, baseOffset, fileSuffix))
  }
}

object LogFlushStats extends KafkaMetricsGroup {
  val logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
}
