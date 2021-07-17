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

import java.io._
import java.nio.file.Files
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.{BrokerState, RecoveringFromUncleanShutdown, _}
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.errors.{KafkaStorageException, LogDirNotFoundException}

import scala.jdk.CollectionConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 *
 * A background thread handles log retention by periodically truncating excess log segments.

 * logManager负责log的创建, 重试, 清理.
 * 把所有的读写请求导入到独立的的log对象上去.

 * 因为logDir可以配多个, 然后logManager创建新log(partition)的时候会选择数据量最少的文件夹.

 * 有一个后台线程负责定期清洗log segments.
 */
@threadsafe
class LogManager(
                  logDirs: Seq[File],
                  initialOfflineDirs: Seq[File],
                  val topicConfigs: Map[String, LogConfig], // note that this doesn't get updated after creation
                  val initialDefaultConfig: LogConfig,
                  val cleanerConfig: CleanerConfig,
                  recoveryThreadsPerDataDir: Int,
                  val flushCheckMs: Long,
                  val flushRecoveryOffsetCheckpointMs: Long,
                  val flushStartOffsetCheckpointMs: Long,
                  val retentionCheckMs: Long,
                  val maxPidExpirationMs: Int,
                  scheduler: Scheduler,
                  val brokerState: BrokerState,
                  brokerTopicStats: BrokerTopicStats,
                  logDirFailureChannel: LogDirFailureChannel,
                  time: Time) extends Logging with KafkaMetricsGroup {

  import LogManager._

  val LockFile = ".lock"
  val InitialTaskDelayMs = 30 * 1000

  private val logCreationOrDeletionLock = new Object
  // 这个看起来是维护了所有的tp和对应的log对象
  private val currentLogs = new Pool[TopicPartition, Log]()
  // Future logs are put in the directory with "-future" suffix. Future log is created when user wants to move replica
  // from one log directory to another log directory on the same broker. The directory of the future log will be renamed
  // to replace the current log of the partition after the future log catches up with the current log
  // 用户想要移动partitionLog文件了, 就会用到这个futureLogs集合, 这个应该是用到了双写, 等-future文件夹下的log跟上来就替换掉了.
  private val futureLogs = new Pool[TopicPartition, Log]()

  // Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion.
  // 需要干掉的Log对象, 还有要干掉的时间
  private val logsToBeDeleted = new LinkedBlockingQueue[(Log, Long)]()

  // 校验一下, 然后把正常的目录都包装成对象.
  private val _liveLogDirs: ConcurrentLinkedQueue[File] = createAndValidateLogDirs(logDirs, initialOfflineDirs)
  @volatile private var _currentDefaultConfig = initialDefaultConfig
  @volatile private var numRecoveryThreadsPerDataDir = recoveryThreadsPerDataDir

  // 这里面包含所有的正在生成Log的partition,
  // This map contains all partitions whose logs are getting loaded and initialized. If log configuration
  // of these partitions get updated at the same time, the corresponding entry in this map is set to "true",
  // which triggers a config reload after initialization is finished (to get the latest config value).
  // See KAFKA-8813 for more detail on the race condition
  // Visible for testing
  private[log] val partitionsInitializing = new ConcurrentHashMap[TopicPartition, Boolean]().asScala

  def reconfigureDefaultLogConfig(logConfig: LogConfig): Unit = {
    this._currentDefaultConfig = logConfig
  }

  def currentDefaultConfig: LogConfig = _currentDefaultConfig

  def liveLogDirs: Seq[File] = {
    if (_liveLogDirs.size == logDirs.size)
      logDirs
    else
      _liveLogDirs.asScala.toBuffer
  }

  private val dirLocks = lockLogDirs(liveLogDirs)
  @volatile private var recoveryPointCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel))).toMap
  @volatile private var logStartOffsetCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel))).toMap

  private val preferredLogDirs = new ConcurrentHashMap[TopicPartition, String]()

  private def offlineLogDirs: Iterable[File] = {
    val logDirsSet = mutable.Set[File]() ++= logDirs
    _liveLogDirs.forEach(dir => logDirsSet -= dir)
    logDirsSet
  }

  // 这里是把本地村的log的信息都加载出来了, 也就是把<tp, log>之类的map都搞出来, 别人就能正常读写了.
  // !注意: 只会校验一下, 然后创建出每个partition对应的Log对象, log对象里面只放着我们期待的offset, 不会真实的去校验/恢复数据.
  loadLogs()

  // 建立一个清道夫.
  private[kafka] val cleaner: LogCleaner =
    if (cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, liveLogDirs, currentLogs, logDirFailureChannel, time = time)
    else
      null

  newGauge("OfflineLogDirectoryCount", () => offlineLogDirs.size) // 暴露出一个metrics

  for (dir <- logDirs) {
    newGauge("LogDirectoryOffline",
      () => if (_liveLogDirs.contains(dir)) 0 else 1,
      Map("logDirectory" -> dir.getAbsolutePath))
  }

  /**
   * Create and check validity of the given directories that are not in the given offline directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory
   * </ol>
    一般到这里了就是一个目录, 要是有多个磁盘, 肯定就配置多个目录了.
   */
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    val liveLogDirs = new ConcurrentLinkedQueue[File]()
    val canonicalPaths = mutable.HashSet.empty[String]

    for (dir <- dirs) {
      try {
        // 校验下线的目录. 就是不能有下线的目录.
        if (initialOfflineDirs.contains(dir))
          throw new IOException(s"Failed to load ${dir.getAbsolutePath} during broker startup")

        // 目录不存在就创建, 创建不出来就报错.
        if (!dir.exists) {
          info(s"Log directory ${dir.getAbsolutePath} not found, creating it.")
          val created = dir.mkdirs()
          if (!created)
            throw new IOException(s"Failed to create data directory ${dir.getAbsolutePath}")
        }
        // 目录不能访问也会报错.
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(s"${dir.getAbsolutePath} is not a readable log directory.")

        // getCanonicalPath() throws IOException if a file system query fails or if the path is invalid (e.g. contains
        // the Nul character). Since there's no easy way to distinguish between the two cases, we treat them the same
        // and mark the log directory as offline.
        if (!canonicalPaths.add(dir.getCanonicalPath))
          throw new KafkaException(s"Duplicate log directory found: ${dirs.mkString(", ")}")


        liveLogDirs.add(dir)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Failed to create or validate data directory ${dir.getAbsolutePath}", e)
      }
    }
    if (liveLogDirs.isEmpty) {
      fatal(s"Shutdown broker because none of the specified log dirs from ${dirs.mkString(", ")} can be created or validated")
      Exit.halt(1)
    }

    liveLogDirs
  }

  def resizeRecoveryThreadPool(newSize: Int): Unit = {
    info(s"Resizing recovery thread pool size for each data dir from $numRecoveryThreadsPerDataDir to $newSize")
    numRecoveryThreadsPerDataDir = newSize
  }

  // dir should be an absolute path
  def handleLogDirFailure(dir: String): Unit = {
    warn(s"Stopping serving logs in dir $dir")
    logCreationOrDeletionLock synchronized {
      _liveLogDirs.remove(new File(dir))
      if (_liveLogDirs.isEmpty) {
        fatal(s"Shutdown broker because all log dirs in ${logDirs.mkString(", ")} have failed")
        Exit.halt(1)
      }

      recoveryPointCheckpoints = recoveryPointCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      logStartOffsetCheckpoints = logStartOffsetCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      if (cleaner != null)
        cleaner.handleLogDirFailure(dir)

      val offlineCurrentTopicPartitions = currentLogs.collect {
        case (tp, log) if log.parentDir == dir => tp
      }
      offlineCurrentTopicPartitions.foreach { topicPartition => {
        val removedLog = currentLogs.remove(topicPartition)
        if (removedLog != null) {
          removedLog.closeHandlers()
          removedLog.removeLogMetrics()
        }
      }}

      val offlineFutureTopicPartitions = futureLogs.collect {
        case (tp, log) if log.parentDir == dir => tp
      }
      offlineFutureTopicPartitions.foreach { topicPartition => {
        val removedLog = futureLogs.remove(topicPartition)
        if (removedLog != null) {
          removedLog.closeHandlers()
          removedLog.removeLogMetrics()
        }
      }}

      warn(s"Logs for partitions ${offlineCurrentTopicPartitions.mkString(",")} are offline and " +
        s"logs for future partitions ${offlineFutureTopicPartitions.mkString(",")} are offline due to failure on log directory $dir")
      dirLocks.filter(_.file.getParent == dir).foreach(dir => CoreUtils.swallow(dir.destroy(), this))
    }
  }

  /**
   * Lock all the given directories
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.flatMap { dir =>
      try {
        val lock = new FileLock(new File(dir, LockFile))
        if (!lock.tryLock())
          throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParent +
            ". A Kafka instance in another process or thread is using this directory.")
        Some(lock)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while locking directory $dir", e)
          None
      }
    }
  }

  private def addLogToBeDeleted(log: Log): Unit = {
    this.logsToBeDeleted.add((log, time.milliseconds()))
  }

  // Only for testing
  private[log] def hasLogsToBeDeleted: Boolean = !logsToBeDeleted.isEmpty

  // load一个文件夹, 一个文件夹就是一个partition.
  private def loadLog(logDir: File,
                      recoveryPoints: Map[TopicPartition, Long],
                      logStartOffsets: Map[TopicPartition, Long]): Log = {
    // 文件名字应该是 {topicName}-{partition}.log这样子
    val topicPartition = Log.parseTopicPartitionName(logDir)
    val config = topicConfigs.getOrElse(topicPartition.topic, currentDefaultConfig)
    // 恢复的起点, 如果没有就从0开始恢复.
    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
    val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)

    // 创建一个log对象, 这个其实就是一个partition一个对象, 因为logDir里面的dir就是一个partition
    val log = Log(
      dir = logDir,  // partition的文件夹目录
      config = config, // 这个是topic的配置. 从zk上拿下来的这个topic的配置
      logStartOffset = logStartOffset, // 这个partition的log开始的offset
      recoveryPoint = logRecoveryPoint, // 我们要恢复的起始点
      maxProducerIdExpirationMs = maxPidExpirationMs, // 不知道什么意思. producerId被认为过期的时间, 这个有什么用?
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      scheduler = scheduler,
      time = time,
      brokerTopicStats = brokerTopicStats, // 一个公用的状态机, 现在好像是..忘记了, 就是恢复状态?
      logDirFailureChannel = logDirFailureChannel // 一个下线目录的集合/.
    )

    // 如果当前的partition目录, 是'-delete'结尾的, 就把log对象放在delete队列里, 有个定时任务不断地在删除: kafka-delete-logs
    if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {
      addLogToBeDeleted(log)
    } else {
      val previous = {
        // 这个只是放在对应的队列里, 谁会负责加载呢?????????????????
        if (log.isFuture)
          this.futureLogs.put(topicPartition, log)
        else
          this.currentLogs.put(topicPartition, log)
      }
      if (previous != null) {
        if (log.isFuture)
          throw new IllegalStateException(s"Duplicate log directories found: ${log.dir.getAbsolutePath}, ${previous.dir.getAbsolutePath}")
        else
          throw new IllegalStateException(s"Duplicate log directories for $topicPartition are found in both ${log.dir.getAbsolutePath} " +
            s"and ${previous.dir.getAbsolutePath}. It is likely because log directory failure happened while broker was " +
            s"replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two directories " +
            s"for this partition. It is recommended to delete the partition in the log directory that is known to have failed recently.")
      }
    }

    log
  }

  /**
   * Recover and load all logs in the given data directories
   * 刚启动, 先把本地的信息恢复出来. 只会校验一下, 然后创建出每个partition对应的Log对象, log对象里面只放着我们期待的offset, 不会真实的去校验/恢复数据.
   */
  private def loadLogs(): Unit = {
    info(s"Loading logs from log dirs $liveLogDirs")
    val startMs = time.hiResClockMs()
    val threadPools = ArrayBuffer.empty[ExecutorService]
    val offlineDirs = mutable.Set.empty[(String, IOException)]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]
    var numTotalLogs = 0

    for (dir <- liveLogDirs) {
      val logDirAbsolutePath = dir.getAbsolutePath
      try {
        // 对这个目录建立一个线程池, 默认应该就是1.
        val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
        threadPools.append(pool)

        // 这应该是检查logDir里面有没有.kafka_cleanshutdown文件
        // 应该是clean关闭后, 就不恢复数据了.
        val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)
        if (cleanShutdownFile.exists) {
          info(s"Skipping recovery for all logs in $logDirAbsolutePath since clean shutdown file was found")
        } else {
          // log recovery itself is being performed by `Log` class during initialization
          info(s"Attempting recovery for all logs in $logDirAbsolutePath since no clean shutdown file was found")
          brokerState.newState(RecoveringFromUncleanShutdown)
        }

        // 这应该是读取了"recovery-point-offset-checkpoint"文件, 看看里面恢复的offset, 格式应该是<tp, offset>这样.
        var recoveryPoints = Map[TopicPartition, Long]()
        try {
          recoveryPoints = this.recoveryPointCheckpoints(dir).read
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading recovery-point-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting the recovery checkpoint to 0", e)
        }

        // 这个应该是读取了"log-start-offset-checkpoint"文件, 查看里面的startOffset什么的, 格式相似.
        var logStartOffsets = Map[TopicPartition, Long]()
        try {
          logStartOffsets = this.logStartOffsetCheckpoints(dir).read
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading log-start-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting to the base offset of the first segment", e)
        }

        // 拿到所有log
        val logsToLoad = Option(dir.listFiles).getOrElse(Array.empty).filter(_.isDirectory)
        val numLogsLoaded = new AtomicInteger(0)
        numTotalLogs += logsToLoad.length

        // 开始一个一个load了. 这个严格说只是把Log对象根据数据目录建立了起来, 只有我们期望的恢复offset, 还没有加载各种具体的.index文件里面的元数据啊什么的
        val jobsForDir = logsToLoad.map { logDir =>
          val runnable: Runnable = () => {
            try {
              debug(s"Loading log $logDir")

              val logLoadStartMs = time.hiResClockMs()
              val log = loadLog(logDir, recoveryPoints, logStartOffsets)
              val logLoadDurationMs = time.hiResClockMs() - logLoadStartMs
              val currentNumLoaded = numLogsLoaded.incrementAndGet()

              info(s"Completed load of $log with ${log.numberOfSegments} segments in ${logLoadDurationMs}ms " +
                s"($currentNumLoaded/${logsToLoad.length} loaded in $logDirAbsolutePath)")
            } catch {
              case e: IOException =>
                offlineDirs.add((logDirAbsolutePath, e))
                error(s"Error while loading log dir $logDirAbsolutePath", e)
            }
          }
          runnable
        }

        jobs(cleanShutdownFile) = jobsForDir.map(pool.submit)
      } catch {
        case e: IOException =>
          offlineDirs.add((logDirAbsolutePath, e))
          error(s"Error while loading log dir $logDirAbsolutePath", e)
      }
    }

    try {
      // 这应该是校验了一下恢复的成功, 恢复好了, 然后就把每个partition目录下的cleanShutdown文件干掉了.
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        try {
          cleanShutdownFile.delete()
        } catch {
          case e: IOException =>
            offlineDirs.add((cleanShutdownFile.getParent, e))
            error(s"Error while deleting the clean shutdown file $cleanShutdownFile", e)
        }
      }

      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while deleting the clean shutdown file in dir $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        error(s"There was an error in one of the threads during logs loading: ${e.getCause}")
        throw e.getCause
    } finally {
      // 把恢复的线程池(也就1个)关掉.
      threadPools.foreach(_.shutdown())
    }

    info(s"Loaded $numTotalLogs logs in ${time.hiResClockMs() - startMs}ms.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   * 把后台线程都启动了, 妈的
   */
  def startup(): Unit = {
    /* Schedule the cleanup task to delete old logs */
    if (scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      // 这应该是不压缩的, 过期日志都干掉
      scheduler.schedule("kafka-log-retention",
        cleanupLogs _,
        delay = InitialTaskDelayMs,
        period = retentionCheckMs,
        TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher",
        flushDirtyLogs _,
        delay = InitialTaskDelayMs,
        period = flushCheckMs,
        TimeUnit.MILLISECONDS)
      // 这个是做个一个checkpoint文件, 就是我们恢复时候读取的一个期待点, 但是我很纳闷: 数据呢?就丢掉了??
      scheduler.schedule("kafka-recovery-point-checkpoint",
        checkpointLogRecoveryOffsets _,
        delay = InitialTaskDelayMs,
        period = flushRecoveryOffsetCheckpointMs,
        TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-log-start-offset-checkpoint",
        checkpointLogStartOffsets _,
        delay = InitialTaskDelayMs,
        period = flushStartOffsetCheckpointMs,
        TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-delete-logs", // will be rescheduled after each delete logs with a dynamic period
        deleteLogs _,
        delay = InitialTaskDelayMs,
        unit = TimeUnit.MILLISECONDS)
    }
    // 这个是启动了压缩之后, 就会起一个清道夫线程.
    if (cleanerConfig.enableCleaner)
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown(): Unit = {
    info("Shutting down.")

    removeMetric("OfflineLogDirectoryCount")
    for (dir <- logDirs) {
      removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath))
    }

    val threadPools = ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown(), this)
    }

    val localLogsByDir = logsByDir

    // close logs in each dir
    for (dir <- liveLogDirs) {
      debug(s"Flushing and closing logs at $dir")

      val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
      threadPools.append(pool)

      val logsInDir = localLogsByDir.getOrElse(dir.toString, Map()).values

      val jobsForDir = logsInDir.map { log =>
        val runnable: Runnable = () => {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
        runnable
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        // update the last flush point
        debug(s"Updating recovery points at $dir")
        checkpointRecoveryOffsetsAndCleanSnapshot(dir, localLogsByDir.getOrElse(dir.toString, Map()).values.toSeq)

        debug(s"Updating log start offsets at $dir")
        checkpointLogStartOffsetsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug(s"Writing clean shutdown marker at $dir")
        CoreUtils.swallow(Files.createFile(new File(dir, Log.CleanShutdownFile).toPath), this)
      }
    } catch {
      case e: ExecutionException =>
        error(s"There was an error in one of the threads during LogManager shutdown: ${e.getCause}")
        throw e.getCause
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }

  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   * @param isFuture True iff the truncation should be performed on the future log of the specified partitions
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long], isFuture: Boolean): Unit = {
    val affectedLogs = ArrayBuffer.empty[Log]
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = {
        if (isFuture)
          futureLogs.get(topicPartition)
        else
          currentLogs.get(topicPartition)
      }
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner = cleaner != null && truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && !isFuture)
          cleaner.abortAndPauseCleaning(topicPartition)
        try {
          if (log.truncateTo(truncateOffset))
            affectedLogs += log
          if (needToStopCleaner && !isFuture)
            cleaner.maybeTruncateCheckpoint(log.parentDirFile, topicPartition, log.activeSegment.baseOffset)
        } finally {
          if (needToStopCleaner && !isFuture) {
            cleaner.resumeCleaning(Seq(topicPartition))
            info(s"Cleaning for partition $topicPartition is resumed")
          }
        }
      }
    }

    for ((dir, logs) <- affectedLogs.groupBy(_.parentDirFile)) {
      checkpointRecoveryOffsetsAndCleanSnapshot(dir, logs)
    }
  }

  /**
   * Delete all data in a partition and start the log at the new offset
   *
   * @param topicPartition The partition whose log needs to be truncated
   * @param newOffset The new offset to start the log with
   * @param isFuture True iff the truncation should be performed on the future log of the specified partition
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long, isFuture: Boolean): Unit = {
    val log = {
      if (isFuture)
        futureLogs.get(topicPartition)
      else
        currentLogs.get(topicPartition)
    }
    // If the log does not exist, skip it
    if (log != null) {
      //Abort and pause the cleaning of the log, and resume after truncation is done.
      if (cleaner != null && !isFuture)
        cleaner.abortAndPauseCleaning(topicPartition)
      try {
        log.truncateFullyAndStartAt(newOffset)
        if (cleaner != null && !isFuture) {
          cleaner.maybeTruncateCheckpoint(log.parentDirFile, topicPartition, log.activeSegment.baseOffset)
        }
      } finally {
        if (cleaner != null && !isFuture) {
          cleaner.resumeCleaning(Seq(topicPartition))
          info(s"Compaction for partition $topicPartition is resumed")
        }
      }
      checkpointRecoveryOffsetsAndCleanSnapshot(log.parentDirFile, Seq(log))
    }
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory
   * to avoid recovering the whole log on startup.
   */
  def checkpointLogRecoveryOffsets(): Unit = {
    logsByDir.foreach { case (dir, partitionToLogMap) =>
      liveLogDirs.find(_.getAbsolutePath.equals(dir)).foreach { f =>
        checkpointRecoveryOffsetsAndCleanSnapshot(f, partitionToLogMap.values.toSeq)
      }
    }
  }

  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   */
  def checkpointLogStartOffsets(): Unit = {
    liveLogDirs.foreach(checkpointLogStartOffsetsInDir)
  }

  /**
   * Write the recovery checkpoint file for all logs in provided directory and clean older snapshots for provided logs.
   *
   * @param dir the directory in which logs are checkpointed
   * @param logsToCleanSnapshot logs whose snapshots need to be cleaned
   */
  // Only for testing
  private[log] def checkpointRecoveryOffsetsAndCleanSnapshot(dir: File, logsToCleanSnapshot: Seq[Log]): Unit = {
    try {
      checkpointLogRecoveryOffsetsInDir(dir)
      logsToCleanSnapshot.foreach(_.deleteSnapshotsAfterRecoveryPointCheckpoint())
    } catch {
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while writing to recovery point " +
          s"file in directory $dir", e)
    }
  }

  private def checkpointLogRecoveryOffsetsInDir(dir: File): Unit = {
    for {
      partitionToLog <- logsByDir.get(dir.getAbsolutePath)
      checkpoint <- recoveryPointCheckpoints.get(dir)
    } {
      checkpoint.write(partitionToLog.map { case (tp, log) => tp -> log.recoveryPoint })
    }
  }

  /**
   * Checkpoint log start offset for all logs in provided directory.
   */
  private def checkpointLogStartOffsetsInDir(dir: File): Unit = {
    for {
      partitionToLog <- logsByDir.get(dir.getAbsolutePath)
      checkpoint <- logStartOffsetCheckpoints.get(dir)
    } {
      try {
        val logStartOffsets = partitionToLog.collect {
          case (k, log) if log.logStartOffset > log.logSegments.head.baseOffset => k -> log.logStartOffset
        }
        checkpoint.write(logStartOffsets)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while writing to logStartOffset file in directory $dir", e)
      }
    }
  }

  // The logDir should be an absolute path
  def maybeUpdatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // Do not cache the preferred log directory if either the current log or the future log for this partition exists in the specified logDir
    if (!getLog(topicPartition).exists(_.parentDir == logDir) &&
      !getLog(topicPartition, isFuture = true).exists(_.parentDir == logDir))
      preferredLogDirs.put(topicPartition, logDir)
  }

  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null)
      cleaner.abortAndPauseCleaning(topicPartition)
  }


  /**
   * Get the log if it exists, otherwise return None
   *
   * @param topicPartition the partition of the log
   * @param isFuture True iff the future log of the specified partition should be returned
   */
  def getLog(topicPartition: TopicPartition, isFuture: Boolean = false): Option[Log] = {
    if (isFuture)
      Option(futureLogs.get(topicPartition))
    else
      Option(currentLogs.get(topicPartition))
  }

  /**
   * Method to indicate that logs are getting initialized for the partition passed in as argument.
   * This method should always be followed by [[kafka.log.LogManager#finishedInitializingLog]] to indicate that log
   * initialization is done.
   */
  def initializingLog(topicPartition: TopicPartition): Unit = {
    partitionsInitializing(topicPartition) = false
  }

  /**
   * Mark the partition configuration for all partitions that are getting initialized for topic
   * as dirty. That will result in reloading of configuration once initialization is done.
   */
  def topicConfigUpdated(topic: String): Unit = {
    partitionsInitializing.keys.filter(_.topic() == topic).foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Mark all in progress partitions having dirty configuration if broker configuration is updated.
   */
  def brokerConfigUpdated(): Unit = {
    partitionsInitializing.keys.foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Method to indicate that the log initialization for the partition passed in as argument is
   * finished. This method should follow a call to [[kafka.log.LogManager#initializingLog]].
   *
   * It will retrieve the topic configs a second time if they were updated while the
   * relevant log was being loaded.
   */
  def finishedInitializingLog(topicPartition: TopicPartition,
                              maybeLog: Option[Log],
                              fetchLogConfig: () => LogConfig): Unit = {
    val removedValue = partitionsInitializing.remove(topicPartition)
    if (removedValue.contains(true))
      maybeLog.foreach(_.updateConfig(fetchLogConfig()))
  }

  /**
   * If the log already exists, just return a copy of the existing log
   * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
   * Otherwise throw KafkaStorageException
   *
   * @param topicPartition The partition whose log needs to be returned or created
   * @param loadConfig A function to retrieve the log config, this is only called if the log is created
   * @param isNew Whether the replica should have existed on the broker or not
   * @param isFuture True if the future log of the specified partition should be returned or created
   * @throws KafkaStorageException if isNew=false, log is not found in the cache and there is offline log directory on the broker
   */
  def getOrCreateLog(topicPartition: TopicPartition, loadConfig: () => LogConfig, isNew: Boolean = false, isFuture: Boolean = false): Log = {
    logCreationOrDeletionLock synchronized {
      getLog(topicPartition, isFuture).getOrElse {
        // create the log if it has not already been created in another thread
        if (!isNew && offlineLogDirs.nonEmpty)
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")

        val logDirs: List[File] = {
          val preferredLogDir = preferredLogDirs.get(topicPartition)

          if (isFuture) {
            if (preferredLogDir == null)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition without having a preferred log directory")
            else if (getLog(topicPartition).get.parentDir == preferredLogDir)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition in the current log directory of this partition")
          }

          if (preferredLogDir != null)
            List(new File(preferredLogDir))
          else
            nextLogDirs()
        }

        val logDirName = {
          if (isFuture)
            Log.logFutureDirName(topicPartition)
          else
            Log.logDirName(topicPartition)
        }

        val logDir = logDirs
          .iterator // to prevent actually mapping the whole list, lazy map
          .map(createLogDirectory(_, logDirName))
          .find(_.isSuccess)
          .getOrElse(Failure(new KafkaStorageException("No log directories available. Tried " + logDirs.map(_.getAbsolutePath).mkString(", "))))
          .get // If Failure, will throw

        val config = loadConfig()
        val log = Log(
          dir = logDir,
          config = config,
          logStartOffset = 0L,
          recoveryPoint = 0L,
          maxProducerIdExpirationMs = maxPidExpirationMs,
          producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
          scheduler = scheduler,
          time = time,
          brokerTopicStats = brokerTopicStats,
          logDirFailureChannel = logDirFailureChannel)

        if (isFuture)
          futureLogs.put(topicPartition, log)
        else
          currentLogs.put(topicPartition, log)

        info(s"Created log for partition $topicPartition in $logDir with properties " + s"{${config.originals.asScala.mkString(", ")}}.")
        // Remove the preferred log dir since it has already been satisfied
        preferredLogDirs.remove(topicPartition)

        log
      }
    }
  }

  private[log] def createLogDirectory(logDir: File, logDirName: String): Try[File] = {
    val logDirPath = logDir.getAbsolutePath
    if (isLogDirOnline(logDirPath)) {
      val dir = new File(logDirPath, logDirName)
      try {
        Files.createDirectories(dir.toPath)
        Success(dir)
      } catch {
        case e: IOException =>
          val msg = s"Error while creating log for $logDirName in dir $logDirPath"
          logDirFailureChannel.maybeAddOfflineLogDir(logDirPath, msg, e)
          warn(msg, e)
          Failure(new KafkaStorageException(msg, e))
      }
    } else {
      Failure(new KafkaStorageException(s"Can not create log $logDirName because log directory $logDirPath is offline"))
    }
  }

  /**
   *  Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
   *  has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
   *  considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
   *  after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
   *  `deleteLogs` will be executed after `currentDefaultConfig.fileDeleteDelayMs`.
   */
  private def deleteLogs(): Unit = {
    var nextDelayMs = 0L
    try {
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          scheduleTimeMs + currentDefaultConfig.fileDeleteDelayMs - time.milliseconds()
        } else
          currentDefaultConfig.fileDeleteDelayMs
      }

      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              error(s"Exception while deleting $removedLog in dir ${removedLog.parentDir}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Exception in kafka-delete-logs thread.", e)
    } finally {
      try {
        scheduler.schedule("kafka-delete-logs",
          deleteLogs _,
          delay = nextDelayMs,
          unit = TimeUnit.MILLISECONDS)
      } catch {
        case e: Throwable =>
          if (scheduler.isStarted) {
            // No errors should occur unless scheduler has been shutdown
            error(s"Failed to schedule next delete in kafka-delete-logs thread", e)
          }
      }
    }
  }

  /**
   * Mark the partition directory in the source log directory for deletion and
   * rename the future log of this partition in the destination log directory to be the current log
   *
   * @param topicPartition TopicPartition that needs to be swapped
   */
  def replaceCurrentWithFutureLog(topicPartition: TopicPartition): Unit = {
    logCreationOrDeletionLock synchronized {
      val sourceLog = currentLogs.get(topicPartition)
      val destLog = futureLogs.get(topicPartition)

      info(s"Attempting to replace current log $sourceLog with $destLog for $topicPartition")
      if (sourceLog == null)
        throw new KafkaStorageException(s"The current replica for $topicPartition is offline")
      if (destLog == null)
        throw new KafkaStorageException(s"The future replica for $topicPartition is offline")

      destLog.renameDir(Log.logDirName(topicPartition))
      destLog.updateHighWatermark(sourceLog.highWatermark)

      // Now that future replica has been successfully renamed to be the current replica
      // Update the cached map and log cleaner as appropriate.
      futureLogs.remove(topicPartition)
      currentLogs.put(topicPartition, destLog)
      if (cleaner != null) {
        cleaner.alterCheckpointDir(topicPartition, sourceLog.parentDirFile, destLog.parentDirFile)
        cleaner.resumeCleaning(Seq(topicPartition))
        info(s"Compaction for partition $topicPartition is resumed")
      }

      try {
        sourceLog.renameDir(Log.logDeleteDirName(topicPartition))
        // Now that replica in source log directory has been successfully renamed for deletion.
        // Close the log, update checkpoint files, and enqueue this log to be deleted.
        sourceLog.close()
        checkpointRecoveryOffsetsAndCleanSnapshot(sourceLog.parentDirFile, ArrayBuffer.empty)
        checkpointLogStartOffsetsInDir(sourceLog.parentDirFile)
        addLogToBeDeleted(sourceLog)
      } catch {
        case e: KafkaStorageException =>
          // If sourceLog's log directory is offline, we need close its handlers here.
          // handleLogDirFailure() will not close handlers of sourceLog because it has been removed from currentLogs map
          sourceLog.closeHandlers()
          sourceLog.removeLogMetrics()
          throw e
      }

      info(s"The current replica is successfully replaced with the future replica for $topicPartition")
    }
  }

  /**
   * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
   * add it in the queue for deletion.
   *
   * @param topicPartition TopicPartition that needs to be deleted
   * @param isFuture True iff the future log of the specified partition should be deleted
   * @return the removed log
   */
  def asyncDelete(topicPartition: TopicPartition, isFuture: Boolean = false): Log = {
    val removedLog: Log = logCreationOrDeletionLock synchronized {
      if (isFuture)
        futureLogs.remove(topicPartition)
      else
        currentLogs.remove(topicPartition)
    }
    if (removedLog != null) {
      //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
      if (cleaner != null && !isFuture) {
        cleaner.abortCleaning(topicPartition)
        cleaner.updateCheckpoints(removedLog.parentDirFile)
      }
      removedLog.renameDir(Log.logDeleteDirName(topicPartition))
      checkpointRecoveryOffsetsAndCleanSnapshot(removedLog.parentDirFile, ArrayBuffer.empty)
      checkpointLogStartOffsetsInDir(removedLog.parentDirFile)
      addLogToBeDeleted(removedLog)
      info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")
    } else if (offlineLogDirs.nonEmpty) {
      throw new KafkaStorageException(s"Failed to delete log for ${if (isFuture) "future" else ""} $topicPartition because it may be in one of the offline directories ${offlineLogDirs.mkString(",")}")
    }
    removedLog
  }

  /**
   * Provides the full ordered list of suggested directories for the next partition.
   * Currently this is done by calculating the number of partitions in each directory and then sorting the
   * data directories by fewest partitions.
   */
  private def nextLogDirs(): List[File] = {
    if(_liveLogDirs.size == 1) {
      List(_liveLogDirs.peek())
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.parentDir).map { case (parent, logs) => parent -> logs.size }
      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
      val dirCounts = (zeros ++ logCounts).toBuffer

      // choose the directory with the least logs in it
      dirCounts.sortBy(_._2).map {
        case (path: String, _: Int) => new File(path)
      }.toList
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   */
  def cleanupLogs(): Unit = {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds

    // clean current logs.
    val deletableLogs = {
      if (cleaner != null) {
        // prevent cleaner from working on same partitions when changing cleanup policy
        cleaner.pauseCleaningForNonCompactedPartitions()
      } else {
        currentLogs.filter {
          case (_, log) => !log.config.compact
        }
      }
    }

    try {
      deletableLogs.foreach {
        case (topicPartition, log) =>
          debug(s"Garbage collecting '${log.name}'")
          total += log.deleteOldSegments()

          val futureLog = futureLogs.get(topicPartition)
          if (futureLog != null) {
            // clean future logs
            debug(s"Garbage collecting future log '${futureLog.name}'")
            total += futureLog.deleteOldSegments()
          }
      }
    } finally {
      if (cleaner != null) {
        cleaner.resumeCleaning(deletableLogs.map(_._1))
      }
    }

    debug(s"Log cleanup completed. $total files deleted in " +
      (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs: Iterable[Log] = currentLogs.values ++ futureLogs.values

  def logsByTopic(topic: String): Seq[Log] = {
    (currentLogs.toList ++ futureLogs.toList).collect {
      case (topicPartition, log) if topicPartition.topic == topic => log
    }
  }

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  def logsByDir: Map[String, Map[TopicPartition, Log]] = {
    // This code is called often by checkpoint processes and is written in a way that reduces
    // allocations and CPU with many topic partitions.
    // When changing this code please measure the changes with org.apache.kafka.jmh.server.CheckpointBench
    val byDir = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Log]]()
    def addToDir(tp: TopicPartition, log: Log): Unit = {
      byDir.getOrElseUpdate(log.parentDir, new mutable.AnyRefMap[TopicPartition, Log]()).put(tp, log)
    }
    currentLogs.foreachEntry(addToDir)
    futureLogs.foreachEntry(addToDir)
    byDir
  }

  // logDir should be an absolute path
  def isLogDirOnline(logDir: String): Boolean = {
    // The logDir should be an absolute path
    if (!logDirs.exists(_.getAbsolutePath == logDir))
      throw new LogDirNotFoundException(s"Log dir $logDir is not found in the config.")

    _liveLogDirs.contains(new File(logDir))
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  private def flushDirtyLogs(): Unit = {
    debug("Checking for dirty logs to flush...")

    for ((topicPartition, log) <- currentLogs.toList ++ futureLogs.toList) {
      try {
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug(s"Checking if flush is needed on ${topicPartition.topic} flush interval ${log.config.flushMs}" +
          s" last flushed ${log.lastFlushTime} time since last flush: $timeSinceLastFlush")
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush
      } catch {
        case e: Throwable =>
          error(s"Error flushing topic ${topicPartition.topic}", e)
      }
    }
  }
}

object LogManager {

  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"
  val ProducerIdExpirationCheckIntervalMs = 10 * 60 * 1000


  // 反正我理解的, kafkaServer的初始化方法里面的创建logManager对象的入口就是这里了.
  def apply(
             config: KafkaConfig,   // 全部的配置
             initialOfflineDirs: Seq[String], // 下线的logDir集合, 一般没有, 这个是刚开始读logDir里面的meta.properties时候整理的下线集合.
             zkClient: KafkaZkClient, // 这是一个zkClient, 没看里面的具体内容, 就大概看成是一个zkClient就好了, 以后看zk的时候有时间再看
             brokerState: BrokerState, // 这个是当前broker的state, 很简单的一个状态, 应该是starting状态才会进来..
             kafkaScheduler: KafkaScheduler, // 这个就是一个JUC的定时任务线程池.
             time: Time,
             brokerTopicStats: BrokerTopicStats, // 这个也是一个状态, 但不知道有什么用.
             logDirFailureChannel: LogDirFailureChannel // 这个是一个下线的logDir的集合.
           ): LogManager = {

    // 这个是拿到了log的默认配置
    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)

    LogConfig.validateValues(defaultProps)
    val defaultLogConfig = LogConfig(defaultProps)

    // 这又从zk上拿了一遍配置.
    // read the log configurations from zookeeper
    val (topicConfigs, failed) = zkClient.getLogConfigs(
      zkClient.getAllTopicsInCluster(),
      defaultProps
    )
    if (!failed.isEmpty) throw failed.head._2

    // 从配置上提取出来了compact模式清道夫的配置
    val cleanerConfig = LogCleaner.cleanerConfig(config)

    // 创建返回一个logManager
    new LogManager(
      // 1. 这个是所有的logDir, 一般就一个.
      logDirs = config.logDirs.map(new File(_).getAbsoluteFile),
      // 2. 下显得logDir
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
      topicConfigs = topicConfigs,
      initialDefaultConfig = defaultLogConfig,
      cleanerConfig = cleanerConfig,
      recoveryThreadsPerDataDir = config.numRecoveryThreadsPerDataDir,
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      // 我理解是保留数据的检查时间间隔
      retentionCheckMs = config.logCleanupIntervalMs,
      maxPidExpirationMs = config.transactionalIdExpirationMs,
      scheduler = kafkaScheduler,
      brokerState = brokerState,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      time = time)
  }
}
