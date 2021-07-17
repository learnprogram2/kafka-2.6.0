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

import java.io.IOException
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

import kafka.utils.Logging

/*
 * LogDirFailureChannel allows an external thread to block waiting for new offline log dirs.
 *
 * There should be a single instance of LogDirFailureChannel accessible by any class that does disk-IO operation.
 * If IOException is encountered while accessing a log directory, the corresponding class can add the log directory name
 * to the LogDirFailureChannel using maybeAddOfflineLogDir(). Each log directory will be added only once. After a log
 * directory is added for the first time, a thread which is blocked waiting for new offline log directories
 * can take the name of the new offline log directory out of the LogDirFailureChannel and handle the log failure properly.
 * An offline log directory will stay offline until the broker is restarted.
 *
   logDirFailureChannel应该是一个全局的directory离线后的降级方案:
  有了出现IOException的目录就可以放进来.

  -- 放进来有毛用? 就是可以让外部的线程阻塞拿到它们, 然后做什么什么处理. 这就是下线dir一家人集合处.

  下面是翻译.
     LogDirFailureChannel 允许外部线程阻塞等待新的离线日志目录。

     应该有一个 LogDirFailureChannel 实例可供任何执行磁盘 IO 操作的类访问。

     如果在访问日志目录时遇到IOException，相应的类可以使用maybeAddOfflineLogDir()将日志目录名称添加到LogDirFailureChannel。

     每个日志目录只会添加一次。
     首次添加日志目录后，被阻塞等待新的离线日志目录的线程可以从LogDirFailureChannel中取出新的离线日志目录的名称，正确处理日志失败。

     离线日志目录将保持离线状态，直到代理重新启动。


 */
class LogDirFailureChannel(logDirNum: Int) extends Logging {

  private val offlineLogDirs = new ConcurrentHashMap[String, String]
  // 这个是logDir的size大小的队列, 最多就是全部目录都下线了.
  private val offlineLogDirQueue = new ArrayBlockingQueue[String](logDirNum)

  /*
   * If the given logDir is not already offline, add it to the
   * set of offline log dirs and enqueue it to the logDirFailureEvent queue
   */
  def maybeAddOfflineLogDir(logDir: String, msg: => String, e: IOException): Unit = {
    error(msg, e)
    if (offlineLogDirs.putIfAbsent(logDir, logDir) == null)
      offlineLogDirQueue.add(logDir)
  }

  /*
   * Get the next offline log dir from logDirFailureEvent queue.
   * The method will wait if necessary until a new offline log directory becomes available
   */
  def takeNextOfflineLogDir(): String = offlineLogDirQueue.take()

}
