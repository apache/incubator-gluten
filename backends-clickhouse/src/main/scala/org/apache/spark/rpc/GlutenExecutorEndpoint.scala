/*
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
package org.apache.spark.rpc

import org.apache.gluten.execution.{CHBroadcastBuildSideCache, CHNativeCacheManager}

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.rpc.GlutenRpcMessages._
import org.apache.spark.rpc.RpcCallContext
import org.apache.spark.util.{ExecutorManager, ThreadUtils, Utils}

import scala.util.{Failure, Success}

/** Gluten executor endpoint. */
class GlutenExecutorEndpoint(val executorId: String, val conf: SparkConf)
  extends IsolatedRpcEndpoint
  with Logging {
  override val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv

  private val driverHost = conf.get(config.DRIVER_HOST_ADDRESS.key, "localhost")
  private val driverPort = conf.getInt(config.DRIVER_PORT.key, 7077)
  private val rpcAddress = RpcAddress(driverHost, driverPort)
  private val driverUrl =
    RpcEndpointAddress(rpcAddress, GlutenRpcConstants.GLUTEN_DRIVER_ENDPOINT_NAME).toString

  @volatile var driverEndpointRef: RpcEndpointRef = null

  rpcEnv.setupEndpoint(GlutenRpcConstants.GLUTEN_EXECUTOR_ENDPOINT_NAME, this)
  // With Apache Spark, endpoint uses a dedicated thread pool for delivering messages and
  // ensured to be thread-safe by default.
  override def threadCount(): Int = 1
  override def onStart(): Unit = {
    rpcEnv
      .asyncSetupEndpointRefByURI(driverUrl)
      .flatMap {
        ref =>
          // This is a very fast action so we can use "ThreadUtils.sameThread"
          driverEndpointRef = ref
          ref.ask[Boolean](GlutenRegisterExecutor(executorId, self))
      }(ThreadUtils.sameThread)
      .onComplete {
        case Success(_) => logTrace("Register GlutenExecutor listener success.")
        case Failure(e) => logError("Register GlutenExecutor listener error.", e)
      }(ThreadUtils.sameThread)
    logInfo("Initialized GlutenExecutorEndpoint.")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case GlutenCleanExecutionResource(executionId, hashIds) =>
      if (executionId != null) {
        hashIds.forEach(
          resource_id => CHBroadcastBuildSideCache.invalidateBroadcastHashtable(resource_id))
      }

    case GlutenDumpNativeStackAsyncRequest(requestId) =>
      // Async collection: run in a separate thread and report result to driverEndpointRef
      val runnable = new Runnable {
        override def run(): Unit = {
          def sendResult(msg: GlutenRpcMessage): Unit = {
            var ref = driverEndpointRef
            if (ref == null) {
              try {
                ref = rpcEnv.setupEndpointRefByURI(driverUrl)
                driverEndpointRef = ref
              } catch {
                case t: Throwable =>
                  logWarning("Resolve driverEndpointRef failed when reporting async result", t)
              }
            }
            if (ref != null) {
              try {
                ref.send(msg)
              } catch {
                case t: Throwable => logWarning("Send async result to driver failed", t)
              }
            } else {
              logError(
                s"DriverEndpointRef unavailable; " +
                  s"async stack result lost for requestId=$requestId")
            }
          }
          try {
            val pid = ExecutorManager.getProcessId()
            def runCmd(cmd: String): Option[String] = {
              try {
                Some(Utils.executeAndGetOutput(Seq("bash", "-c", cmd)))
              } catch { case _: Throwable => None }
            }
            def has(cmd: String): Boolean = {
              try {
                val out = Utils.executeAndGetOutput(
                  Seq("bash", "-c", s"command -v $cmd >/dev/null 2>&1 && echo yes || echo no"))
                out != null && out.trim == "yes"
              } catch { case _: Throwable => false }
            }
            def tryInstallTools(): Unit = {
              // Best-effort install for available package manager; may require root privileges.
              val sudo = if (has("sudo")) "sudo " else ""
              val cmdOpt = {
                if (has("apt-get")) {
                  val base = s"${sudo}apt-get update -y && ${sudo}apt-get install -y "
                  Some(base + "gdb elfutils")
                } else if (has("yum")) {
                  Some(s"${sudo}yum install -y gdb elfutils")
                } else if (has("dnf")) {
                  Some(s"${sudo}dnf install -y gdb elfutils")
                } else if (has("zypper")) {
                  Some(s"${sudo}zypper -n install gdb elfutils")
                } else {
                  None
                }
              }
              cmdOpt.foreach {
                c =>
                  try { Utils.executeCommand(Seq("bash", "-c", c)) }
                  catch { case _: Throwable => () }
              }
            }
            // Ensure gdb exists; best-effort install
            if (!has("gdb")) { tryInstallTools() }
            // Always use full-thread backtrace
            val gdbCmdPrefix =
              if (has("stdbuf")) "stdbuf -o0 -e0 gdb --batch-silent -q" else "gdb --batch-silent -q"
            // Use gdb logging file to capture full output reliably, then segment and send
            val tmpLog = java.nio.file.Files.createTempFile(s"gluten-bt-$requestId", ".log")
            val logPath = tmpLog.toAbsolutePath.toString
            val charset = java.nio.charset.StandardCharsets.UTF_8
            val gdbWithLog = s"$gdbCmdPrefix -ex 'set pagination off' " +
              s"-ex 'set print thread-events off' -ex 'set logging file $logPath' " +
              s"-ex 'set logging overwrite on' -ex 'set logging on' " +
              s"-ex 'thread apply all bt full' " +
              s"-ex 'set logging off' -p $pid"
            logInfo(
              s"Starting async native stack collection: requestId=$requestId, pid=$pid, " +
                s"cmd=$gdbWithLog")
            val proc = new ProcessBuilder("bash", "-c", gdbWithLog).start()
            // Incrementally tail the log file and stream to driver while gdb runs
            val maxSegmentBytes = 64 * 1024
            var pos: Long = 0L
            var segments = 0
            var totalBytes = 0L
            def streamBytes(bytes: Array[Byte]): Unit = {
              var offset = 0
              while (offset < bytes.length) {
                val end = Math.min(bytes.length, offset + maxSegmentBytes)
                val segStr = new String(Arrays.copyOfRange(bytes, offset, end), charset)
                sendResult(GlutenRpcMessages.GlutenNativeStackAsyncChunk(requestId, segStr))
                segments += 1
                offset = end
              }
            }
            // Tail loop: read any appended bytes every ~100ms
            while (proc.isAlive) {
              try {
                val raf = new RandomAccessFile(logPath, "r")
                val len = raf.length()
                if (len > pos) {
                  val toRead = (len - pos).toInt
                  val buf = new Array[Byte](toRead)
                  raf.seek(pos)
                  raf.readFully(buf)
                  pos = len
                  totalBytes += toRead
                  streamBytes(buf)
                }
                raf.close()
              } catch { case _: Throwable => () }
              try Thread.sleep(100)
              catch { case _: Throwable => () }
            }
            // After gdb exits, flush any remaining content
            try {
              val raf = new RandomAccessFile(logPath, "r")
              val len = raf.length()
              if (len > pos) {
                val toRead = (len - pos).toInt
                val buf = new Array[Byte](toRead)
                raf.seek(pos)
                raf.readFully(buf)
                pos = len
                totalBytes += toRead
                streamBytes(buf)
              }
              raf.close()
            } catch { case _: Throwable => () }
            val exitCode = proc.waitFor()
            logInfo(
              s"gdb process exit code: $exitCode for requestId=$requestId; " +
                s"streamedBytes=$totalBytes, segments=$segments")
            // Cleanup temp file
            try java.nio.file.Files.deleteIfExists(tmpLog)
            catch { case _: Throwable => () }

            if (exitCode != 0) {
              sendResult(
                GlutenRpcMessages.GlutenNativeStackAsyncResult(
                  requestId,
                  success = false,
                  message =
                    s"Check executor logs for native C++ stack. gdb exit code: " + exitCode))
            } else {
              sendResult(
                GlutenRpcMessages.GlutenNativeStackAsyncResult(
                  requestId,
                  success = true,
                  message = ""
                ))
            }
          } catch {
            case t: Throwable =>
              sendResult(
                GlutenRpcMessages.GlutenNativeStackAsyncResult(
                  requestId,
                  success = false,
                  message = s"Async stack collection failed: ${t.getMessage}"))
          }
        }
      }
      // Use executor RPC env to run asynchronously
      new Thread(runnable, s"gluten-stack-async-$requestId").start()

    case e =>
      logError(s"Received unexpected message. $e")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GlutenMergeTreeCacheLoad(mergeTreeTable, columns, onlyMetaCache) =>
      try {
        val jobId = CHNativeCacheManager.cacheParts(mergeTreeTable, columns, onlyMetaCache)
        context.reply(CacheJobInfo(status = true, jobId))
      } catch {
        case e: Exception =>
          context.reply(
            CacheJobInfo(
              status = false,
              "",
              s"executor: $executorId cache data failed: ${e.getMessage}."))
      }
    case GlutenCacheLoadStatus(jobId) =>
      val status = CHNativeCacheManager.getCacheStatus(jobId)
      context.reply(status)
    case GlutenFilesCacheLoad(files) =>
      try {
        val jobId = CHNativeCacheManager.nativeCacheFiles(files)
        context.reply(CacheJobInfo(status = true, jobId))
      } catch {
        case e: Exception =>
          context.reply(
            CacheJobInfo(
              status = false,
              s"executor: $executorId cache data failed. ${e.getMessage}"))
      }
    case e =>
      logError(s"Received unexpected message. $e")
  }
}
object GlutenExecutorEndpoint {
  var executorEndpoint: GlutenExecutorEndpoint = _
}
