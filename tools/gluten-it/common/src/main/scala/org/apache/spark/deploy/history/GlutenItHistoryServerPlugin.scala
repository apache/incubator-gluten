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
package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerHelper.LogServerRpcEnvs
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.ConfUtils.ConfImplicits.SparkConfWrapper
import org.apache.spark.status.{AppHistoryServerPlugin, ElementTrackingStore, ExecutorSummaryWrapper}
import org.apache.spark.status.api.v1

import com.google.common.base.Preconditions

import java.net.{ServerSocket, URI, URL}
import java.util.Locale

import scala.collection.JavaConverters._

class GlutenItHistoryServerPlugin extends AppHistoryServerPlugin {

  // create log server (a fake master + a fake worker)
  private val lsConf = new SparkConf(false)
  lsConf.setWarningOnOverriding("spark.master.rest.enabled", "false")
  lsConf.setWarningOnOverriding("spark.shuffle.service.enabled", "false")
  private val logServerRpcEnvs = HistoryServerHelper.startLogServer(lsConf)

  private def rewriteLogs(
      executorLogs: Map[String, String],
      envs: LogServerRpcEnvs): Map[String, String] = {

    def rewriteUrl(uriStr: String): String = {
      val uri = new URI(uriStr)
      new URI(
        uri.getScheme.toLowerCase(Locale.getDefault),
        s"${envs.worker.address.host}:${envs.workerWebUiPort}",
        uri.getPath,
        uri.getQuery,
        uri.getFragment).toString
    }

    executorLogs.map {
      case (name, url) =>
        val newName = s"gluten-it-$name"
        val newUrl = rewriteUrl(url)
        (newName, newUrl)
    }
  }

  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] = {
    store.onFlush {

      val wrappers = org.apache.spark.util.Utils
        .tryWithResource(store.view(classOf[ExecutorSummaryWrapper]).closeableIterator()) {
          iter => iter.asScala.toList
        }
      // create new executor summaries
      wrappers
        .map {
          wrapper =>
            Preconditions.checkArgument(wrapper.info.attributes.isEmpty)
            new ExecutorSummaryWrapper(
              new v1.ExecutorSummary(
                id = wrapper.info.id,
                hostPort = wrapper.info.hostPort,
                isActive = wrapper.info.isActive,
                rddBlocks = wrapper.info.rddBlocks,
                memoryUsed = wrapper.info.memoryUsed,
                diskUsed = wrapper.info.diskUsed,
                totalCores = wrapper.info.totalCores,
                maxTasks = wrapper.info.maxTasks,
                activeTasks = wrapper.info.activeTasks,
                failedTasks = wrapper.info.failedTasks,
                completedTasks = wrapper.info.completedTasks,
                totalTasks = wrapper.info.totalTasks,
                totalDuration = wrapper.info.totalDuration,
                totalGCTime = wrapper.info.totalGCTime,
                totalInputBytes = wrapper.info.totalInputBytes,
                totalShuffleRead = wrapper.info.totalShuffleRead,
                totalShuffleWrite = wrapper.info.totalShuffleWrite,
                isBlacklisted = wrapper.info.isBlacklisted,
                maxMemory = wrapper.info.maxMemory,
                addTime = wrapper.info.addTime,
                removeTime = wrapper.info.removeTime,
                removeReason = wrapper.info.removeReason,
                executorLogs = rewriteLogs(wrapper.info.executorLogs, logServerRpcEnvs),
                memoryMetrics = wrapper.info.memoryMetrics,
                blacklistedInStages = wrapper.info.blacklistedInStages,
                peakMemoryMetrics = wrapper.info.peakMemoryMetrics,
                attributes = wrapper.info.attributes,
                resources = wrapper.info.resources,
                resourceProfileId = wrapper.info.resourceProfileId,
                isExcluded = wrapper.info.isExcluded,
                excludedInStages = wrapper.info.excludedInStages
              ))
        }
        .foreach(store.write(_))
    }
    Seq()
  }

  override def setupUI(ui: _root_.org.apache.spark.ui.SparkUI): Unit = {
    // no-op
  }
}
