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
package org.apache.gluten.qt

import org.apache.spark.status.api.v1
import org.apache.spark.util.kvstore.InMemoryStore

import ReflectionUtils.{accessPrivateClass, accessPrivateMethod}

import java.util.regex.Pattern

import scala.collection.JavaConverters._

class AppStatusStore(kvStore: InMemoryStore) {
  private val appStatusStore =
    accessPrivateClass("org.apache.spark.status.AppStatusStore", kvStore, None, None)
  private lazy val applicationInfo = accessPrivateMethod(appStatusStore, "applicationInfo")
  private val HOST_URL_PATTERN = Pattern.compile("gdpic-srvls-batch-([a-z0-9\\-]+)-m.*")

  def getApplicationId: String = accessPrivateMethod(applicationInfo, "id").asInstanceOf[String]
  def getApplicationName: String = accessPrivateMethod(applicationInfo, "name").asInstanceOf[String]

  def getJobIdToTaskDurationMap: Seq[(Int, Long)] = {
    val jobSeq = accessPrivateMethod(appStatusStore, "jobsList", null).asInstanceOf[Seq[_]]
    val jobIdToTaskDurationMap = jobSeq.map {
      js =>
        val jobId = accessPrivateMethod(js, "jobId").asInstanceOf[Int]
        val taskDuration = accessPrivateMethod(js, "stageIds")
          .asInstanceOf[collection.Seq[Int]]
          .flatMap(
            accessPrivateMethod(
              appStatusStore,
              "stageData",
              _,
              false,
              List[v1.TaskStatus]().asJava,
              false,
              Array.empty[Double]).asInstanceOf[Seq[_]])
          .map(accessPrivateMethod(_, "executorRunTime").asInstanceOf[Long])
          .sum
        jobId -> taskDuration
    }
    jobIdToTaskDurationMap
  }

  def getBatchUuid: String = {
    val envInfo = accessPrivateMethod(appStatusStore, "environmentInfo", null)
      .asInstanceOf[v1.ApplicationEnvironmentInfo]
    envInfo.sparkProperties
      .find(_._1.equalsIgnoreCase("spark.driver.host"))
      .map(_._2)
      .flatMap {
        host =>
          val hostMatcher = HOST_URL_PATTERN.matcher(host)
          if (hostMatcher.find()) Some(hostMatcher.group(1)) else None
      }
      .getOrElse("UNKNOWN")
  }
}
