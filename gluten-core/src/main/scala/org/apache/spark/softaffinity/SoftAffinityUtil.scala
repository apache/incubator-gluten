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
package org.apache.spark.softaffinity

import io.glutenproject.GlutenConfig
import io.glutenproject.execution.GlutenMergeTreePartition
import io.glutenproject.softaffinity.SoftAffinityManager
import io.glutenproject.utils.LogLevelUtil

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorCacheTaskLocation

object SoftAffinityUtil extends LogLevelUtil with Logging {

  private lazy val softAffinityLogLevel = GlutenConfig.getConf.softAffinityLogLevel

  /** Get the locations by SoftAffinityManager */
  def getFilePartitionLocations(
      filePaths: Array[String],
      preferredLocations: Array[String]): Array[String] = {
    if (
      !filePaths.isEmpty && SoftAffinityManager.usingSoftAffinity
      && !SoftAffinityManager.checkTargetHosts(preferredLocations)
    ) {
      // if there is no host in the node list which are executors running on,
      // using SoftAffinityManager to generate target executors.
      // Only using the first file to calculate the target executors
      // Only get one file to calculate the target host
      val filePath = filePaths.min
      val locations = SoftAffinityManager.askExecutors(filePath)
      if (!locations.isEmpty) {
        logOnLevel(
          softAffinityLogLevel,
          s"SAMetrics=File $filePath - " +
            s"the expected executors are ${locations.mkString("_")} ")
        locations.map {
          p =>
            if (p._1.equals("")) p._2
            else ExecutorCacheTaskLocation(p._2, p._1).toString
        }
      } else {
        Array.empty[String]
      }
    } else {
      preferredLocations
    }
  }

  /** Get the locations by SoftAffinityManager */
  def getNativeMergeTreePartitionLocations(
      filePartition: GlutenMergeTreePartition): Array[String] = {
    if (SoftAffinityManager.usingSoftAffinity) {
      val file = filePartition.tablePath + "_" + filePartition.maxParts
      val locations = SoftAffinityManager.askExecutors(file)
      if (!locations.isEmpty) {
        logOnLevel(
          softAffinityLogLevel,
          s"SAMetrics=File $file - " +
            s"the expected executors are ${locations.mkString("_")} ")
        locations.map {
          p =>
            if (p._1.equals("")) p._2
            else ExecutorCacheTaskLocation(p._2, p._1).toString
        }
      } else {
        Array.empty[String]
      }
    } else {
      Array.empty[String]
    }
  }
}
