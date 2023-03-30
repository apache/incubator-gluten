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
package org.apache.spark.util

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

import java.io.File
import java.nio.file.{Path, Paths}
import java.util.UUID

object SparkResourcesUtil extends Logging {
  private val blockManager = SparkEnv.get.blockManager
  private val localDirs = blockManager.diskBlockManager.localDirs.mkString(",")
  private val localDirsCycleLooper = Stream.continually(localDirs).iterator

  /**
   * Get the total cores of the Spark application
   */
  def getTotalCores(sqlConf: SQLConf): Int = {
    sqlConf.getConfString("spark.master") match {
      case local if local.startsWith("local") =>
        sqlConf.getConfString("spark.default.parallelism", "1").toInt
      case yarn if yarn.startsWith("yarn") =>
        val instances = getYarnExecutorNum(sqlConf)
        val cores = sqlConf.getConfString("spark.executor.cores", "1").toInt
        Math.max(instances * cores, sqlConf.getConfString("spark.default.parallelism", "1").toInt)
      case standalone if standalone.startsWith("spark:") =>
        Math.max(sqlConf.getConfString("spark.cores.max", "1").toInt,
          sqlConf.getConfString("spark.default.parallelism", "1").toInt)
    }
  }

  /**
   * Get the executor number for yarn
   */
  def getYarnExecutorNum(sqlConf: SQLConf): Int = {
    if (sqlConf.getConfString("spark.dynamicAllocation.enabled", "false").toBoolean) {
      val maxExecutors =
        sqlConf.getConfString("spark.dynamicAllocation.maxExecutors",
          sqlConf.getConfString("spark.default.parallelism", "1")).toInt
      maxExecutors
    } else {
      sqlConf.getConfString("spark.executor.instances", "1").toInt
    }
  }

  def getGlutenLocalDirRoundRobin(): String = synchronized {
    // FIXME Could it be better not to use Spark's local dir?
    //   Does Spark's local dir have specific rule/format for
    //   its internal use?
    if (!localDirsCycleLooper.hasNext) {
      throw new IllegalStateException()
    }
    val localDir = localDirsCycleLooper.next()
    if (StringUtils.isEmpty(localDir)) {
      throw new IllegalArgumentException(s"Illegal local dir: $localDir")
    }
    val path = Paths.get(localDir)
      .resolve("gluten")
      .resolve(UUID.randomUUID().toString)
    val file = path.toFile
    FileUtils.forceMkdir(file)
    file.getAbsolutePath
  }
}
