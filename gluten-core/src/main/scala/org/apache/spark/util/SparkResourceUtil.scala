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

import org.apache.spark.{SparkConf, SparkMasterRegex}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{EXECUTOR_MEMORY, EXECUTOR_MEMORY_OVERHEAD}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.internal.SQLConf

object SparkResourceUtil extends Logging {
  private val MEMORY_OVERHEAD_FACTOR = "spark.executor.memoryOverheadFactor"
  private val MIN_MEMORY_OVERHEAD = "spark.executor.minMemoryOverhead"

  /** Get the total cores of the Spark application */
  def getTotalCores(sqlConf: SQLConf): Int = {
    sqlConf.getConfString("spark.master") match {
      case local if local.startsWith("local") =>
        sqlConf.getConfString("spark.default.parallelism", "1").toInt
      case otherResourceManager if otherResourceManager.matches("(yarn|k8s:).*") =>
        val instances = getExecutorNum(sqlConf)
        val cores = sqlConf.getConfString("spark.executor.cores", "1").toInt
        Math.max(instances * cores, sqlConf.getConfString("spark.default.parallelism", "1").toInt)
      case standalone if standalone.startsWith("spark:") =>
        Math.max(
          sqlConf.getConfString("spark.cores.max", "1").toInt,
          sqlConf.getConfString("spark.default.parallelism", "1").toInt)
    }
  }

  /** Get the executor number for yarn */
  def getExecutorNum(sqlConf: SQLConf): Int = {
    if (sqlConf.getConfString("spark.dynamicAllocation.enabled", "false").toBoolean) {
      val maxExecutors =
        sqlConf
          .getConfString(
            "spark.dynamicAllocation.maxExecutors",
            sqlConf.getConfString("spark.default.parallelism", "1"))
          .toInt
      maxExecutors
    } else {
      sqlConf.getConfString("spark.executor.instances", "1").toInt
    }
  }

  def getExecutorCores(conf: SparkConf): Int = {
    val master = conf.get("spark.master")

    // part of the code originated from org.apache.spark.SparkContext#numDriverCores
    def convertToInt(threads: String): Int = {
      if (threads == "*") Runtime.getRuntime.availableProcessors() else threads.toInt
    }

    val cores = master match {
      case "local" => 1
      case SparkMasterRegex.LOCAL_N_REGEX(threads) => convertToInt(threads)
      case SparkMasterRegex.LOCAL_N_FAILURES_REGEX(threads, _) => convertToInt(threads)
      case _ => conf.getInt("spark.executor.cores", 1)
    }

    cores
  }

  def getTaskSlots(conf: SparkConf): Int = {
    val executorCores = SparkResourceUtil.getExecutorCores(conf)
    val taskCores = conf.getInt("spark.task.cpus", 1)
    executorCores / taskCores
  }

  def isLocalMaster(conf: SparkConf): Boolean = {
    Utils.isLocalMaster(conf)
  }

  // Returns whether user manually sets memory overhead.
  def isMemoryOverheadSet(conf: SparkConf): Boolean = {
    Seq(EXECUTOR_MEMORY_OVERHEAD.key, MEMORY_OVERHEAD_FACTOR, MIN_MEMORY_OVERHEAD).exists(
      conf.contains)
  }

  def getMemoryOverheadSize(conf: SparkConf): Long = {
    val overheadMib = conf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse {
      val executorMemMib = conf.get(EXECUTOR_MEMORY)
      val factor =
        conf.getDouble(MEMORY_OVERHEAD_FACTOR, 0.1d)
      val minMib = conf.getLong(MIN_MEMORY_OVERHEAD, 384L)
      (executorMemMib * factor).toLong.max(minMib)
    }
    ByteUnit.MiB.toBytes(overheadMib)
  }
}
