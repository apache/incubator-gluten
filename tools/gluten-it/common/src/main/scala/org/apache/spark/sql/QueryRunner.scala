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
package org.apache.spark.sql

import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorMetricsUpdate, SparkListenerTaskStart}

import com.google.common.base.Preconditions
import org.apache.commons.lang3.RandomUtils

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

object QueryRunner {
  private val availableExecutorMetrics: Set[String] = Set(
    "JVMHeapMemory",
    "JVMOffHeapMemory",
    "OnHeapExecutionMemory",
    "OffHeapExecutionMemory",
    "OnHeapStorageMemory",
    "OffHeapStorageMemory",
    "OnHeapUnifiedMemory",
    "OffHeapUnifiedMemory",
    "DirectPoolMemory",
    "MappedPoolMemory",
    "ProcessTreeJVMVMemory",
    "ProcessTreeJVMRSSMemory",
    "ProcessTreePythonVMemory",
    "ProcessTreePythonRSSMemory",
    "ProcessTreeOtherVMemory",
    "ProcessTreeOtherRSSMemory"
  )

  def runTpcQuery(
      spark: SparkSession,
      desc: String,
      queryPath: String,
      explain: Boolean,
      metrics: Array[String],
      randomKillTasks: Boolean): RunResult = {
    val unrecognizableMetrics = metrics.filter(!availableExecutorMetrics.contains(_))
    if (unrecognizableMetrics.nonEmpty) {
      throw new IllegalArgumentException(
        "Unrecognizable metric names: " + unrecognizableMetrics.mkString("Array(", ", ", ")"))
    }
    val sc = spark.sparkContext
    sc.setJobDescription(desc)
    val em = new ExecutorMetrics()
    val metricsListener = new SparkListener {
      override def onExecutorMetricsUpdate(
          executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
        executorMetricsUpdate.executorUpdates.foreach {
          case (_, peakUpdates) =>
            em.compareAndUpdatePeakValues(peakUpdates)
        }
        super.onExecutorMetricsUpdate(executorMetricsUpdate)
      }
    }
    sc.addSparkListener(metricsListener)
    if (randomKillTasks) {
      sc.addSparkListener(new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          val killer = new Thread {
            override def run(): Unit = {
              // TODO make this configurable
              // After 1s - 10s, kill the task
              val waitMs = RandomUtils.nextLong(1000L, 10000L)
              Thread.sleep(waitMs)
              // We have 20% chance to kill the task. Otherwise let the task run
              if (RandomUtils.nextFloat(0.0f, 1.0f) < 0.2f) {
                if (sc.isStopped) {
                  return
                }
                println(s"Killing task attempt after $waitMs ms: ${taskStart.taskInfo.taskId}")
                sc.killTaskAttempt(taskStart.taskInfo.taskId, interruptThread = true)
              }
            }
          }
          killer.setDaemon(true)
          killer.start()
        }
      })
    }
    println(s"Executing SQL query from resource path $queryPath...")
    try {
      val sql = resourceToString(queryPath)
      val prev = System.nanoTime()
      val df = spark.sql(sql)
      if (explain) {
        df.explain(extended = true)
      }
      val rows = df.collect()
      val millis = (System.nanoTime() - prev) / 1000000L
      val collectedMetrics = metrics.map(name => (name, em.getMetricValue(name))).toMap
      RunResult(rows, millis, collectedMetrics)
    } finally {
      sc.removeSparkListener(metricsListener)
      sc.setJobDescription(null)
    }
  }

  private def resourceToString(resource: String): String = {
    val inStream = QueryRunner.getClass.getResourceAsStream(resource)
    Preconditions.checkNotNull(inStream)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while (reading) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    } finally {
      inStream.close()
    }
    new String(outStream.toByteArray, StandardCharsets.UTF_8)
  }

}

case class RunResult(rows: Seq[Row], executionTimeMillis: Long, metrics: Map[String, Long])
