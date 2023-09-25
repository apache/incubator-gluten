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
package org.apache.spark.memory

import io.glutenproject.memory.memtarget.spark.TaskMemoryTarget
import io.glutenproject.proto.MemoryUsageStats

import org.apache.spark.SparkEnv
import org.apache.spark.util.Utils

import com.google.common.base.Preconditions
import org.apache.commons.lang3.StringUtils

import java.util

import scala.collection.JavaConverters._

object SparkMemoryUtil {
  private val mmClazz = classOf[MemoryManager]
  private val smpField = mmClazz.getDeclaredField("offHeapStorageMemoryPool")
  private val empField = mmClazz.getDeclaredField("offHeapExecutionMemoryPool")
  smpField.setAccessible(true)
  empField.setAccessible(true)

  private val tmmClazz = classOf[TaskMemoryManager]
  private val consumersField = tmmClazz.getDeclaredField("consumers")
  private val taskIdField = tmmClazz.getDeclaredField("taskAttemptId")
  consumersField.setAccessible(true)
  taskIdField.setAccessible(true)

  // We assume storage memory can be fully transferred to execution memory so far
  def getCurrentAvailableOffHeapMemory: Long = {
    val mm = SparkEnv.get.memoryManager
    val smp = smpField.get(mm).asInstanceOf[StorageMemoryPool]
    val emp = empField.get(mm).asInstanceOf[ExecutionMemoryPool]
    smp.memoryFree + emp.memoryFree
  }

  def dumpMemoryConsumerStats(tmm: TaskMemoryManager): String = {
    def sortStats(stats: Seq[MemoryConsumerStats]) = {
      stats.sortBy(_.used.getOrElse(Long.MinValue))(Ordering.Long.reverse)
    }

    val stats = tmm.synchronized {
      val consumers = consumersField.get(tmm).asInstanceOf[util.HashSet[MemoryConsumer]]

      // create stats map
      val statsMap = new util.HashMap[String, MemoryUsageStats]()
      consumers.asScala.foreach {
        case mt: TaskMemoryTarget =>
          statsMap.put(mt.name(), mt.stats())
        case mc =>
          statsMap.put(
            mc.toString,
            MemoryUsageStats
              .newBuilder()
              .setCurrent(mc.getUsed)
              .setPeak(-1L)
              .build())
      }
      Preconditions.checkState(statsMap.size() == consumers.size())

      // add root
      MemoryUsageStats
        .newBuilder()
        .setCurrent(tmm.getMemoryConsumptionForThisTask)
        .setPeak(-1L)
        .putAllChildren(statsMap)
        .build()
    }

    def toMemoryConsumerStats(name: String, mus: MemoryUsageStats): MemoryConsumerStats = {
      MemoryConsumerStats(
        name,
        Some(mus.getCurrent),
        mus.getPeak match {
          case -1L => None
          case v => Some(v)
        },
        sortStats(
          mus.getChildrenMap
            .entrySet()
            .asScala
            .toList
            .map(entry => toMemoryConsumerStats(entry.getKey, entry.getValue)))
      )
    }

    prettyPrintToString(toMemoryConsumerStats(s"Task.${taskIdField.get(tmm)}", stats))
  }

  private def prettyPrintToString(stats: MemoryConsumerStats): String = {

    def getBytes(bytes: Option[Long]): String = {
      bytes.map(Utils.bytesToString).getOrElse("N/A")
    }

    def getFullName(name: String, prefix: String): String = {
      "%s%s:".format(prefix, name)
    }

    val sb = new StringBuilder()
    sb.append(s"Memory consumer stats:")

    // determine padding widths
    var nameWidth = 0
    var usedWidth = 0
    var peakWidth = 0
    def addPaddingSingleLevel(stats: MemoryConsumerStats, extraWidth: Integer): Unit = {
      nameWidth = Math.max(nameWidth, getFullName(stats.name, "").length + extraWidth)
      usedWidth = Math.max(usedWidth, getBytes(stats.used).length)
      peakWidth = Math.max(peakWidth, getBytes(stats.peak).length)
      stats.children.foreach(addPaddingSingleLevel(_, extraWidth + 3)) // e.g. "\- "
    }
    addPaddingSingleLevel(stats, 1) // take the leading '\t' into account

    // print
    def printSingleLevel(
        stats: MemoryConsumerStats,
        treePrefix: String,
        treeChildrenPrefix: String): Unit = {
      sb.append(System.lineSeparator())

      val name = getFullName(stats.name, treePrefix)
      sb.append(
        s"%s Current used bytes: %s, peak bytes: %s"
          .format(
            StringUtils.rightPad(name, nameWidth, ' '),
            StringUtils.leftPad(String.valueOf(getBytes(stats.used)), usedWidth, ' '),
            StringUtils.leftPad(String.valueOf(getBytes(stats.peak)), peakWidth, ' ')
          ))

      stats.children.zipWithIndex.foreach {
        case (child, i) =>
          if (i != stats.children.size - 1) {
            printSingleLevel(child, treeChildrenPrefix + "+- ", treeChildrenPrefix + "|  ")
          } else {
            printSingleLevel(child, treeChildrenPrefix + "\\- ", treeChildrenPrefix + "   ")
          }
      }
    }

    printSingleLevel(
      stats,
      "\t",
      "\t"
    ) // top level is indented with one tab (align with exception stack trace)

    // return
    sb.toString()
  }

  private case class MemoryConsumerStats(
      name: String,
      used: Option[Long],
      peak: Option[Long],
      children: Iterable[MemoryConsumerStats])
}
