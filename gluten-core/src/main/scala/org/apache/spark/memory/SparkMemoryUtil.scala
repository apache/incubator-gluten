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

import io.glutenproject.memory.memtarget.MemoryTarget

import org.apache.spark.SparkEnv
import org.apache.spark.util.Utils

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
  consumersField.setAccessible(true)

  def getCurrentAvailableOffHeapMemory: Long = {
    val mm = SparkEnv.get.memoryManager
    val smp = smpField.get(mm).asInstanceOf[StorageMemoryPool]
    val emp = empField.get(mm).asInstanceOf[ExecutionMemoryPool]
    smp.memoryFree + emp.memoryFree
  }

  def dumpMemoryConsumerStats(tmm: TaskMemoryManager): String = {
    def getName(consumer: MemoryConsumer): String = {
      consumer match {
        case mt: MemoryTarget =>
          mt.name + "@" + Integer.toHexString(System.identityHashCode(mt));
        case mc =>
          mc.toString
      }
    }

    val stats = tmm.synchronized {
      val consumers = consumersField.get(tmm).asInstanceOf[util.HashSet[MemoryConsumer]]
      val sortedConsumers = consumers.asScala.toArray.sortBy(consumer => consumer.used)(
        Ordering.Long.reverse
      ) // ranked by used bytes

      sortedConsumers.map {
        consumer =>
          val name = getName(consumer)
          val used = Some(consumer.getUsed)
          val peak = consumer match {
            case mt: MemoryTarget =>
              Some(mt.stats().peak)
            case mc =>
              None
          }
          MemoryConsumerStats(name, used, peak)
      }
    }

    prettyPrintToString(stats)
  }

  private def prettyPrintToString(stats: Seq[MemoryConsumerStats]): String = {
    def getBytes(bytes: Option[Long]): String = {
      bytes.map(Utils.bytesToString).getOrElse("N/A")
    }
    val sb = new StringBuilder()
    sb.append(s"Memory consumer stats:")
    sb.append(System.lineSeparator())
    val nameWidth = stats.map(_.name.length).max
    val usedWidth = stats.map(each => getBytes(each.used).length).max
    val peakWidth = stats.map(each => getBytes(each.peak).length).max
    for (i <- stats.indices) {
      val each = stats(i)
      sb.append("    from ") // indent with 4 whitespaces (align with exception stack trace)
      sb.append(
        s"%${nameWidth}s: Current used bytes: %${usedWidth}s, peak bytes: %${peakWidth}s"
          .format(each.name, getBytes(each.used), getBytes(each.peak)))
      if (i != stats.size - 1) {
        sb.append(System.lineSeparator)
      }
    }
    sb.toString()
  }

  private case class MemoryConsumerStats(name: String, used: Option[Long], peak: Option[Long])
}
