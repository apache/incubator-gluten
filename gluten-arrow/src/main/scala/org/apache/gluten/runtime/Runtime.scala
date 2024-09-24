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
package org.apache.gluten.runtime

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backend.Backend
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.memory.MemoryUsageStatsBuilder
import org.apache.gluten.memory.listener.ReservationListeners
import org.apache.gluten.memory.memtarget.{KnownNameAndStats, MemoryTarget, Spiller, Spillers}
import org.apache.gluten.proto.MemoryUsageStats
import org.apache.gluten.utils.ConfigUtil

import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.sql.internal.{GlutenConfigUtil, SQLConf}
import org.apache.spark.task.TaskResource

import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable

trait Runtime {
  def addSpiller(spiller: Spiller): Unit
  def holdMemory(): Unit
  def collectMemoryUsage(): MemoryUsageStats
  def getHandle(): Long
}

object Runtime {
  private[runtime] def apply(name: String): Runtime with TaskResource = {
    new RuntimeImpl(name)
  }

  private class RuntimeImpl(name: String) extends Runtime with TaskResource {
    private val LOGGER = LoggerFactory.getLogger(classOf[Runtime])

    private val spillers = Spillers.appendable()
    private val mutableStats: mutable.Map[String, MemoryUsageStatsBuilder] = mutable.Map()
    private val rl = ReservationListeners.create(resourceName(), spillers, mutableStats.asJava)
    private val handle = RuntimeJniWrapper.createRuntime(
      Backend.get().name(),
      rl,
      ConfigUtil.serialize(
        GlutenConfig.getNativeSessionConf(
          Backend.get().name(),
          GlutenConfigUtil.parseConfig(SQLConf.get.getAllConfs)))
    )

    spillers.append(new Spiller() {
      override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = {
        if (!Spillers.PHASE_SET_SHRINK_ONLY.contains(phase)) {
          // Only respond for shrinking.
          return 0L
        }
        RuntimeJniWrapper.shrinkMemory(handle, size)
      }
    })
    mutableStats += "single" -> new MemoryUsageStatsBuilder {
      override def toStats: MemoryUsageStats = collectMemoryUsage()
    }

    private val released: AtomicBoolean = new AtomicBoolean(false)

    def getHandle(): Long = handle

    def addSpiller(spiller: Spiller): Unit = {
      spillers.append(spiller)
    }

    def holdMemory(): Unit = {
      RuntimeJniWrapper.holdMemory(handle)
    }

    def collectMemoryUsage(): MemoryUsageStats = {
      MemoryUsageStats.parseFrom(RuntimeJniWrapper.collectMemoryUsage(handle))
    }

    override def release(): Unit = {
      if (!released.compareAndSet(false, true)) {
        throw new GlutenException(
          s"Runtime instance already released: $handle, ${resourceName()}, ${priority()}")
      }

      def dump(): String = {
        SparkMemoryUtil.prettyPrintStats(
          s"[${resourceName()}]",
          new KnownNameAndStats() {
            override def name: String = resourceName()
            override def stats: MemoryUsageStats = collectMemoryUsage()
          })
      }

      if (LOGGER.isDebugEnabled) {
        LOGGER.debug("About to release memory manager, " + dump())
      }

      RuntimeJniWrapper.releaseRuntime(handle)

      if (rl.getUsedBytes != 0) {
        LOGGER.warn(
          String.format(
            "%s Reservation listener %s still reserved non-zero bytes, which may cause memory" +
              " leak, size: %s.",
            name,
            rl.toString,
            SparkMemoryUtil.bytesToString(rl.getUsedBytes)
          ))
      }
    }

    override def priority(): Int = 0

    override def resourceName(): String = name
  }
}
