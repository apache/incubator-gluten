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
package org.apache.gluten.integration

import org.apache.gluten.integration.metrics.MetricMapper
import org.apache.gluten.integration.metrics.MetricMapper.SelfTimeMapper

import org.apache.spark.SparkConf
import org.apache.spark.sql.TypeUtils
import org.apache.spark.sql.types._

import java.sql.Date

object Constants {

  val VANILLA_CONF: SparkConf = new SparkConf(false)

  val VELOX_CONF: SparkConf = new SparkConf(false)
    .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
    .set("spark.sql.parquet.enableVectorizedReader", "true")
    .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
    .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "0")
    .set(
      "spark.gluten.sql.columnar.physicalJoinOptimizeEnable",
      "false"
    ) // q72 slow if false, q64 fails if true

  val VELOX_WITH_CELEBORN_CONF: SparkConf = new SparkConf(false)
    .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
    .set("spark.gluten.sql.columnar.shuffle.celeborn.fallback.enabled", "false")
    .set("spark.sql.parquet.enableVectorizedReader", "true")
    .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.gluten.celeborn.CelebornShuffleManager")
    .set("spark.celeborn.shuffle.writer", "hash")
    .set("spark.celeborn.push.replicate.enabled", "false")
    .set("spark.celeborn.client.shuffle.compression.codec", "none")
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    .set("spark.dynamicAllocation.enabled", "false")
    .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
    .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "0")
    .set(
      "spark.gluten.sql.columnar.physicalJoinOptimizeEnable",
      "false"
    ) // q72 slow if false, q64 fails if true
    .set("spark.celeborn.push.data.timeout", "600s")
    .set("spark.celeborn.push.limit.inFlight.timeout", "1200s")

  val VELOX_WITH_UNIFFLE_CONF: SparkConf = new SparkConf(false)
    .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "true")
    .set("spark.sql.parquet.enableVectorizedReader", "true")
    .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.gluten.uniffle.UniffleShuffleManager")
    .set("spark.rss.coordinator.quorum", "localhost:19999")
    .set("spark.rss.storage.type", "MEMORY_LOCALFILE")
    .set("spark.rss.client.type", "GRPC_NETTY")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.shuffle.service.enabled", "false")
    .set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    .set("spark.dynamicAllocation.enabled", "false")
    .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
    .set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "0")
    .set("spark.gluten.sql.columnar.physicalJoinOptimizeEnable", "false")

  val VANILLA_METRIC_MAPPER: MetricMapper = SelfTimeMapper(
    Map(
      "FileSourceScanExec" -> Set("metadataTime", "scanTime"),
      "HashAggregateExec" -> Set("aggTime"),
      "ProjectExec" -> Set(), // No available metrics provided by vanilla Spark.
      "FilterExec" -> Set(), // No available metrics provided by vanilla Spark.
      "WindowExec" -> Set(), // No available metrics provided by vanilla Spark.
      "BroadcastExchangeExec" -> Set("broadcastTime", "buildTime", "collectTime"),
      "BroadcastHashJoinExec" -> Set(), // No available metrics provided by vanilla Spark.
      "ColumnarToRowExec" -> Set(), // No available metrics provided by vanilla Spark.
      "ShuffleExchangeExec" -> Set("fetchWaitTime", "shuffleWriteTime"),
      "ShuffledHashJoinExec" -> Set("buildTime"),
      "WindowGroupLimitExec" -> Set() // No available metrics provided by vanilla Spark.
    ))

  val VELOX_METRIC_MAPPER: MetricMapper = VANILLA_METRIC_MAPPER.and(
    SelfTimeMapper(
      Map(
        "FileSourceScanExecTransformer" -> Set("scanTime", "pruningTime", "remainingFilterTime"),
        "ProjectExecTransformer" -> Set("wallNanos"),
        "FilterExecTransformer" -> Set("wallNanos"),
        "SortExecTransformer" -> Set("wallNanos"),
        "RegularHashAggregateExecTransformer" -> Set("aggWallNanos", "rowConstructionWallNanos"),
        "FlushableHashAggregateExecTransformer" -> Set("aggWallNanos", "rowConstructionWallNanos"),
        "VeloxColumnarToRowExec" -> Set("convertTime"),
        "RowToVeloxColumnarExec" -> Set("convertTime"),
        "VeloxResizeBatchesExec" -> Set("selfTime"),
        "ColumnarShuffleExchangeExec" -> Set(
          "splitTime",
          "sortTime",
          "shuffleWallTime",
          "fetchWaitTime",
          "decompressTime",
          "deserializeTime"),
        "ShuffledHashJoinExecTransformer" -> Set("hashBuildWallNanos", "hashProbeWallNanos"),
        "ColumnarBroadcastExchangeExec" -> Set("broadcastTime", "collectTime"),
        "BroadcastHashJoinExecTransformer" -> Set("hashBuildWallNanos", "hashProbeWallNanos"),
        "WindowExecTransformer" -> Set("wallNanos"),
        "WindowGroupLimitExecTransformer" -> Set("wallNanos"),
        "VeloxBroadcastNestedLoopJoinExecTransformer" -> Set("wallNanos"),
        "ExpandExecTransformer" -> Set("wallNanos")
      )
    ))

  @deprecated
  val TYPE_MODIFIER_DATE_AS_DOUBLE: TypeModifier =
    new TypeModifier(TypeUtils.typeAccepts(_, DateType), DoubleType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: Date => v.getTime.asInstanceOf[Double] / 86400.0d / 1000.0d
        }
      }
    }

  @deprecated
  val TYPE_MODIFIER_INTEGER_AS_DOUBLE: TypeModifier =
    new TypeModifier(TypeUtils.typeAccepts(_, IntegerType), DoubleType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: Int => v.asInstanceOf[Double]
        }
      }
    }

  @deprecated
  val TYPE_MODIFIER_LONG_AS_DOUBLE: TypeModifier =
    new TypeModifier(TypeUtils.typeAccepts(_, LongType), DoubleType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: Long => v.asInstanceOf[Double]
        }
      }
    }

  @deprecated
  val TYPE_MODIFIER_DATE_AS_STRING: TypeModifier =
    new TypeModifier(TypeUtils.typeAccepts(_, DateType), StringType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: Date => v.toString
        }
      }
    }

  val TYPE_MODIFIER_DECIMAL_AS_DOUBLE: TypeModifier =
    new TypeModifier(TypeUtils.decimalAccepts, DoubleType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: java.math.BigDecimal => v.doubleValue()
        }
      }
    }
}
