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
package io.glutenproject.backendsapi.clickhouse

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi._
import io.glutenproject.expression.WindowFunctionsBuilder
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat.{JsonReadFormat, MergeTreeReadFormat, OrcReadFormat, ParquetReadFormat, TextReadFormat}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, DenseRank, Lag, Lead, NamedExpression, Rank, RowNumber}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

import scala.util.control.Breaks.{break, breakable}

class CHBackend extends Backend {
  override def name(): String = GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND
  override def contextApi(): ContextApi = new CHContextApi
  override def iteratorApi(): IteratorApi = new CHIteratorApi
  override def sparkPlanExecApi(): SparkPlanExecApi = new CHSparkPlanExecApi
  override def transformerApi(): TransformerApi = new CHTransformerApi
  override def validatorApi(): ValidatorApi = new CHValidatorApi
  override def metricsApi(): MetricsApi = new CHMetricsApi
  override def settings(): BackendSettingsApi = CHBackendSettings
}

object CHBackendSettings extends BackendSettingsApi with Logging {

  private val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD = "spark.gluten.sql.columnar.separate.scan.rdd.for.ch"
  private val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT = "false"

  // experimental: when the files count per partition exceeds this threshold,
  // it will put the files into one partition.
  val GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND +
      ".files.per.partition.threshold"
  val GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT = "-1"

  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND +
      ".customized.shuffle.codec.enable"
  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE_DEFAULT = false
  lazy val useCustomizedShuffleCodec: Boolean = SparkEnv.get.conf.getBoolean(
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE,
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE_DEFAULT
  )

  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND +
      ".customized.buffer.size"
  private val GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE_DEFAULT = 4096
  lazy val customizeBufferSize: Int = SparkEnv.get.conf.getInt(
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE,
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE_DEFAULT
  )

  val GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND +
      ".broadcast.cache.expired.time"
  // unit: SECONDS, default 1 day
  val GLUTEN_CLICKHOUSE_BROADCAST_CACHE_EXPIRED_TIME_DEFAULT: Int = 86400

  private val GLUTEN_CLICKHOUSE_SHUFFLE_SUPPORTED_CODEC: Set[String] = Set("lz4", "zstd", "snappy")

  override def supportFileFormatRead(
      format: ReadFileFormat,
      fields: Array[StructField],
      partTable: Boolean,
      paths: Seq[String]): Boolean = {

    def validateFilePath: Boolean = {
      // Fallback to vanilla spark when the input path
      // does not contain the partition info.
      if (partTable && !paths.forall(_.contains("="))) {
        return false
      }
      true
    }

    // Validate if all types are supported.
    def hasComplexType: Boolean = {
      // Collect unsupported types.
      val unsupportedDataTypes = fields.map(_.dataType).collect {
        case _: MapType => "MapType"
        case _: StructType => "StructType"
        case _: ArrayType => "ArrayType"
      }
      for (unsupportedDataType <- unsupportedDataTypes) {
        // scalastyle:off println
        println(
          s"Validation failed for ${this.getClass.toString}" +
            s" due to: data type $unsupportedDataType. in file schema. ")
        // scalastyle:on println
      }
      !unsupportedDataTypes.isEmpty
    }
    format match {
      case ParquetReadFormat => validateFilePath
      case OrcReadFormat => true
      case MergeTreeReadFormat => true
      case TextReadFormat => !hasComplexType
      case JsonReadFormat => true
      case _ => false
    }
  }

  override def utilizeShuffledHashJoinHint(): Boolean = true
  override def supportShuffleWithProject(
      outputPartitioning: Partitioning,
      child: SparkPlan): Boolean = {
    child match {
      case hash: HashAggregateExec =>
        // support project when aggregation only has grouping keys, for tpcds q14a,b
        hash.aggregateExpressions.isEmpty
      case _ =>
        true
    }
  }

  override def supportSortExec(): Boolean = {
    GlutenConfig.getConf.enableColumnarSort
  }

  override def supportSortMergeJoinExec(): Boolean = {
    false
  }

  override def supportWindowExec(windowFunctions: Seq[NamedExpression]): Boolean = {
    var allSupported = true
    breakable {
      windowFunctions.foreach(
        func => {
          val aliasExpr = func.asInstanceOf[Alias]
          val wExpression = WindowFunctionsBuilder.extractWindowExpression(aliasExpr.child)
          wExpression.windowFunction match {
            case _: RowNumber | _: AggregateExpression | _: Rank | _: Lead | _: Lag |
                _: DenseRank =>
              allSupported = allSupported
            case _ =>
              logDebug(s"Not support window function: ${wExpression.getClass}")
              allSupported = false
              break
          }
        })
    }
    allSupported
  }

  override def supportStructType(): Boolean = true

  override def supportExpandExec(): Boolean = true

  override def excludeScanExecFromCollapsedStage(): Boolean =
    SQLConf.get
      .getConfString(GLUTEN_CLICKHOUSE_SEP_SCAN_RDD, GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT)
      .toBoolean

  /** Get the config prefix for each backend */
  override def getBackendConfigPrefix: String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND

  override def shuffleSupportedCodec(): Set[String] = GLUTEN_CLICKHOUSE_SHUFFLE_SUPPORTED_CODEC
  override def needOutputSchemaForPlan(): Boolean = true
}
