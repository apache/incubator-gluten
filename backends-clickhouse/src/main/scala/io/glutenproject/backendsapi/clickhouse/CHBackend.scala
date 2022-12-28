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

import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf

class CHBackend extends Backend {
  override def name(): String = GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND
  override def initializerApi(): IInitializerApi = new CHInitializerApi
  override def iteratorApi(): IIteratorApi = new CHIteratorApi
  override def sparkPlanExecApi(): ISparkPlanExecApi = new CHSparkPlanExecApi
  override def transformerApi(): ITransformerApi = new CHTransformerApi
  override def validatorApi(): IValidatorApi = new CHValidatorApi
  override def settings(): BackendSettings = CHBackendSettings
}

object CHBackendSettings extends BackendSettings {

  val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD = "spark.gluten.sql.columnar.separate.scan.rdd.for.ch"
  val GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT = "false"

  // experimental: when the files count per partition exceeds this threshold,
  // it will put the files into one partition.
  val GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND +
      ".files.per.partition.threshold"
  val GLUTEN_CLICKHOUSE_FILES_PER_PARTITION_THRESHOLD_DEFAULT = "-1"

  override def supportFileFormatRead(): FileFormat => Boolean = {
    case _: ParquetFileFormat => true
    case _: OrcFileFormat => true
    case _ => false
  }

  override def utilizeShuffledHashJoinHint(): Boolean = true

  override def supportSortExec(): Boolean = {
    GlutenConfig.getSessionConf.enableColumnarSort
  }

  override def supportWindowExec(): Boolean = true

  override def excludeScanExecFromCollapsedStage(): Boolean =
    SQLConf.get
      .getConfString(GLUTEN_CLICKHOUSE_SEP_SCAN_RDD, GLUTEN_CLICKHOUSE_SEP_SCAN_RDD_DEFAULT)
      .toBoolean

  /** Get the config prefix for each backend */
  override def getBackendConfigPrefix(): String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND
}
