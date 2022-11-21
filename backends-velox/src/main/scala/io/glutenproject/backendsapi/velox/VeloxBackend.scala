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

package io.glutenproject.backendsapi.velox

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi._

import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.velox.DwrfFileFormat

class VeloxBackend extends Backend {
  override def name: String = GlutenConfig.GLUTEN_VELOX_BACKEND
  override def initializerApi(): IInitializerApi = new VeloxInitializerApi
  override def iteratorApi(): IIteratorApi = new VeloxIteratorApi
  override def sparkPlanExecApi(): ISparkPlanExecApi = new VeloxSparkPlanExecApi
  override def transformerApi(): ITransformerApi = new VeloxTransformerApi
  override def settings(): BackendSettings = VeloxBackendSettings
}

object VeloxBackendSettings extends BackendSettings {
  override def supportedFileFormats(): Set[Class[_ <: FileFormat]] =
    Set(classOf[ParquetFileFormat], classOf[OrcFileFormat], classOf[DwrfFileFormat])
  override def supportExpandExec(): Boolean = true
  override def supportSortExec(): Boolean = true
  override def supportHashBuildJoinTypeOnLeft: JoinType => Boolean = {
    t =>
      if (super.supportHashBuildJoinTypeOnLeft(t)) {
        true
      } else {
        t match {
          // For Velox backend, build right and left are both supported for
          // LeftOuter and LeftSemi.
          case LeftOuter | LeftSemi => true
          case _ => false
        }
      }
  }
  override def supportHashBuildJoinTypeOnRight: JoinType => Boolean = {
    t =>
      if (super.supportHashBuildJoinTypeOnRight(t)) {
        true
      } else {
        t match {
          // For Velox backend, build right and left are both supported for RightOuter.
          case RightOuter => true
          case _ => false
        }
      }
  }
  override def recreateJoinExecOnFallback(): Boolean = true
  override def removeHashColumnFromColumnarShuffleExchangeExec(): Boolean = true
}
