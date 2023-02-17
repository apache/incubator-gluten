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
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat.{DwrfReadFormat, ParquetReadFormat}

import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, MapType, StructField, StructType}

class VeloxBackend extends Backend {
  override def name: String = GlutenConfig.GLUTEN_VELOX_BACKEND
  override def initializerApi(): IInitializerApi = new VeloxInitializerApi
  override def iteratorApi(): IIteratorApi = new VeloxIteratorApi
  override def sparkPlanExecApi(): ISparkPlanExecApi = new VeloxSparkPlanExecApi
  override def transformerApi(): ITransformerApi = new VeloxTransformerApi
  override def validatorApi(): IValidatorApi = new VeloxValidatorApi
  override def settings(): BackendSettings = VeloxBackendSettings
}

object VeloxBackendSettings extends BackendSettings {
  override def supportFileFormatRead(format: ReadFileFormat,
                                     fields: Array[StructField]): Boolean = {
    format match {
      case ParquetReadFormat =>
        // Unsupported types are prevented.
        fields.map(_.dataType).collect {
          case _: BooleanType =>
          case _: ByteType =>
          case _: ArrayType =>
          case _: MapType =>
          case _: StructType =>
        }.isEmpty
      case DwrfReadFormat => true
      case _ => false
    }
  }

  override def supportExpandExec(): Boolean = true
  override def needProjectExpandOutput: Boolean = true
  override def supportSortExec(): Boolean = true
  override def supportWindowExec(): Boolean = true
  override def supportColumnarShuffleExec(): Boolean = {
    GlutenConfig.getSessionConf.isUseColumnarShuffleManager
  }
  override def supportHashBuildJoinTypeOnLeft: JoinType => Boolean = {
    t =>
      if (super.supportHashBuildJoinTypeOnLeft(t)) {
        true
      } else {
        t match {
          // OPPRO-266: For Velox backend, build right and left are both supported for
          // LeftOuter and LeftSemi.
          // FIXME Hongze 22/12/06
          //  HashJoin.scala in shim was not always loaded by class loader.
          //  The file should be removed and we temporarily disable the improvement
          //  introduced by OPPRO-266 by commenting out the following prerequisite
          //  condition.
//          case LeftOuter | LeftSemi => true
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
          // OPPRO-266: For Velox backend, build right and left are both supported for RightOuter.
          // FIXME Hongze 22/12/06
          //  HashJoin.scala in shim was not always loaded by class loader.
          //  The file should be removed and we temporarily disable the improvement
          //  introduced by OPPRO-266 by commenting out the following prerequisite
          //  condition.
//          case RightOuter => true
          case _ => false
        }
      }
  }

  override def disableVanillaColumnarReaders(): Boolean = true
  override def fallbackOnEmptySchema(): Boolean = true
  override def recreateJoinExecOnFallback(): Boolean = true
  override def removeHashColumnFromColumnarShuffleExchangeExec(): Boolean = true

  /**
   * Get the config prefix for each backend
   */
  override def getBackendConfigPrefix(): String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + GlutenConfig.GLUTEN_VELOX_BACKEND
}
