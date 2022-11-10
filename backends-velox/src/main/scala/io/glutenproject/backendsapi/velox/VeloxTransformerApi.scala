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
import io.glutenproject.backendsapi.ITransformerApi
import io.glutenproject.expression.{ArrowConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.SelectionNode
import io.glutenproject.utils.InputPartitionsUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.velox.DwrfFileFormat

class VeloxTransformerApi extends ITransformerApi with Logging {

  /**
   * Do validate for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def validateColumnarShuffleExchangeExec(
                                                    outputPartitioning: Partitioning,
                                                    outputAttributes: Seq[Attribute]): Boolean = {
    // check input datatype
    for (attr <- outputAttributes) {
      try ArrowConverterUtils.createArrowField(attr)
      catch {
        case e: UnsupportedOperationException =>
          logInfo(s"${attr.dataType} is not supported in VeloxColumnarShuffledExchangeExec.")
          return false
      }
    }
    true
  }

  /**
   * Used for table scan validation.
   *
   * @return true if backend supports reading the file format.
   */
  override def supportsReadFileFormat(fileFormat: FileFormat): Boolean = {
    GlutenConfig.getConf.isGazelleBackend && fileFormat.isInstanceOf[ParquetFileFormat] ||
      GlutenConfig.getConf.isVeloxBackend && fileFormat.isInstanceOf[OrcFileFormat] ||
      GlutenConfig.getConf.isVeloxBackend && fileFormat.isInstanceOf[DwrfFileFormat] ||
      GlutenConfig.getConf.isVeloxBackend && fileFormat.isInstanceOf[ParquetFileFormat]
  }

  /**
   * Generate Seq[InputPartition] for FileSourceScanExecTransformer.
   */
  def genInputPartitionSeq(relation: HadoopFsRelation,
                           selectedPartitions: Array[PartitionDirectory]): Seq[InputPartition] = {
    InputPartitionsUtil.genInputPartitionSeq(relation, selectedPartitions)
  }

  /**
   * Get the backend api name.
   *
   * @return
   */
  override def getBackendName: String = GlutenConfig.GLUTEN_VELOX_BACKEND
}
