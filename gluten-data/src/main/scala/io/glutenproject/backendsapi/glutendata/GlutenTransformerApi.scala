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

package io.glutenproject.backendsapi.glutendata

import io.glutenproject.backendsapi.{BackendsApiManager, TransformerApi}
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.utils.{GlutenArrowUtil, InputPartitionsUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

abstract class GlutenTransformerApi extends TransformerApi with Logging {

  /**
   * Do validate for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def validateColumnarShuffleExchangeExec(outputPartitioning: Partitioning,
                                                   outputAttributes: Seq[Attribute]): Boolean = {
    // Complex type is not supported.
    for (attr <- outputAttributes) {
      attr.dataType match {
        case _: ArrayType => return false
        case _: MapType => return false
        case _: StructType => return false
        case _ =>
      }
    }
    // check input datatype
    for (attr <- outputAttributes) {
      try GlutenArrowUtil.createArrowField(attr)
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
  override def supportsReadFileFormat(fileFormat: ReadFileFormat,
                                      fields: Array[StructField],
                                      partTable: Boolean,
                                      paths: Seq[String]): Boolean = {
    BackendsApiManager.getSettings
      .supportFileFormatRead(fileFormat, fields, partTable, paths)
  }

  /**
   * Generate Seq[InputPartition] for FileSourceScanExecTransformer.
   */
  def genInputPartitionSeq(relation: HadoopFsRelation,
                           selectedPartitions: Array[PartitionDirectory]): Seq[InputPartition] = {
    InputPartitionsUtil.genInputPartitionSeq(relation, selectedPartitions)
  }

  def postProcessNativeConfig(nativeConfMap: util.Map[String, String],
    backendPrefix: String): Unit = {
    /// TODO: IMPLEMENT SPECIAL PROCESS FOR VELOX BACKEND
  }
}
