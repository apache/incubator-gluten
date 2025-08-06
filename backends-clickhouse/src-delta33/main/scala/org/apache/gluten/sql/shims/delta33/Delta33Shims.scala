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
package org.apache.gluten.sql.shims.delta33

import org.apache.gluten.execution.{GlutenPlan, MergeTreePartRange}
import org.apache.gluten.extension.{DeltaExpressionExtensionTransformer, ExpressionExtensionTrait}
import org.apache.gluten.sql.shims.DeltaShims

import org.apache.spark.sql.delta.{DeltaParquetFileFormat, RowIndexFilterType}
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.perf.DeltaOptimizedWriterTransformer

import org.apache.hadoop.fs.Path

import java.util.{HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._

class Delta33Shims extends DeltaShims {
  override def supportDeltaOptimizedWriterExec(plan: SparkPlan): Boolean =
    DeltaOptimizedWriterTransformer.support(plan)

  override def offloadDeltaOptimizedWriterExec(plan: SparkPlan): GlutenPlan = {
    DeltaOptimizedWriterTransformer.from(plan)
  }

  override def registerExpressionExtension(): Unit = {
    ExpressionExtensionTrait.registerExpressionExtension(DeltaExpressionExtensionTransformer())
  }

  /**
   * decode ZeroMQ Base85 encoded file path
   *
   * TODO: native size needs to support the ZeroMQ Base85
   */
  override def convertRowIndexFilterIdEncoded(
      partitionColsCnt: Int,
      file: PartitionedFile,
      otherConstantMetadataColumnValues: JMap[String, Object]): JMap[String, Object] = {
    val newOtherConstantMetadataColumnValues: JMap[String, Object] =
      new JHashMap[String, Object]
    for ((k, v) <- otherConstantMetadataColumnValues.asScala) {
      if (k.equalsIgnoreCase(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED)) {
        val decoded = DeletionVectorDescriptor.deserializeFromBase64(v.toString)
        var filePath = new Path(file.filePath.toString()).getParent
        for (_ <- 0 until partitionColsCnt) {
          filePath = filePath.getParent
        }
        val decodedPath = decoded.absolutePath(filePath)
        val newDeletionVectorDescriptor = decoded.copy(
          pathOrInlineDv = decodedPath.toUri.toASCIIString
        )
        newOtherConstantMetadataColumnValues.put(k, JsonUtils.toJson(newDeletionVectorDescriptor))
      } else {
        newOtherConstantMetadataColumnValues.put(k, v)
      }
    }
    newOtherConstantMetadataColumnValues
  }

  override def generateMergeTreePartRange(
      addMergeTreeParts: AddMergeTreeParts,
      start: Long,
      marks: Long,
      size: Long): MergeTreePartRange = {
    val (rowIndexFilterIdEncoded, rowIndexFilterType) =
      if (addMergeTreeParts.deletionVector != null) {
        val tableLocation = new Path(addMergeTreeParts.dirName)
        val decodedPath = addMergeTreeParts.deletionVector.absolutePath(tableLocation)
        val newDeletionVectorDescriptor = addMergeTreeParts.deletionVector.copy(
          pathOrInlineDv = decodedPath.toUri.toASCIIString
        )
        (JsonUtils.toJson(newDeletionVectorDescriptor), RowIndexFilterType.IF_CONTAINED.name())
      } else {
        ("", "")
      }
    MergeTreePartRange(
      addMergeTreeParts.name,
      addMergeTreeParts.dirName,
      addMergeTreeParts.targetNode,
      addMergeTreeParts.bucketNum,
      start,
      marks,
      size,
      rowIndexFilterType,
      rowIndexFilterIdEncoded
    )
  }
}
