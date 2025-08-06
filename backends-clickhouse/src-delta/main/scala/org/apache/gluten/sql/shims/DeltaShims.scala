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
package org.apache.gluten.sql.shims

import org.apache.gluten.execution.{GlutenPlan, MergeTreePartRange}

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts

import java.util.{HashMap => JHashMap, Map => JMap}

trait DeltaShims {
  def supportDeltaOptimizedWriterExec(plan: SparkPlan): Boolean = false

  def offloadDeltaOptimizedWriterExec(plan: SparkPlan): GlutenPlan = {
    throw new UnsupportedOperationException(
      s"Can't transform ColumnarDeltaOptimizedWriterExec from ${plan.getClass.getSimpleName}")
  }

  def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {}

  def registerExpressionExtension(): Unit = {}

  def convertRowIndexFilterIdEncoded(
      partitionColsCnt: Int,
      file: PartitionedFile,
      otherConstantMetadataColumnValues: JMap[String, Object]): JMap[String, Object] =
    new JHashMap[String, Object]()

  def generateMergeTreePartRange(
      addMergeTreeParts: AddMergeTreeParts,
      start: Long,
      marks: Long,
      size: Long): MergeTreePartRange = {
    MergeTreePartRange(
      addMergeTreeParts.name,
      addMergeTreeParts.dirName,
      addMergeTreeParts.targetNode,
      addMergeTreeParts.bucketNum,
      start,
      marks,
      size,
      "",
      "")
  }
}
