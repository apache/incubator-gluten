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
import io.glutenproject.backendsapi.{BackendsApiManager, TransformerApi}
import io.glutenproject.expression.ExpressionConverter
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.SelectionNode
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.utils.CHInputPartitionsUtil

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.utils.RangePartitionerBoundsGenerator
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.v1.ClickHouseFileIndex
import org.apache.spark.sql.types.StructField

import java.util

class CHTransformerApi extends TransformerApi with Logging {

  /**
   * Do validate for ColumnarShuffleExchangeExec. For ClickHouse backend, it will return true
   * directly.
   *
   * @return
   */
  override def validateColumnarShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan): Boolean = {
    val outputAttributes = child.output
    // check repartition expression
    val substraitContext = new SubstraitContext
    outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        !(exprs
          .map(
            expr => {
              val node = ExpressionConverter
                .replaceWithExpressionTransformer(expr, outputAttributes)
                .doTransform(substraitContext.registeredFunction)
              if (!node.isInstanceOf[SelectionNode]) {
                // This is should not happen.
                logDebug("Expressions are not supported in HashPartitioning.")
                false
              } else {
                true
              }
            })
          .exists(_ == false)) ||
        BackendsApiManager.getSettings.supportShuffleWithProject(outputPartitioning, child)
      case rangePartitoning: RangePartitioning =>
        GlutenConfig.getConf.enableColumnarSort &&
        RangePartitionerBoundsGenerator.supportedOrderings(rangePartitoning, child)
      case _ => true
    }
  }

  /**
   * Used for table scan validation.
   *
   * @return
   *   true if backend supports reading the file format.
   */
  def supportsReadFileFormat(
      fileFormat: ReadFileFormat,
      fields: Array[StructField],
      partTable: Boolean,
      paths: Seq[String]): Boolean =
    BackendsApiManager.getSettings.supportFileFormatRead(fileFormat, fields, partTable, paths)

  /** Generate Seq[InputPartition] for FileSourceScanExecTransformer. */
  def genInputPartitionSeq(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory]): Seq[InputPartition] = {
    if (relation.location.isInstanceOf[ClickHouseFileIndex]) {
      // Generate NativeMergeTreePartition for MergeTree
      relation.location.asInstanceOf[ClickHouseFileIndex].partsPartitions
    } else {
      // Generate FilePartition for Parquet
      CHInputPartitionsUtil.genInputPartitionSeq(relation, selectedPartitions)
    }
  }

  override def postProcessNativeConfig(
      nativeConfMap: util.Map[String, String],
      backendPrefix: String): Unit = {
    // IMPLEMENT POST PROCESS FOR CLICKHOUSE BACKEND
    val settingPrefix = "spark.gluten.sql.columnar.backend." + backendPrefix + ".runtime_conf."
    if (nativeConfMap.getOrDefault("spark.memory.offHeap.enabled", "false").toBoolean) {
      val offHeapSize =
        nativeConfMap.getOrDefault("spark.gluten.memory.offHeap.size.in.bytes", "0").toLong
      if (offHeapSize > 0) {
        val maxBytesBeforeExteralSort = offHeapSize * 0.5
        nativeConfMap.put(
          settingPrefix + "max_bytes_before_external_sort",
          maxBytesBeforeExteralSort.toLong.toString)

        val maxBytesBeforeExternalGroupBy = offHeapSize * 0.5
        nativeConfMap.put(
          settingPrefix + "max_bytes_before_external_sort",
          maxBytesBeforeExternalGroupBy.toLong.toString)
      }
    }
  }
}
