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

import io.glutenproject.backendsapi.{BackendsApiManager, ITransformerApi}
import io.glutenproject.expression.{ExpressionConverter, ExpressionTransformer}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.SelectionNode
import io.glutenproject.utils.InputPartitionsUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.v1.ClickHouseFileIndex

class CHTransformerApi extends ITransformerApi with Logging {

  /**
   * Do validate for ColumnarShuffleExchangeExec. For ClickHouse backend, it will return true
   * directly.
   *
   * @return
   */
  override def validateColumnarShuffleExchangeExec(
      outputPartitioning: Partitioning,
      outputAttributes: Seq[Attribute]): Boolean = {
    !outputPartitioning.isInstanceOf[RangePartitioning]

    // check repartition expression
    val substraitContext = new SubstraitContext
    outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        !(exprs
          .map(
            expr => {
              val node = ExpressionConverter
                .replaceWithExpressionTransformer(expr, outputAttributes)
                .asInstanceOf[ExpressionTransformer]
                .doTransform(substraitContext.registeredFunction)
              if (!node.isInstanceOf[SelectionNode]) {
                logDebug("Expressions are not supported in HashPartitioning.")
                false
              } else {
                true
              }
            })
          .exists(_ == false))
      case RangePartitioning(_, _) => false
      case _ => true
    }
  }

  /**
   * Used for table scan validation.
   *
   * @return
   *   true if backend supports reading the file format.
   */
  def supportsReadFileFormat(fileFormat: FileFormat): Boolean =
    BackendsApiManager.getSettings.supportedFileFormats().contains(fileFormat.getClass)

  /** Generate Seq[InputPartition] for FileSourceScanExecTransformer. */
  def genInputPartitionSeq(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory]): Seq[InputPartition] = {
    if (relation.location.isInstanceOf[ClickHouseFileIndex]) {
      // Generate NativeMergeTreePartition for MergeTree
      relation.location.asInstanceOf[ClickHouseFileIndex].partsPartitions
    } else {
      // Generate FilePartition for Parquet
      InputPartitionsUtil.genInputPartitionSeq(relation, selectedPartitions)
    }
  }
}
