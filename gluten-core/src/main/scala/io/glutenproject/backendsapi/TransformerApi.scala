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
package io.glutenproject.backendsapi

import io.glutenproject.extension.ValidationResult
import io.glutenproject.substrait.expression.ExpressionNode

import org.apache.spark.sql.catalyst.expressions.{Attribute, Generator}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.util.collection.BitSet

import java.util

trait TransformerApi {

  /**
   * Do validate for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  def validateColumnarShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan): Boolean

  /** Generate Seq[InputPartition] for FileSourceScanExecTransformer. */
  def genInputPartitionSeq(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean): Seq[InputPartition]

  /**
   * Post process native config For example, for ClickHouse backend, sync 'spark.executor.cores' to
   * 'spark.gluten.sql.columnar.backend.ch.runtime_settings.max_threads'
   */
  def postProcessNativeConfig(
      nativeConfMap: util.Map[String, String],
      backendPrefix: String): Unit = {}

  def getSupportExpressionClassName: util.Set[String] = {
    util.Collections.emptySet()
  }

  def getPlanOutput(plan: SparkPlan): Seq[Attribute] = {
    plan.output
  }

  def validateGenerator(generator: Generator, outer: Boolean): ValidationResult =
    ValidationResult.ok

  def createDateDiffParamList(start: ExpressionNode, end: ExpressionNode): Iterable[ExpressionNode]

  def createLikeParamList(
      left: ExpressionNode,
      right: ExpressionNode,
      escapeChar: ExpressionNode): Iterable[ExpressionNode]

  def createCheckOverflowExprNode(
      args: java.lang.Object,
      substraitExprName: String,
      childNode: ExpressionNode,
      dataType: DecimalType,
      nullable: Boolean,
      nullOnOverflow: Boolean): ExpressionNode

  def getNativePlanString(substraitPlan: Array[Byte], details: Boolean): String
}
