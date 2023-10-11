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

import io.glutenproject.backendsapi.TransformerApi
import io.glutenproject.extension.ValidationResult
import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.utils.InputPartitionsUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateMap, Explode, Generator, JsonTuple, Literal, PosExplode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.BitSet

import java.util

class TransformerApiImpl extends TransformerApi with Logging {

  /**
   * Do validate for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def validateColumnarShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan): Boolean = {
    new ValidatorApiImpl().doSchemaValidate(child.schema)
  }

  /** Generate Seq[InputPartition] for FileSourceScanExecTransformer. */
  def genInputPartitionSeq(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean): Seq[InputPartition] = {
    InputPartitionsUtil(
      relation,
      selectedPartitions,
      output,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan)
      .genInputPartitionSeq()
  }

  override def postProcessNativeConfig(
      nativeConfMap: util.Map[String, String],
      backendPrefix: String): Unit = {
    // TODO: IMPLEMENT SPECIAL PROCESS FOR VELOX BACKEND
  }

  override def validateGenerator(generator: Generator, outer: Boolean): ValidationResult = {
    if (outer) {
      ValidationResult.notOk(s"Velox backend does not support outer")
    }
    generator match {
      case generator: JsonTuple =>
        ValidationResult.notOk(s"Velox backend does not support this json_tuple")
      case generator: PosExplode =>
        // TODO(yuan): support posexplode and remove this check
        ValidationResult.notOk(s"Velox backend does not support this posexplode")
      case explode: Explode if (explode.child.isInstanceOf[CreateMap]) =>
        // explode(MAP(col1, col2))
        ValidationResult.notOk(s"Velox backend does not support MAP datatype")
      case explode: Explode if (explode.child.isInstanceOf[Literal]) =>
        // explode(ARRAY(1, 2, 3))
        ValidationResult.notOk(s"Velox backend does not support literal Array datatype")
      case explode: Explode =>
        explode.child.dataType match {
          case _: MapType =>
            ValidationResult.notOk(s"Velox backend does not support MAP datatype")
          case _ =>
            ValidationResult.ok
        }
      case _ =>
        ValidationResult.ok
    }
  }

  override def createDateDiffParamList(
      start: ExpressionNode,
      end: ExpressionNode): Iterable[ExpressionNode] = {
    List(start, end)
  }

  override def createLikeParamList(
      left: ExpressionNode,
      right: ExpressionNode,
      escapeChar: ExpressionNode): Iterable[ExpressionNode] = {
    List(left, right, escapeChar)
  }
}
