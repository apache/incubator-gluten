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
package org.apache.spark.shuffle.utils

import io.glutenproject.execution.SortExecTransformer
import io.glutenproject.expression.ExpressionConverter
import io.glutenproject.expression.ExpressionTransformer
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.vectorized.{BlockNativeConverter, BlockSplitIterator, CHNativeBlock, CloseablePartitionedBlockIterator, NativePartitioning}

import org.apache.spark.{Partitioner, RangePartitioner, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import io.substrait.proto.ProjectRel
import io.substrait.proto.Rel
import io.substrait.proto.RelCommon
import play.api.libs.json._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32
// In spark RangePartitioner, the rangeBounds is private, so we make a copied-implementation here.
// It is based on the fact that, there has been a pre-projection before the range partition
// and remove all function expressions in the sort ordering expressions.
class RangePartitionerBoundsGenerator[K: Ordering: ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    ordering: Seq[SortOrder],
    inputAttributes: Seq[Attribute],
    private var ascending: Boolean = true,
    val samplePointsPerPartitionHint: Int = 20
) {
  def getRangeBounds(): Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach {
          case (idx, n, sample) =>
            if (fraction * n > sampleSizePerPartition) {
              imbalancedPartitions += idx
            } else {
              val weight = (n.toDouble / sample.length).toFloat
              for (key <- sample) {
                candidates += ((key, weight))
              }
            }
        }
        if (imbalancedPartitions.nonEmpty) {
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, math.min(partitions, candidates.size))
      }
    }
  }

  /*
  return json structure
  {
    "ordering":[
      {
        "column_ref":0,
        "data_type":"xxx",
        "is_nullable":true,
        "direction":0
      },
        ...
    ],
    "range_bounds":[
        {
          "is_null":false,
          "value": ...
        },
        {
          "is_null":true
        },
        ...
    ]
  }
   */
  def getExpressionFiedlReference(ordering: SortOrder): Int = {
    val substraitCtx = new SubstraitContext()
    val funcs = substraitCtx.registeredFunction
    val colExpr =
      ExpressionConverter.replaceWithExpressionTransformer(ordering.child, inputAttributes)
    val projExprNode = colExpr.asInstanceOf[ExpressionTransformer].doTransform(funcs)
    val pb = projExprNode.toProtobuf
    if (!pb.hasSelection()) {
      throw new IllegalArgumentException(s"A sorting field should be an attribute")
    }
    pb.getSelection().getDirectReference().getStructField.getField()
  }
  def buildOrderingJson(ordering: Seq[SortOrder]): JsValue = {
    val data = ordering.map {
      order =>
        {
          val orderJson = Json.toJson(
            Map(
              // expression is a protobuf of io.substrait.proto.Expression
              "column_ref" -> Json.toJson(getExpressionFiedlReference(order)),
              "data_type" -> Json.toJson(order.dataType.toString),
              "is_nullable" -> Json.toJson(order.nullable),
              "direction" -> Json.toJson(SortExecTransformer
                .transformSortDirection(order.direction.sql, order.nullOrdering.sql))
            ))
          orderJson
        }
    }
    Json.toJson(data)
  }

  def getFieldValue(row: UnsafeRow, dataType: DataType, field: Int): JsValue = {
    dataType match {
      case _: BooleanType => Json.toJson(row.getBoolean(field))
      case _: ByteType => Json.toJson(row.getByte(field))
      case _: ShortType => Json.toJson(row.getShort(field))
      case _: IntegerType => Json.toJson(row.getInt(field))
      case _: LongType => Json.toJson(row.getLong(field))
      case _: FloatType => Json.toJson(row.getFloat(field))
      case _: DoubleType => Json.toJson(row.getDouble(field))
      case _: StringType => Json.toJson(row.getString(field))
      case _: DateType => Json.toJson(row.getShort(field))
      case d =>
        throw new IllegalArgumentException(s"Unsupported data type ${d.toString}")
    }
  }
  def buildRangeBoundJson(row: UnsafeRow, ordering: Seq[SortOrder]): JsValue = {
    val data = Json.toJson(
      (0 until row.numFields).map {
        i =>
          {
            if (row.isNullAt(i)) {
              Json.toJson(
                Map(
                  "is_null" -> Json.toJson(true)
                ))
            } else {
              val order = ordering(i)
              Json.toJson(
                Map(
                  "is_null" -> Json.toJson(false),
                  "value" -> getFieldValue(row, order.dataType, i)
                ))
            }
          }
      }
    )
    data
  }

  def buildRangeBoundsJson(): JsValue = {
    val bounds = getRangeBounds()
    val data = Json.toJson(
      bounds.map(
        bound => {
          // Be careful, it should be an unsafe row here
          val row = bound.asInstanceOf[UnsafeRow]
          buildRangeBoundJson(row, ordering)
        })
    )
    data
  }

  // Make a json structure that can be passed to native engine
  def getRangeBoundsJsonString(): String = {
    val data = Json.toJson(
      Map(
        "ordering" -> buildOrderingJson(ordering),
        "range_bounds" -> buildRangeBoundsJson()
      )
    )
    Json.stringify(data)
  }
}
