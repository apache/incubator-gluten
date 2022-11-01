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

import io.glutenproject.vectorized.{BlockNativeConverter, BlockSplitIterator, CHNativeBlock, CloseablePartitionedBlockIterator, NativePartitioning}

import org.apache.spark.{Partitioner, RangePartitioner, ShuffleDependency}
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.types._
import io.glutenproject.vectorized.{BlockNativeConverter, BlockSplitIterator, CHNativeBlock, CloseablePartitionedBlockIterator, NativePartitioning}
import org.apache.spark.sql.vectorized.ColumnarBatch
import play.api.libs.json._
import io.glutenproject.execution.SortExecTransformer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.byteswap32

// In spark RangePartitioner, the rangeBounds is private, so we make a copied-implementation here.
class RangePartitionerBoundsGenerator [K : Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    ordering: Seq[SortOrder],
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
        sketched.foreach { case (idx, n, sample) =>
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
          "expression":"...",
          "data_type":"...",
          "direction":0
        },
        ...
      ],
      "range_bounds":[
        [...],
        [...],
        ...
      ]
    }
  */
  def buildOrderingJson(ordering: Seq[SortOrder]): JsValue = {
    val data = ordering.map {
      order => {
        val orderJson = Json.toJson(Map(
          "expression" -> Json.toJson(s"${order.child}"),
          "data_type" -> Json.toJson(order.dataType.toString),
          "direction" -> Json.toJson(
            SortExecTransformer.transformSortDirection(order.direction.sql,
              order.nullOrdering.sql))
        ))
        orderJson
      }
    }
    Json.toJson(data)
  }

  def buildRangeBoundJson(row: UnsafeRow, ordering: Seq[SortOrder]): JsValue = {
    val data = Json.toJson(
      (0 until row.numFields).map {
        i => {
          val order = ordering(i)
          order.dataType match {
            case _: BooleanType => Json.toJson(row.getBoolean(i))
            case _: ByteType => Json.toJson(row.getByte(i))
            case _: ShortType => Json.toJson(row.getShort(i))
            case _: IntegerType => Json.toJson(row.getInt(i))
            case _: LongType => Json.toJson(row.getLong(i))
            case _: FloatType => Json.toJson(row.getFloat(i))
            case _: DoubleType => Json.toJson(row.getDouble(i))
            case _: StringType => Json.toJson(row.getString(i))
            case d =>
              throw new IllegalArgumentException(s"Unsupported data type ${d.toString}")
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
        }
      )
    )
    data
  }

  // Make a json structure that can be passed to native engine
  def getRangeBoundsJsonString(): String = {
    val bounds = getRangeBounds()
    val data = Json.toJson(
      Map(
        "ordering" -> buildOrderingJson(ordering),
        "range_bounds" -> buildRangeBoundsJson()
      )
    )
    Json.stringify(data)
  }
}
