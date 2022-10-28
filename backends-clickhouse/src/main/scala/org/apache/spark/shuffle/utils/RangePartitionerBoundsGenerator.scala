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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.byteswap32

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

  def getRangeBoundsJsonString(): String = {
    val bounds = getRangeBounds()
    var jsonBuffer = new StringBuffer()
    jsonBuffer.append("{")

    var cnt = 0
    jsonBuffer.append("\"schema\":[")
    ordering.foreach(order => {
      if (cnt != 0) {
        jsonBuffer.append(",")
      }
      val boundRef = order.child.asInstanceOf[BoundReference]
      jsonBuffer.append("\"")
      jsonBuffer.append(boundRef.dataType.typeName)
      jsonBuffer.append("\"")
      cnt += 1
    })
    // end schema
    jsonBuffer.append("],")

    jsonBuffer.append("rows:[")
    (0 until bounds.length).foreach {
      i => {
        val row = bounds(i)
        if (i != 0) {
          jsonBuffer.append(",")
        }

        jsonBuffer.append("[")
        (0 until ordering.size).foreach {
          fieldIndex => {
            if (fieldIndex != 0) {
              jsonBuffer.append(",")
            }

            val order = ordering(fieldIndex).child.asInstanceOf[BoundReference]
            order.dataType match {
              case _: StringType =>
                jsonBuffer.append("\"")
                jsonBuffer.append("\"")
            }
          }
        }
        jsonBuffer.append("]")
      }
    }

    // end rows
    jsonBuffer.append("]")

    jsonBuffer.append("}")
    jsonBuffer.toString
  }
}
