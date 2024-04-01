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
package org.apache.spark.sql.execution

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.memory.nmm.NativeMemoryManagers
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.vectorized.{ColumnarBatchSerializeResult, ColumnarBatchSerializerJniWrapper}

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, IdentityBroadcastMode, Partitioning}
import org.apache.spark.sql.execution.joins.{HashedRelation, HashedRelationBroadcastMode, LongHashedRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.TaskResources

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer;

// Utility methods to convert Vanilla broadcast relations from/to Velox broadcast relations.
// FIXME: Truncate output with batch size.
object BroadcastUtils {
  def veloxToSparkUnsafe[F, T](
      context: SparkContext,
      mode: BroadcastMode,
      from: Broadcast[F],
      fn: Iterator[ColumnarBatch] => Iterator[InternalRow]): Broadcast[T] = {
    mode match {
      case HashedRelationBroadcastMode(_, _) =>
        // ColumnarBuildSideRelation to HashedRelation.
        val fromBroadcast = from.asInstanceOf[Broadcast[ColumnarBuildSideRelation]]
        val fromRelation = fromBroadcast.value.asReadOnlyCopy()
        var rowCount: Long = 0
        val toRelation = TaskResources.runUnsafe {
          val rowIterator = fn(fromRelation.deserialized.flatMap {
            cb =>
              rowCount += cb.numRows()
              Iterator(cb)
          })
          mode.transform(rowIterator, Some(rowCount))
        }
        // Rebroadcast Spark relation.
        context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
      case IdentityBroadcastMode =>
        // ColumnarBuildSideRelation to HashedRelation.
        val fromBroadcast = from.asInstanceOf[Broadcast[ColumnarBuildSideRelation]]
        val fromRelation = fromBroadcast.value.asReadOnlyCopy()
        val toRelation = TaskResources.runUnsafe {
          val rowIterator = fn(fromRelation.deserialized)
          val rowArray = new ArrayBuffer[InternalRow]

          /**
           * [[org.apache.gluten.execution.VeloxColumnarToRowExec.toRowIterator()]] creates a single
           * UnsafeRow. The iterator uses this same unsafe row and keep on changing the pointer to
           * point to new value. If we directly call rowIterator.toArray() then all the elements in
           * array points to same UnsafeRow object resulting in wrong output. here we need to create
           * a array having individual UnsafeRow object.
           */
          while (rowIterator.hasNext) {
            val unsafeRow = rowIterator.next().asInstanceOf[UnsafeRow]
            rowArray.append(unsafeRow.copy())
          }
          rowArray.toArray
        }
        // Rebroadcast Spark relation.
        context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
      case _ => throw new IllegalStateException("Unexpected broadcast mode: " + mode)
    }
  }

  def sparkToVeloxUnsafe[F, T](
      context: SparkContext,
      mode: BroadcastMode,
      schema: StructType,
      from: Broadcast[F],
      fn: Iterator[InternalRow] => Iterator[ColumnarBatch]): Broadcast[T] = {
    mode match {
      case HashedRelationBroadcastMode(_, _) =>
        // HashedRelation to ColumnarBuildSideRelation.
        val fromBroadcast = from.asInstanceOf[Broadcast[HashedRelation]]
        val fromRelation = fromBroadcast.value.asReadOnlyCopy()
        val toRelation = TaskResources.runUnsafe {
          val batchItr: Iterator[ColumnarBatch] = fn(reconstructRows(fromRelation))
          val serialized: Array[Array[Byte]] = serializeStream(batchItr) match {
            case ColumnarBatchSerializeResult.EMPTY =>
              Array()
            case result: ColumnarBatchSerializeResult =>
              Array(result.getSerialized)
          }
          ColumnarBuildSideRelation(
            SparkShimLoader.getSparkShims.attributesFromStruct(schema),
            serialized)
        }
        // Rebroadcast Velox relation.
        context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
      case IdentityBroadcastMode =>
        // Array[InternalRow] to ColumnarBuildSideRelation.
        val fromBroadcast = from.asInstanceOf[Broadcast[Array[InternalRow]]]
        val fromRelation = fromBroadcast.value
        val toRelation = TaskResources.runUnsafe {
          val batchItr: Iterator[ColumnarBatch] = fn(fromRelation.iterator)
          val serialized: Array[Array[Byte]] = serializeStream(batchItr) match {
            case ColumnarBatchSerializeResult.EMPTY =>
              Array()
            case result: ColumnarBatchSerializeResult =>
              Array(result.getSerialized)
          }
          ColumnarBuildSideRelation(
            SparkShimLoader.getSparkShims.attributesFromStruct(schema),
            serialized)
        }
        // Rebroadcast Velox relation.
        context.broadcast(toRelation).asInstanceOf[Broadcast[T]]
      case _ => throw new IllegalStateException("Unexpected broadcast mode: " + mode)
    }
  }

  def getBroadcastMode(partitioning: Partitioning): BroadcastMode = {
    partitioning match {
      case BroadcastPartitioning(mode) =>
        mode
      case _ =>
        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning.toString)
    }
  }

  def serializeStream(batches: Iterator[ColumnarBatch]): ColumnarBatchSerializeResult = {
    val filtered = batches
      .filter(_.numRows() != 0)
      .map(
        b => {
          ColumnarBatches.retain(b)
          b
        })
      .toArray
    if (filtered.isEmpty) {
      return ColumnarBatchSerializeResult.EMPTY
    }
    val batchRuntime = ColumnarBatches.getRuntime(filtered.toList.asJava)
    val handleArray = filtered.map(ColumnarBatches.getNativeHandle)
    val serializeResult =
      try {
        ColumnarBatchSerializerJniWrapper
          .forRuntime(batchRuntime)
          .serialize(
            handleArray,
            NativeMemoryManagers
              .contextInstance("BroadcastRelation")
              .getNativeInstanceHandle)
      } finally {
        filtered.foreach(ColumnarBatches.release)
      }
    serializeResult
  }

  private def reconstructRows(relation: HashedRelation): Iterator[InternalRow] = {
    // It seems that LongHashedRelation and UnsafeHashedRelation don't follow the same
    //  criteria while getting values from them.
    // Should review the internals of this part of code.
    relation match {
      case relation: LongHashedRelation if relation.keyIsUnique =>
        relation.keys().map(k => relation.getValue(k))
      case relation: LongHashedRelation if !relation.keyIsUnique =>
        relation.keys().flatMap(k => relation.get(k))
      case other => other.valuesWithKeyIndex().map(_.getValue)
    }
  }
}
