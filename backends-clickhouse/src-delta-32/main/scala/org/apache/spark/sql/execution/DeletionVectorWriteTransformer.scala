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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.ValidatablePlan
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.vectorized.{CHBlockConverterJniWrapper, CHNativeBlock, DeltaWriterJNIWrapper}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.commands.DeletionVectorResult
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.util.{Codec, Utils => DeltaUtils}
import org.apache.spark.sql.execution.datasources.CallTransformer
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.hadoop.fs.Path

import java.util.UUID

case class DeletionVectorWriteTransformer(
    child: SparkPlan,
    table: Path,
    deltaTxn: OptimisticTransaction)
  extends UnaryExecNode
  with ValidatablePlan {
  override def output: Seq[Attribute] = Seq(
    AttributeReference("filePath", StringType, nullable = false)(),
    AttributeReference(
      "deletionVector",
      DeletionVectorWriteTransformer.deletionVectorType,
      nullable = false
    )(),
    AttributeReference("matchedRowCount", LongType, nullable = false)()
  )

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    assert(child.supportsColumnar)
    val prefixLen = DeltaUtils.getRandomPrefixLength(deltaTxn.metadata)
    val tablePathString = DeletionVectorStore.pathToEscapedString(table)
    val packingTargetSize =
      session.conf.get(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE)

    child.executeColumnar().mapPartitions {
      blockIterator =>
        val res = new Iterator[ColumnarBatch] {
          private var last_block: Long = 0

          override def hasNext: Boolean = {
            if (last_block != 0) {
              CHBlockConverterJniWrapper.freeBlock(last_block)
              last_block = 0
            }
            blockIterator.hasNext
          }

          override def next(): ColumnarBatch = {
            val n = blockIterator.next()
            val last_block_address = CHNativeBlock.fromColumnarBatch(n).blockAddress()

            val address = DeltaWriterJNIWrapper
              .deletionVectorWrite(
                last_block_address,
                tablePathString,
                prefixLen,
                packingTargetSize)

            new CHNativeBlock(address).toColumnarBatch
          }
        }
        res
    }

  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): DeletionVectorWriteTransformer = copy(child = newChild)

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
}

object DeletionVectorWriteTransformer {
  private val deletionVectorType: StructType = StructType.apply(
    Seq(
      StructField.apply("storageType", StringType, nullable = false),
      StructField.apply("pathOrInlineDv", StringType, nullable = false),
      StructField.apply("offset", IntegerType, nullable = true),
      StructField.apply("sizeInBytes", IntegerType, nullable = false),
      StructField.apply("cardinality", LongType, nullable = false),
      StructField.apply("maxRowIndex", LongType, nullable = true)
    ))

  def encodeUUID(uuid: String, randomPrefix: String): String = {
    val uuidData = Codec.Base85Codec.encodeUUID(UUID.fromString(uuid))
    // This should always be true and we are relying on it for separating out the
    // prefix again later without having to spend an extra character as a separator.
    assert(uuidData.length == 20)
    // uuidData
    s"$randomPrefix$uuidData"
  }

  def decodeUUID(encodedUuid: String): String = {
    Codec.Base85Codec.decodeUUID(encodedUuid).toString
  }

  def replace(
      aggregated: DataFrame,
      tablePath: Path,
      deltaTxn: OptimisticTransaction,
      spark: SparkSession): Seq[DeletionVectorResult] = {
    val queryExecution = aggregated.queryExecution
    val new_e = DeletionVectorWriteTransformer(queryExecution.sparkPlan, tablePath, deltaTxn)

    val result = CallTransformer(spark, new_e).executedPlan.executeCollect()

    def internalRowToDeletionVectorResult(row: InternalRow): DeletionVectorResult = {
      val filePath = row.getString(0)
      val deletionVector = row.getStruct(1, 6)
      val matchedRowCount = row.getLong(2)
      val offset = if (deletionVector.isNullAt(2)) {
        Option.empty
      } else {
        Some(deletionVector.getInt(2))
      }

      val maxRowIndex = if (deletionVector.isNullAt(5)) {
        Option.empty
      } else {
        Some(deletionVector.getLong(5))
      }

      DeletionVectorResult(
        filePath,
        DeletionVectorDescriptor(
          deletionVector.getString(0),
          deletionVector.getString(1),
          offset,
          deletionVector.getInt(3),
          deletionVector.getLong(4),
          maxRowIndex
        ),
        matchedRowCount
      )
    }

    result.map(internalRowToDeletionVectorResult).toSeq
  }
}
