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

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.{BatchCarrierRow, PlaceholderRow, SparkRowIterator, TerminalRow}
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.vectorized.{CHBlockConverterJniWrapper, CHNativeBlock}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class CHRDDScanTransformer(
    outputAttributes: Seq[Attribute],
    rdd: RDD[InternalRow],
    name: String,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val outputOrdering: Seq[SortOrder]
) extends RDDScanTransformer(outputAttributes, outputPartitioning, outputOrdering) {

  override protected def doValidateInternal(): ValidationResult = {
    output
      .foreach(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
    ValidationResult.succeeded
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val localSchema = this.schema
    val fieldNames = output.map(ConverterUtils.genColumnNameWithExprId).toArray
    val fieldTypes = output
      .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable).toProtobuf.toByteArray)
      .toArray

    rdd.mapPartitions(
      it => {
        if (it.hasNext) {
          val projection = UnsafeProjection.create(localSchema)

          val res: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {
            var sample = false
            var isBatch = false
            var byteArrayIterator: Iterator[Array[Byte]] = _
            private var last_address: Long = 0

            override def hasNext: Boolean = {
              if (isBatch || !sample) {
                it.hasNext
              } else {
                if (last_address != 0) {
                  CHBlockConverterJniWrapper.freeBlock(last_address)
                  last_address = 0
                }
                byteArrayIterator.hasNext
              }
            }

            override def next(): ColumnarBatch = {
              def findNextTerminalRow: TerminalRow = {
                while (it.hasNext) {
                  it.next().asInstanceOf[BatchCarrierRow] match {
                    case _: PlaceholderRow =>
                    case t: TerminalRow =>
                      return t
                  }
                }
                throw new GlutenException("The next terminal row not found")
              }

              if (isBatch) {
                return findNextTerminalRow.batch()
              }

              if (sample) {
                val slice = byteArrayIterator.take(8192)
                val sparkRowIterator = new SparkRowIterator(slice)
                last_address = CHBlockConverterJniWrapper
                  .convertSparkRowsToCHColumn(sparkRowIterator, fieldNames, fieldTypes)
                return new CHNativeBlock(last_address).toColumnarBatch
              }

              sample = true
              val data = it.next()
              data match {
                case _: BatchCarrierRow =>
                  isBatch = true
                  findNextTerminalRow.batch()
                case _ =>
                  byteArrayIterator = it.map {
                    case u: UnsafeRow => u.getBytes
                    case i: InternalRow => projection.apply(i).getBytes
                  }

                  // deal first block
                  val bytes = data match {
                    case u: UnsafeRow => u.getBytes
                    case i: InternalRow => projection.apply(i).getBytes
                  }

                  val sparkRowIterator = new SparkRowIterator(Iterator.apply(bytes))
                  last_address = CHBlockConverterJniWrapper
                    .convertSparkRowsToCHColumn(sparkRowIterator, fieldNames, fieldTypes)
                  new CHNativeBlock(last_address).toColumnarBatch
              }
            }

          }
          res
        } else {
          Iterator.empty
        }
      })
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(outputAttributes, rdd, name, outputPartitioning, outputOrdering)
}

object CHRDDScanTransformer {
  def replace(rdd: RDDScanExec): RDDScanTransformer =
    CHRDDScanTransformer(
      rdd.output,
      rdd.inputRDD,
      rdd.nodeName,
      rdd.outputPartitioning,
      rdd.outputOrdering)
}
