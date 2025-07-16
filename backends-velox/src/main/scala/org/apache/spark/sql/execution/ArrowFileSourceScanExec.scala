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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.concurrent.duration.NANOSECONDS

case class ArrowFileSourceScanExec(original: FileSourceScanExec)
  extends ArrowFileSourceScanLikeShim(original)
  with BaseArrowScanExec {

  lazy val inputRDD: RDD[InternalRow] = original.inputRDD

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

  override def output: Seq[Attribute] = original.output

  override def doCanonicalize(): FileSourceScanExec = original.doCanonicalize()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal {
      batches =>
        new Iterator[ColumnarBatch] {

          override def hasNext: Boolean = {
            // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
            val startNs = System.nanoTime()
            val res = batches.hasNext
            scanTime += NANOSECONDS.toMillis(System.nanoTime() - startNs)
            res
          }

          override def next(): ColumnarBatch = {
            val batch = batches.next()
            numOutputRows += batch.numRows()
            batch
          }
        }
    }
  }
}
