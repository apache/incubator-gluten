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
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.execution.{BatchCarrierRow, PlaceholderRow, TerminalRow}
import org.apache.gluten.execution.datasource.GlutenFormatFactory

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.hadoop.mapreduce.TaskAttemptContext

/**
 * Dynamic partition writer with single writer, meaning only one writer is opened at any time for
 * writing. The records to be written are required to be sorted on partition and/or bucket column(s)
 * before writing.
 */
class DynamicPartitionDataSingleWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    customMetrics: Map[String, SQLMetric] = Map.empty)
  extends BaseDynamicPartitionDataWriter(
    description,
    taskAttemptContext,
    committer,
    customMetrics) {

  private var currentPartitionValues: Option[UnsafeRow] = None
  private var currentBucketId: Option[Int] = None

  private val partitionColIndice: Array[Int] =
    description.partitionColumns.flatMap {
      pcol =>
        description.allColumns.zipWithIndex.collect {
          case (acol, index) if acol.name == pcol.name && acol.exprId == pcol.exprId => index
        }
    }.toArray

  private def beforeWrite(record: InternalRow): Unit = {
    val nextPartitionValues = if (isPartitioned) Some(getPartitionValues(record)) else None
    val nextBucketId = if (isBucketed) Some(getBucketId(record)) else None

    if (currentPartitionValues != nextPartitionValues || currentBucketId != nextBucketId) {
      // See a new partition or bucket - write to a new partition dir (or a new bucket file).
      if (isPartitioned && currentPartitionValues != nextPartitionValues) {
        currentPartitionValues = Some(nextPartitionValues.get.copy())
        statsTrackers.foreach(_.newPartition(currentPartitionValues.get))
      }
      if (isBucketed) {
        currentBucketId = nextBucketId
      }

      fileCounter = 0
      renewCurrentWriter(currentPartitionValues, currentBucketId, closeCurrentWriter = true)
    } else if (
      description.maxRecordsPerFile > 0 &&
      recordsInFile >= description.maxRecordsPerFile
    ) {
      renewCurrentWriterIfTooManyRecords(currentPartitionValues, currentBucketId)
    }
  }

  override def write(record: InternalRow): Unit = {
    record match {
      case carrierRow: BatchCarrierRow =>
        carrierRow match {
          case placeholderRow: PlaceholderRow =>
          // Do nothing.
          case terminalRow: TerminalRow =>
            val numRows = terminalRow.batch().numRows()
            if (numRows > 0) {
              val blockStripes = GlutenFormatFactory.rowSplitter
                .splitBlockByPartitionAndBucket(terminalRow.batch(), partitionColIndice, isBucketed)
              val iter = blockStripes.iterator()
              while (iter.hasNext) {
                val blockStripe = iter.next()
                val headingRow = blockStripe.getHeadingRow
                beforeWrite(headingRow)
                val columnBatch = blockStripe.getColumnarBatch
                currentWriter.write(terminalRow.withNewBatch(columnBatch))
                columnBatch.close()
              }
              blockStripes.release()
              for (_ <- 0 until numRows) {
                statsTrackers.foreach(_.newRow(currentWriter.path, record))
              }
              recordsInFile += numRows
            }
        }
      case _ =>
        beforeWrite(record)
        writeRecord(record)
    }
  }
}
