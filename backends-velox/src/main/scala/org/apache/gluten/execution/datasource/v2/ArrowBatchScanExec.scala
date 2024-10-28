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
package org.apache.gluten.execution.datasource.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.{Batch, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.BaseArrowScanExec
import org.apache.spark.sql.execution.datasources.v2.{ArrowBatchScanExecShim, BatchScanExec}

case class ArrowBatchScanExec(original: BatchScanExec)
  extends ArrowBatchScanExecShim(original)
  with BaseArrowScanExec {

  @transient lazy val batch: Batch = original.batch

  override lazy val readerFactory: PartitionReaderFactory = original.readerFactory

  override lazy val inputRDD: RDD[InternalRow] = original.inputRDD

  override def outputPartitioning: Partitioning = original.outputPartitioning

  override def scan: Scan = original.scan

  override def doCanonicalize(): ArrowBatchScanExec =
    this.copy(original = original.doCanonicalize())

  override def nodeName: String = "Arrow" + original.nodeName

  override def output: Seq[Attribute] = original.output
}
