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
package io.glutenproject.execution

import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.substrait.rel.SplitInfo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.iceberg.spark.source.GlutenIcebergSourceUtil

class IcebergScanTransformer(
    output: Seq[AttributeReference],
    @transient scan: Scan,
    runtimeFilters: Seq[Expression],
    @transient table: Table)
  extends BatchScanExecTransformer(
    output = output,
    scan = scan,
    runtimeFilters = runtimeFilters,
    table = table) {

  override def filterExprs(): Seq[Expression] = Seq.empty

  override def getPartitionSchema: StructType = new StructType()

  override def getDataSchema: StructType = new StructType()

  override def getInputFilePathsInternal: Seq[String] = Seq.empty

  override lazy val fileFormat: ReadFileFormat = GlutenIcebergSourceUtil.getFileFormat(scan)

  override def doExecuteColumnar(): RDD[ColumnarBatch] = throw new UnsupportedOperationException()

  override def getSplitInfos: Seq[SplitInfo] = {
    getPartitions.zipWithIndex.map {
      case (p, index) => GlutenIcebergSourceUtil.genSplitInfo(p, index)
    }
  }
}

object IcebergScanTransformer {
  def apply(batchScan: BatchScanExec, partitionFilters: Seq[Expression]): IcebergScanTransformer = {
    new IcebergScanTransformer(
      batchScan.output,
      batchScan.scan,
      partitionFilters,
      table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScan))
  }
}
