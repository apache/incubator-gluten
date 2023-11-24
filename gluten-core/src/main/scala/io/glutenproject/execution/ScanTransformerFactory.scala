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

import io.glutenproject.execution.FilterHandler.{flattenCondition, getLeftFilters}
import io.glutenproject.expression.ExpressionConverter
import io.glutenproject.sql.shims.SparkShimLoader

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}

import scala.reflect.runtime.{universe => ru}

object ScanTransformerFactory {

  private val IcebergScanClassName = "org.apache.iceberg.spark.source.SparkBatchQueryScan"
  private val IcebergTransformerClassName = "io.glutenproject.execution.IcebergScanTransformer"

  def createFileSourceScanTransformer(
      scanExec: FileSourceScanExec,
      reuseSubquery: Boolean,
      validation: Boolean = false): FileSourceScanExecTransformer = {
    // TODO: Add delta match here
    val newPartitionFilters = if (validation) {
      scanExec.partitionFilters
    } else {
      ExpressionConverter.transformDynamicPruningExpr(scanExec.partitionFilters, reuseSubquery)
    }
    new FileSourceScanExecTransformer(
      scanExec.relation,
      scanExec.output,
      scanExec.requiredSchema,
      newPartitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      scanExec.dataFilters,
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan
    )
  }

  def createFileSourceScanTransformer(
      scanExec: FileSourceScanExec,
      reuseSubquery: Boolean,
      filter: FilterExec): FileSourceScanExecTransformer = {
    val leftFilters =
      getLeftFilters(scanExec.dataFilters, flattenCondition(filter.condition))
    // transform BroadcastExchangeExec to ColumnarBroadcastExchangeExec in partitionFilters
    val newPartitionFilters =
      ExpressionConverter.transformDynamicPruningExpr(scanExec.partitionFilters, reuseSubquery)
    new FileSourceScanExecTransformer(
      scanExec.relation,
      scanExec.output,
      scanExec.requiredSchema,
      newPartitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      scanExec.dataFilters ++ leftFilters,
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan
    )
  }

  def createBatchScanTransformer(
      batchScanExec: BatchScanExec,
      reuseSubquery: Boolean,
      validation: Boolean = false): BatchScanExecTransformer = {
    val newPartitionFilters = if (validation) {
      batchScanExec.runtimeFilters
    } else {
      ExpressionConverter.transformDynamicPruningExpr(batchScanExec.runtimeFilters, reuseSubquery)
    }
    val scan = batchScanExec.scan
    scan match {
      case _ if scan.getClass.getName == IcebergScanClassName =>
        createBatchScanTransformer(IcebergTransformerClassName, batchScanExec, newPartitionFilters)
      case _ =>
        new BatchScanExecTransformer(
          batchScanExec.output,
          batchScanExec.scan,
          newPartitionFilters,
          table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScanExec))
    }
  }

  def supportedBatchScan(scan: Scan): Boolean = scan match {
    case _: FileScan => true
    case _ if scan.getClass.getName == IcebergScanClassName => true
    case _ => false
  }

  private def createBatchScanTransformer(
      className: String,
      params: Any*): BatchScanExecTransformer = {
    val classMirror = ru.runtimeMirror(getClass.getClassLoader)
    val classModule = classMirror.staticModule(className)
    val mirror = classMirror.reflectModule(classModule)
    val apply = mirror.symbol.typeSignature.member(ru.TermName("apply")).asMethod
    val objMirror = classMirror.reflect(mirror.instance)
    objMirror.reflectMethod(apply)(params: _*).asInstanceOf[BatchScanExecTransformer]
  }
}
