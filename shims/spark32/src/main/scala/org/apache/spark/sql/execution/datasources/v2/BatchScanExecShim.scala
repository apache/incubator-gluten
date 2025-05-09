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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{InputPartition, Scan, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class BatchScanExecShim(
    val output: Seq[AttributeReference],
    @transient val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    val ordering: Option[Seq[SortOrder]] = None,
    @transient val table: Table,
    val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    val applyPartialClustering: Boolean = false,
    val replicatePartitions: Boolean = false)
  extends AbstractBatchScanExec(output, scan, runtimeFilters) {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] = Map()

  def metadataColumns: Seq[AttributeReference] = Seq.empty

  def hasUnsupportedColumns: Boolean = false

  def postDriverMetrics(): Unit = {}

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException("Need to implement this method")
  }

  // to comply v3.3. and v3.2, change return type from Seq[InputPartition] to current
  @transient protected lazy val filteredPartitions: Seq[Seq[InputPartition]] = {
    val dataSourceFilters = runtimeFilters.flatMap {
      case DynamicPruningExpression(e) => DataSourceStrategy.translateRuntimeFilter(e)
      case _ => None
    }

    if (dataSourceFilters.nonEmpty) {
      val originalPartitioning = outputPartitioning

      // the cast is safe as runtime filters are only assigned if the scan can be filtered
      val filterableScan = scan.asInstanceOf[SupportsRuntimeFiltering]
      filterableScan.filter(dataSourceFilters.toArray)

      // call toBatch again to get filtered partitions
      val newPartitions = scan.toBatch.planInputPartitions()

      originalPartitioning match {
        case p: DataSourcePartitioning if p.numPartitions != newPartitions.size =>
          throw new SparkException(
            "Data source must have preserved the original partitioning during runtime filtering; " +
              s"reported num partitions: ${p.numPartitions}, " +
              s"num partitions after runtime filtering: ${newPartitions.size}")
        case _ =>
        // no validation is needed as the data source did not report any specific partitioning
      }

      newPartitions.map(Seq(_))
    } else {
      partitions.map(Seq(_))
    }
  }

  @transient lazy val pushedAggregate: Option[Aggregation] = None

  final override protected def otherCopyArgs: Seq[AnyRef] = {
    Seq(
      keyGroupedPartitioning,
      ordering,
      table,
      commonPartitionValues,
      // Box boolean to match `AnyRef`
      Boolean.box(applyPartialClustering),
      Boolean.box(replicatePartitions)
    )
  }
}

abstract class ArrowBatchScanExecShim(original: BatchScanExec) extends DataSourceV2ScanExecBase {
  @transient override lazy val partitions: Seq[InputPartition] = original.partitions
}
