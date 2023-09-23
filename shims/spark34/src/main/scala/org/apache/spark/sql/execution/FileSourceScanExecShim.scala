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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

class FileSourceScanExecShim(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends FileSourceScanExec(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] = Map()

  override def equals(other: Any): Boolean = other match {
    case that: FileSourceScanExecShim =>
      (that.canEqual(this)) && super.equals(that)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def canEqual(other: Any): Boolean = other.isInstanceOf[FileSourceScanExecShim]

  def hasMetadataColumns: Boolean = fileConstantMetadataColumns.nonEmpty

  def hasFieldIds: Boolean = ParquetUtils.hasFieldIds(requiredSchema)
}
