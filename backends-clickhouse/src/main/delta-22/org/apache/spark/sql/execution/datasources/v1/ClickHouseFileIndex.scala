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
package org.apache.spark.sql.execution.datasources.v1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2

import org.apache.hadoop.fs.Path

case class ClickHouseFileIndex(
    override val spark: SparkSession,
    override val deltaLog: DeltaLog,
    override val path: Path,
    table: ClickHouseTableV2,
    snapshotAtAnalysis: Snapshot,
    partitionFilters: Seq[Expression] = Nil,
    isTimeTravelQuery: Boolean = false
) extends ClickHouseFileIndexBase(
    spark,
    deltaLog,
    path,
    table,
    snapshotAtAnalysis,
    partitionFilters,
    isTimeTravelQuery) {

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {
    getSnapshot.filesForScan(this.partitionFilters ++ partitionFilters ++ dataFilters).files
  }

  override def version: Long =
    if (isTimeTravelQuery) snapshotAtAnalysis.version else deltaLog.unsafeVolatileSnapshot.version

  override def metadata: Metadata = snapshotAtAnalysis.metadata

  override def protocol: Protocol = snapshotAtAnalysis.protocol
}
