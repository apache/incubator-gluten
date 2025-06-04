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
package org.apache.spark.sql.execution.datasources.v2.clickhouse.source

import org.apache.spark.sql.delta.{DeltaParquetFileFormat, MergeTreeFileFormat}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}

@SuppressWarnings(Array("io.github.zhztheplayer.scalawarts.InheritFromCaseClass"))
class DeltaMergeTreeFileFormat(
    protocol: Protocol,
    metadata: Metadata,
    nullableRowTrackingFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false)
  extends DeltaParquetFileFormat(
    protocol,
    metadata,
    nullableRowTrackingFields,
    optimizationsEnabled,
    tablePath,
    isCDCRead)
  with MergeTreeFileFormat {

  override def equals(other: Any): Boolean = {
    other match {
      case ff: DeltaMergeTreeFileFormat =>
        ff.columnMappingMode == columnMappingMode &&
        ff.referenceSchema == referenceSchema &&
        ff.optimizationsEnabled == optimizationsEnabled
      case _ => false
    }
  }

  def copyWithDVInfoForMergeTree(
      tablePath: String,
      optimizationsEnabled: Boolean): DeltaMergeTreeFileFormat = {
    // When predicate pushdown is enabled we allow both splits and predicate pushdown.
    this.copyDeltaMergeTreeFileFormat(
      _optimizationsEnabled = optimizationsEnabled,
      _tablePath = Some(tablePath))
  }

  def copyDeltaMergeTreeFileFormat(
      _protocol: Protocol = protocol,
      _metadata: Metadata = metadata,
      _nullableRowTrackingFields: Boolean = nullableRowTrackingFields,
      _optimizationsEnabled: Boolean = optimizationsEnabled,
      _tablePath: Option[String] = tablePath,
      _isCDCRead: Boolean = isCDCRead): DeltaMergeTreeFileFormat =
    new DeltaMergeTreeFileFormat(
      _protocol,
      _metadata,
      _nullableRowTrackingFields,
      _optimizationsEnabled,
      _tablePath,
      _isCDCRead)

  override def hashCode(): Int = getClass.getCanonicalName.hashCode()
}
