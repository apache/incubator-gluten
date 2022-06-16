/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2.clickhouse.source

import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2

class ClickHouseWriteBuilder(spark: SparkSession,
                             table: ClickHouseTableV2,
                             deltaLog: DeltaLog,
                             info: LogicalWriteInfo) extends WriteBuilder {

  def querySchema = info.schema()

  def queryId = info.queryId()

  private val options = info.options()

  override def buildForBatch(): BatchWrite = {
    new ClickHouseBatchWrite(info)
  }
}
