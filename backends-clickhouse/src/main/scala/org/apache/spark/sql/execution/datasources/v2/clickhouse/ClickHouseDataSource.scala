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
package org.apache.spark.sql.execution.datasources.v2.clickhouse

import org.apache.spark.sql._
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

/** A DataSource V1 for integrating Delta into Spark SQL batch and Streaming APIs. */
class ClickHouseDataSource extends DataSourceRegister with TableProvider {

  override def shortName(): String = {
    ClickHouseConfig.NAME
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = inferSchema

  def inferSchema: StructType = new StructType() // empty

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val path = options.get("path")
    if (path == null) throw DeltaErrors.pathNotSpecifiedException
    new ClickHouseTableV2(SparkSession.active, new Path(path))
  }
}
