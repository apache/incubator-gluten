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
package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.Identifier

object CommandUtils {
  // Ensure ClickHouseTableV2 table exists
  def ensureClickHouseTableV2(
      tableId: Option[TableIdentifier],
      sparkSession: SparkSession): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    if (tableId.isEmpty) {
      throw new UnsupportedOperationException("Current command requires table identifier.")
    }
    // If user comes into this function without previously triggering loadTable
    // (which creates ClickhouseTableV2), we have to load the table manually
    // Notice: Multi-catalog case is not well considered!
    sparkSession.sessionState.catalogManager.currentCatalog.asTableCatalog.loadTable(
      Identifier.of(
        Array(
          tableId.get.database.getOrElse(
            sparkSession.sessionState.catalogManager.currentNamespace.head)),
        tableId.get.table)
    )
  }
}
