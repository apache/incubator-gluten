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
package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, LogicalPlan, TableSpec}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.execution.datasources.mergetree.StorageMeta

/** This object is responsible for adding storage information to the CreateTable. */

object AddStorageInfo extends Rule[LogicalPlan] {

  private def createMergeTreeTable(tableSpec: TableSpec): Boolean = {
    tableSpec.provider.contains(StorageMeta.Provider) ||
    tableSpec.properties
      .get(StorageMeta.DEFAULT_FILE_FORMAT)
      .contains(StorageMeta.DEFAULT_FILE_FORMAT_DEFAULT)
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(_.containsAnyPattern(COMMAND)) {
      case create @ CreateTable(ResolvedIdentifier(_, ident), _, _, tableSpec: TableSpec, _)
          if createMergeTreeTable(tableSpec) =>
        val newTableSpec = tableSpec.copy(
          properties = tableSpec.properties ++ Seq(
            StorageMeta.STORAGE_DB -> ident
              .namespace()
              .lastOption
              .getOrElse(StorageMeta.DEFAULT_CREATE_TABLE_DATABASE),
            StorageMeta.STORAGE_TABLE -> ident.name())
        )
        create.copy(tableSpec = newTableSpec)
    }
}
