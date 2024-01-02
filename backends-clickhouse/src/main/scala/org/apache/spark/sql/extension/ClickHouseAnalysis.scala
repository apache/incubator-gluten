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
package org.apache.spark.sql.extension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.AnalysisHelper
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class ClickHouseAnalysis(session: SparkSession, conf: SQLConf)
  extends Rule[LogicalPlan]
  with AnalysisHelper
  with DeltaLogging {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    // This rule falls back to V1 nodes according to 'spark.gluten.sql.columnar.backend.ch.use.v2'
    case dsv2 @ DataSourceV2Relation(tableV2: ClickHouseTableV2, _, _, _, options) =>
      ClickHouseAnalysis.fromV2Relation(tableV2, dsv2, options)
  }
}

object ClickHouseAnalysis {
  def unapply(plan: LogicalPlan): Option[LogicalRelation] = plan match {
    case dsv2 @ DataSourceV2Relation(d: ClickHouseTableV2, _, _, _, options) =>
      Some(fromV2Relation(d, dsv2, options))
    case lr @ ClickHouseTable(_) => Some(lr)
    case _ => None
  }

  // convert 'DataSourceV2Relation' to 'LogicalRelation'
  def fromV2Relation(
      tableV2: ClickHouseTableV2,
      v2Relation: DataSourceV2Relation,
      options: CaseInsensitiveStringMap): LogicalRelation = {
    val relation = tableV2.withOptions(options.asScala.toMap).toBaseRelation
    val output = v2Relation.output

    val catalogTable = if (tableV2.catalogTable.isDefined) {
      Some(tableV2.v1Table)
    } else {
      None
    }
    LogicalRelation(relation, output, catalogTable, isStreaming = false)
  }
}

object ClickHouseTable {
  def unapply(a: LogicalRelation): Option[InputPartition] = a match {
    case LogicalRelation(HadoopFsRelation(index: InputPartition, _, _, _, _, _), _, _, _) =>
      Some(index)
    case _ =>
      None
  }
}
