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

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2._

case class CHDataSourceV2Strategy(spark: SparkSession) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // TODO: remove this strategy first
    /* case CreateV2Table(catalog, ident, schema, parts, props, ifNotExists)
        if catalog.isInstanceOf[ClickHouseSparkCatalog] =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      CreateTableExec(catalog, ident, schema, parts, propsWithOwner, ifNotExists) :: Nil

    case CreateTableAsSelect(catalog, ident, parts, query, props, options, ifNotExists)
        if catalog.isInstanceOf[ClickHouseSparkCatalog] =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          // AtomicCreateTableAsSelectExec(staging, ident, parts, query, planLater(query),
          //  propsWithOwner, writeOptions, ifNotExists) :: Nil
          Nil
        case _ =>
          CreateTableAsSelectExec(
            catalog,
            ident,
            parts,
            query,
            planLater(query),
            propsWithOwner,
            writeOptions,
            ifNotExists) :: Nil
      }

    case AppendData(r: DataSourceV2Relation, query, writeOptions, _, Some(write))
        if r.table.isInstanceOf[ClickHouseTableV2] =>
      r.table.asWritable match {
        case v2 =>
          ClickHouseAppendDataExec(
            v2,
            writeOptions.asOptions,
            planLater(query),
            write,
            refreshCache(r)) :: Nil
      }
     */
    case _ => Nil
  }

  private def refreshCache(r: DataSourceV2Relation)(): Unit = {
    spark.sharedState.cacheManager.recacheByPlan(spark, r)
  }
}
