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
package org.apache.spark.sql.execution.utils

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType

import org.apache.parquet.schema.MessageType

object PushDownUtil {
  private def createParquetFilters(
      conf: SQLConf,
      schema: MessageType,
      caseSensitive: Option[Boolean] = None,
      datetimeRebaseSpec: RebaseSpec = RebaseSpec(LegacyBehaviorPolicy.CORRECTED)
  ): ParquetFilters =
    new ParquetFilters(
      schema,
      conf.parquetFilterPushDownDate,
      conf.parquetFilterPushDownTimestamp,
      conf.parquetFilterPushDownDecimal,
      conf.parquetFilterPushDownStringStartWith,
      conf.parquetFilterPushDownInFilterThreshold,
      caseSensitive.getOrElse(conf.caseSensitiveAnalysis),
      datetimeRebaseSpec
    )

  def removeNotSupportPushDownFilters(
      conf: SQLConf,
      output: Seq[Attribute],
      dataFilters: Seq[Expression]
  ): Seq[Expression] = {
    val schema = new SparkToParquetSchemaConverter(conf).convert(StructType.fromAttributes(output))
    val parquetFilters = createParquetFilters(conf, schema)

    dataFilters
      .flatMap {
        sparkFilter =>
          DataSourceStrategy.translateFilter(
            sparkFilter,
            supportNestedPredicatePushdown = true) match {
            case Some(sources.StringStartsWith(_, _)) => None
            case Some(sources.Not(sources.In(_, _) | sources.StringStartsWith(_, _))) => None
            case Some(sourceFilter) => Some((sparkFilter, sourceFilter))
            case _ => None
          }
      }
      .flatMap {
        case (sparkFilter, sourceFilter) =>
          parquetFilters.createFilter(sourceFilter) match {
            case Some(_) => Some(sparkFilter)
            case None => None
          }
      }
  }
}
