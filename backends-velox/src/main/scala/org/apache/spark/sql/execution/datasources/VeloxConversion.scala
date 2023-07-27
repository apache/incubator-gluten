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
package org.apache.spark.sql.execution.datasources

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.velox.Validator

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.velox.VeloxParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types.{DataType, DateType, Metadata, StructType, TimestampType}

/**
 * This rule converts vanilla Spark datasource write to velox write, including:
 * [[InsertIntoHadoopFsRelationCommand]], [[InsertIntoDataSourceDirCommand]]
 *
 * Note that, we do not support write partitioned table and bucketed table using velox. More details
 * see [[GlutenColumnarRules]].
 */
case class VeloxConversion(session: SparkSession) extends Rule[LogicalPlan] {
  private val emptyMetadataHashCode = Metadata.empty.hashCode()

  private def support(options: Map[String, String], schema: StructType): Boolean = {
    supportConf(options) && supportSchema(schema)
  }

  private def supportConf(options: Map[String, String]): Boolean = {
    !options.contains("maxRecordsPerFile") && conf.maxRecordsPerFile == 0
  }

  private def supportDataType(dt: DataType): Boolean = {
    val supported = dt match {
      case _: TimestampType =>
        // The default behavior of Spark is write INT96 to parquet.
        // It seems velox does not support read INT64 with TIMESTAMP_MICROS time unit.
        // todo, enable this after velox support write timestamp as int96
        // conf.getConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE) == "INT96" &&
        // conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE) !=
        //   LegacyBehaviorPolicy.LEGACY.toString &&
        // conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE) !=
        //   LegacyBehaviorPolicy.LEGACY.toString
        false
      case _: DateType =>
        conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE) != LegacyBehaviorPolicy.LEGACY.toString
      case _ => true
    }
    supported && new Validator().doSchemaValidate(dt)
  }

  private def supportSchema(schema: StructType): Boolean = {
    schema.fields.forall {
      field =>
        supportDataType(field.dataType) &&
        // TODO, do not support write metadata to parquet
        field.metadata.hashCode() == emptyMetadataHashCode
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      !conf.getConf(GlutenConfig.GLUTEN_ENABLED) ||
      !conf.getConf(GlutenConfig.COLUMNAR_PARQUET_WRITE_ENABLED)
    ) {
      return plan
    }

    plan.resolveOperators {
      case i: InsertIntoHadoopFsRelationCommand
          if i.fileFormat.isInstanceOf[ParquetFileFormat] &&
            i.bucketSpec.isEmpty && i.partitionColumns.isEmpty &&
            support(i.options, StructType.fromAttributes(i.outputColumns)) =>
        i.copy(fileFormat = new VeloxParquetFileFormat())

      case i: InsertIntoDataSourceDirCommand
          if i.provider == "parquet" &&
            support(Map.empty, i.query.schema) =>
        i.copy(provider = "velox")
    }
  }
}
