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
package io.glutenproject.extension

import io.glutenproject.execution.{FileSourceScanExecTransformer, ProjectExecTransformer}
import io.glutenproject.extension.DeltaRewriteTransformerRules.columnMappingRule
import io.glutenproject.extension.columnar.TransformHints

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.{DeltaParquetFileFormat, NoMapping}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection._

class DeltaRewriteTransformerRules extends RewriteTransformerRules {
  override def rules: Seq[Rule[SparkPlan]] = columnMappingRule :: Nil
}

object DeltaRewriteTransformerRules {

  private val COLUMN_MAPPING_RULE_TAG: TreeNodeTag[String] =
    TreeNodeTag[String]("io.glutenproject.delta.column.mapping")

  private def notAppliedColumnMappingRule(plan: SparkPlan): Boolean = {
    plan.getTagValue(COLUMN_MAPPING_RULE_TAG).isEmpty
  }

  private def tagColumnMappingRule(plan: SparkPlan): Unit = {
    plan.setTagValue(COLUMN_MAPPING_RULE_TAG, null)
  }

  val columnMappingRule: Rule[SparkPlan] = (plan: SparkPlan) =>
    plan.transformWithSubqueries {
      // If it enables Delta Column Mapping(e.g. nameMapping and idMapping),
      // transform the metadata of Delta into Parquet's,
      // so that gluten can read Delta File using Parquet Reader.
      case p: FileSourceScanExecTransformer
          if isDeltaColumnMappingFileFormat(p.relation.fileFormat) && notAppliedColumnMappingRule(
            p) =>
        transformColumnMappingPlan(p)
    }

  private def isDeltaColumnMappingFileFormat(fileFormat: FileFormat): Boolean = fileFormat match {
    case d: DeltaParquetFileFormat if d.columnMappingMode != NoMapping =>
      true
    case _ =>
      false
  }

  /**
   * This method is only used for Delta ColumnMapping FileFormat(e.g. nameMapping and idMapping)
   * transform the metadata of Delta into Parquet's, each plan should only be transformed once.
   */
  private def transformColumnMappingPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: FileSourceScanExecTransformer =>
      val fmt = plan.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
      // a mapping between the table schemas name to parquet schemas.
      val columnNameMapping = mutable.Map.empty[String, String]
      fmt.referenceSchema.foreach {
        f =>
          val pName = f.metadata.getString("delta.columnMapping.physicalName")
          val lName = f.name
          columnNameMapping += (lName -> pName)
      }

      // transform HadoopFsRelation
      val relation = plan.relation
      val newDataFields = relation.dataSchema.map(e => e.copy(columnNameMapping(e.name)))
      val newPartitionFields = relation.partitionSchema.map {
        e => e.copy(columnNameMapping(e.name))
      }
      val newFsRelation = relation.copy(
        partitionSchema = StructType(newPartitionFields),
        dataSchema = StructType(newDataFields)
      )(SparkSession.active)

      // transform output's name into physical name so Reader can read data correctly
      // should keep the columns order the same as the origin output
      val originColumnNames = mutable.ListBuffer.empty[String]
      val transformedAttrs = mutable.ListBuffer.empty[Attribute]
      val newOutput = plan.output.map {
        o =>
          val newAttr = o.withName(columnNameMapping(o.name))
          if (!originColumnNames.contains(o.name)) {
            transformedAttrs += newAttr
            originColumnNames += o.name
          }
          newAttr
      }
      // transform dataFilters
      val newDataFilters = plan.dataFilters.map {
        e =>
          e.transformDown {
            case attr: AttributeReference =>
              val newAttr = attr.withName(columnNameMapping(attr.name)).toAttribute
              if (!originColumnNames.contains(attr.name)) {
                transformedAttrs += newAttr
                originColumnNames += attr.name
              }
              newAttr
          }
      }
      // transform partitionFilters
      val newPartitionFilters = plan.partitionFilters.map {
        e =>
          e.transformDown {
            case attr: AttributeReference =>
              val newAttr = attr.withName(columnNameMapping(attr.name)).toAttribute
              if (!originColumnNames.contains(attr.name)) {
                transformedAttrs += newAttr
                originColumnNames += attr.name
              }
              newAttr
          }
      }
      // replace tableName in schema with physicalName
      val newRequiredFields = plan.requiredSchema.map {
        e => StructField(columnNameMapping(e.name), e.dataType, e.nullable, e.metadata)
      }
      val scanExecTransformer = new FileSourceScanExecTransformer(
        newFsRelation,
        newOutput,
        StructType(newRequiredFields),
        newPartitionFilters,
        plan.optionalBucketSet,
        plan.optionalNumCoalescedBuckets,
        newDataFilters,
        plan.tableIdentifier,
        plan.disableBucketedScan
      )
      scanExecTransformer.copyTagsFrom(plan)
      tagColumnMappingRule(scanExecTransformer)
      TransformHints.tagTransformable(scanExecTransformer)

      // alias physicalName into tableName
      val expr = (transformedAttrs, originColumnNames).zipped.map {
        (attr, columnName) => Alias(attr, columnName)(exprId = attr.exprId)
      }
      val projectExecTransformer = ProjectExecTransformer(expr, scanExecTransformer)
      TransformHints.tagTransformable(projectExecTransformer)
      projectExecTransformer
    case _ => plan
  }
}
