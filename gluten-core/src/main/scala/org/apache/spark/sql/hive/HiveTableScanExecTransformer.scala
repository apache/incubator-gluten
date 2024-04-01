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
package org.apache.spark.sql.hive

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.BasicScanExecTransformer
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, Expression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.HiveTableScanExecTransformer._
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.hive.execution.{AbstractHiveTableScanExec, HiveTableScanExec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.mapred.TextInputFormat

import java.net.URI

case class HiveTableScanExecTransformer(
    requestedAttributes: Seq[Attribute],
    relation: HiveTableRelation,
    partitionPruningPred: Seq[Expression])(session: SparkSession)
  extends AbstractHiveTableScanExec(requestedAttributes, relation, partitionPruningPred)(session)
  with BasicScanExecTransformer {

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetrics(sparkContext)

  @transient private lazy val hiveQlTable = HiveClientImpl.toHiveTable(relation.tableMeta)

  @transient private lazy val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata)

  override def filterExprs(): Seq[Expression] = Seq.empty

  override def getMetadataColumns(): Seq[AttributeReference] = Seq.empty

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[InputPartition] = partitions

  override def getPartitionSchema: StructType = relation.tableMeta.partitionSchema

  override def getDataSchema: StructType = relation.tableMeta.dataSchema

  override def getInputFilePathsInternal: Seq[String] = {
    // FIXME how does a hive table expose file paths?
    Seq.empty
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetricsUpdater(metrics)

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }

  @transient private lazy val hivePartitionConverter =
    new HivePartitionConverter(session.sessionState.newHadoopConf(), session)

  @transient private lazy val partitions: Seq[InputPartition] =
    if (!relation.isPartitioned) {
      val tableLocation: URI = relation.tableMeta.storage.locationUri.getOrElse {
        throw new UnsupportedOperationException("Table path not set.")
      }
      hivePartitionConverter.createFilePartition(tableLocation)
    } else {
      hivePartitionConverter.createFilePartition(
        prunedPartitions,
        relation.partitionCols.map(_.dataType))
    }

  @transient override lazy val fileFormat: ReadFileFormat = {
    relation.tableMeta.storage.inputFormat match {
      case Some(inputFormat)
          if TEXT_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        relation.tableMeta.storage.serde match {
          case Some("org.openx.data.jsonserde.JsonSerDe") | Some(
                "org.apache.hive.hcatalog.data.JsonSerDe") =>
            ReadFileFormat.JsonReadFormat
          case _ => ReadFileFormat.TextReadFormat
        }
      case Some(inputFormat)
          if ORC_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        ReadFileFormat.OrcReadFormat
      case Some(inputFormat)
          if PARQUET_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        ReadFileFormat.ParquetReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }
  }

  private def createDefaultTextOption(): Map[String, String] = {
    var options: Map[String, String] = Map()
    relation.tableMeta.storage.serde match {
      case Some("org.apache.hadoop.hive.serde2.OpenCSVSerde") =>
        options += ("field_delimiter" -> ",")
        options += ("quote" -> "\"")
        options += ("escape" -> "\\")
      case _ =>
        options += ("field_delimiter" -> DEFAULT_FIELD_DELIMITER.toString)
        options += ("nullValue" -> NULL_VALUE.toString)
        options += ("escape" -> "\\")
    }

    options
  }

  override def getProperties: Map[String, String] = {
    var properties: Map[String, String] = Map()
    tableDesc.getProperties
      .entrySet()
      .forEach(e => properties += (e.getKey.toString -> e.getValue.toString))

    var options: Map[String, String] = createDefaultTextOption()
    // property key string read from org.apache.hadoop.hive.serde.serdeConstants
    properties.foreach {
      case ("separatorChar", v) =>
        // If separatorChar, we should use default separatorChar
        // for org.apache.hadoop.hive.serde2.OpenCSVSerde
        // It fixed issue: https://github.com/oap-project/gluten/issues/3108
        val nv = if (v.isEmpty) "," else v
        options += ("field_delimiter" -> nv)
      case ("field.delim", v) =>
        // If field.delim is empty, we should use default field delimiter
        // for org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
        // It fixed issue: https://github.com/oap-project/gluten/issues/3108
        val nv = if (v.isEmpty) DEFAULT_FIELD_DELIMITER.toString else v
        options += ("field_delimiter" -> nv)
      case ("quoteChar", v) => options += ("quote" -> v)
      case ("quote.delim", v) => options += ("quote" -> v)
      case ("skip.header.line.count", v) => options += ("header" -> v)
      case ("escapeChar", v) => options += ("escape" -> v)
      case ("escape.delim", v) => options += ("escape" -> v)
      case ("serialization.null.format", v) => options += ("nullValue" -> v)
      case (_, _) =>
    }
    options
  }

  override def nodeName: String = s"NativeScan hive ${relation.tableMeta.qualifiedName}"

  override def doCanonicalize(): HiveTableScanExecTransformer = {
    val input: AttributeSeq = relation.output
    HiveTableScanExecTransformer(
      requestedAttributes.map(QueryPlan.normalizeExpressions(_, input)),
      relation.canonicalized.asInstanceOf[HiveTableRelation],
      QueryPlan.normalizePredicates(partitionPruningPred, input)
    )(sparkSession)
  }
}

object HiveTableScanExecTransformer {

  val NULL_VALUE: Char = 0x00
  val DEFAULT_FIELD_DELIMITER: Char = 0x01
  val TEXT_INPUT_FORMAT_CLASS: Class[TextInputFormat] =
    Utils.classForName("org.apache.hadoop.mapred.TextInputFormat")
  val ORC_INPUT_FORMAT_CLASS: Class[OrcInputFormat] =
    Utils.classForName("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
  val PARQUET_INPUT_FORMAT_CLASS: Class[MapredParquetInputFormat] =
    Utils.classForName("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
  def isHiveTableScan(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[HiveTableScanExec]
  }

  def getPartitionFilters(plan: SparkPlan): Seq[Expression] = {
    plan.asInstanceOf[HiveTableScanExec].partitionPruningPred
  }

  def copyWith(plan: SparkPlan, newPartitionFilters: Seq[Expression]): SparkPlan = {
    val hiveTableScanExec = plan.asInstanceOf[HiveTableScanExec]
    hiveTableScanExec.copy(partitionPruningPred = newPartitionFilters)(sparkSession =
      hiveTableScanExec.session)
  }

  def validate(plan: SparkPlan): ValidationResult = {
    plan match {
      case hiveTableScan: HiveTableScanExec =>
        val hiveTableScanTransformer = new HiveTableScanExecTransformer(
          hiveTableScan.requestedAttributes,
          hiveTableScan.relation,
          hiveTableScan.partitionPruningPred)(hiveTableScan.session)
        hiveTableScanTransformer.doValidate()
      case _ => ValidationResult.notOk("Is not a Hive scan")
    }
  }

  def apply(plan: SparkPlan): HiveTableScanExecTransformer = {
    plan match {
      case hiveTableScan: HiveTableScanExec =>
        new HiveTableScanExecTransformer(
          hiveTableScan.requestedAttributes,
          hiveTableScan.relation,
          hiveTableScan.partitionPruningPred)(hiveTableScan.session)
      case _ =>
        throw new UnsupportedOperationException(
          s"Can't transform HiveTableScanExecTransformer from ${plan.getClass.getSimpleName}")
    }
  }
}
