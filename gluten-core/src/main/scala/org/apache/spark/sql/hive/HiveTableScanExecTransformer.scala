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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.{BasicScanExecTransformer, TransformContext}
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.substrait.rel.ReadRelNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.HiveTableScanExecTransformer._
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat
import org.apache.hadoop.mapred.TextInputFormat

import java.net.URI

import scala.collection.JavaConverters

class HiveTableScanExecTransformer(
    requestedAttributes: Seq[Attribute],
    relation: HiveTableRelation,
    partitionPruningPred: Seq[Expression])(session: SparkSession)
  extends HiveTableScanExec(requestedAttributes, relation, partitionPruningPred)(session)
  with BasicScanExecTransformer {

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetrics(sparkContext)

  override def filterExprs(): Seq[Expression] = Seq.empty

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[InputPartition] = partitions

  override def getPartitionSchemas: StructType = relation.tableMeta.partitionSchema

  override def getDataSchemas: StructType = relation.tableMeta.dataSchema

  override def getInputFilePaths: Seq[String] = {
    if (BackendsApiManager.isVeloxBackend) {
      Seq.empty[String]
    } else if (BackendsApiManager.isCHBackend) {
      // We only support reading from text file format, so we can safely return empty.
      // see CHBackendSettings#supportFileFormatRead
      Seq.empty[String]
    } else {
      throw new UnsupportedOperationException("Unsupported backend")
    }
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    Seq.empty
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = {
    this
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetricsUpdater(metrics)

  override def supportsColumnar(): Boolean = GlutenConfig.getConf.enableColumnarIterator

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
    }

    options
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val transformCtx = super.doTransform(context)
    if (
      transformCtx.root != null
      && transformCtx.root.isInstanceOf[ReadRelNode]
    ) {
      val properties = relation.tableMeta.storage.properties ++ relation.tableMeta.properties
      var options: Map[String, String] = createDefaultTextOption()
      // property key string read from org.apache.hadoop.hive.serde.serdeConstants
      properties.foreach {
        case ("separatorChar", v) =>
          // If separatorChar, we should use default separatorChar
          // for org.apache.hadoop.hive.serde2.OpenCSVSerde
          // It ifx issue: https://github.com/oap-project/gluten/issues/3108
          var nv = if (v.isEmpty()) "," else v
          options += ("field_delimiter" -> nv)
        case ("field.delim", v) =>
          // If field.delim is empty, we should use default field delimiter
          // for org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
          // It fixed issue: https://github.com/oap-project/gluten/issues/3108
          var nv = if (v.isEmpty) DEFAULT_FIELD_DELIMITER.toString else v
          options += ("field_delimiter" -> nv)
        case ("quoteChar", v) => options += ("quote" -> v)
        case ("quote.delim", v) => options += ("quote" -> v)
        case ("skip.header.line.count", v) => options += ("header" -> v)
        case ("escapeChar", v) => options += ("escape" -> v)
        case ("escape.delim", v) => options += ("escape" -> v)
        case ("serialization.null.format", v) => options += ("nullValue" -> v)
        case (_, _) =>
      }
      val readRelNode = transformCtx.root.asInstanceOf[ReadRelNode]
      readRelNode.setDataSchema(getDataSchemas)
      readRelNode.setProperties(JavaConverters.mapAsJavaMap(options))
    }
    transformCtx
  }

  override def nodeName: String = s"NativeScan hive ${relation.tableMeta.qualifiedName}"

  override def canEqual(other: Any): Boolean = other.isInstanceOf[HiveTableScanExecTransformer]

  override def equals(other: Any): Boolean = other match {
    case that: HiveTableScanExecTransformer =>
      that.canEqual(this) && super.equals(that)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def doCanonicalize(): HiveTableScanExecTransformer = {
    val canonicalized = super.doCanonicalize()
    new HiveTableScanExecTransformer(
      canonicalized.requestedAttributes,
      canonicalized.relation,
      canonicalized.partitionPruningPred)(canonicalized.session)
  }
}

object HiveTableScanExecTransformer {

  val NULL_VALUE: Char = 0x00
  val DEFAULT_FIELD_DELIMITER: Char = 0x01
  val TEXT_INPUT_FORMAT_CLASS: Class[TextInputFormat] =
    Utils.classForName("org.apache.hadoop.mapred.TextInputFormat")
  val ORC_INPUT_FORMAT_CLASS: Class[OrcInputFormat] =
    Utils.classForName("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")

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
