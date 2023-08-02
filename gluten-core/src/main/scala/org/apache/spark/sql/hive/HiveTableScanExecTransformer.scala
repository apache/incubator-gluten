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
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.rel.ReadRelNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, DynamicPruningExpression, Expression}
import org.apache.spark.sql.connector.read.{InputPartition, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, DataSourceStrategy, GlutenTextBasedScanWrapper}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.HiveTableScanExecTransformer._
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.hadoop.mapred.TextInputFormat

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

class HiveTableScanExecTransformer(
    requestedAttributes: Seq[Attribute],
    relation: HiveTableRelation,
    partitionPruningPred: Seq[Expression])(session: SparkSession)
  extends HiveTableScanExec(requestedAttributes, relation, partitionPruningPred)(session)
  with BasicScanExecTransformer {

  @transient lazy val scan: Option[FileScan] = getHiveTableFileScan

  def getScan: Option[FileScan] = scan

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetrics(sparkContext)

  override def filterExprs(): Seq[Expression] = Seq.empty

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[Seq[InputPartition]] = filteredPartitions

  override def getFlattenPartitions: Seq[InputPartition] = filteredPartitions.flatten

  override def getPartitionSchemas: StructType = relation.tableMeta.partitionSchema

  override def getInputFilePaths: Seq[String] = {
    if (BackendsApiManager.isVeloxBackend) {
      Seq.empty[String]
    } else {
      val inputPaths = scan match {
        case Some(fileScan) => fileScan.fileIndex.inputFiles.toSeq
        case _ => Seq.empty
      }
      inputPaths
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

  private lazy val filteredPartitions: Seq[Seq[InputPartition]] = {
    scan match {
      case Some(fileScan) =>
        val dataSourceFilters = partitionPruningPred.flatMap {
          case DynamicPruningExpression(e) => DataSourceStrategy.translateRuntimeFilter(e)
          case _ => None
        }
        if (dataSourceFilters.nonEmpty) {
          // the cast is safe as runtime filters are only assigned if the scan can be filtered
          val filterableScan = fileScan.asInstanceOf[SupportsRuntimeFiltering]
          filterableScan.filter(dataSourceFilters.toArray)
          // call toBatch again to get filtered partitions
          val newPartitions = fileScan.toBatch.planInputPartitions()
          newPartitions.map(Seq(_))
        } else {
          fileScan.toBatch().planInputPartitions().map(Seq(_))
        }
      case _ =>
        Seq.empty
    }
  }

  private def getHiveTableFileScan: Option[FileScan] = {
    val tableMeta = relation.tableMeta
    val defaultTableSize = session.sessionState.conf.defaultSizeInBytes
    val catalogFileIndex = new CatalogFileIndex(
      session,
      tableMeta,
      tableMeta.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))
    val fileIndex = catalogFileIndex.filterPartitions(partitionPruningPred)

    val planOutput = output.asInstanceOf[Seq[AttributeReference]]
    val outputFieldTypes = new ArrayBuffer[StructField]()
    planOutput.foreach(x => outputFieldTypes.append(StructField(x.name, x.dataType, x.nullable)))

    tableMeta.storage.inputFormat match {
      case Some(inputFormat)
          if TEXT_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        tableMeta.storage.serde match {
          case Some("org.openx.data.jsonserde.JsonSerDe") | Some(
                "org.apache.hive.hcatalog.data.JsonSerDe") =>
            val scan = JsonScan(
              session,
              fileIndex,
              tableMeta.schema,
              StructType(outputFieldTypes.toArray),
              tableMeta.partitionSchema,
              new CaseInsensitiveStringMap(JavaConverters.mapAsJavaMap(tableMeta.properties)),
              Array.empty,
              partitionPruningPred,
              Seq.empty
            )
            Option.apply(GlutenTextBasedScanWrapper.wrap(scan, tableMeta.dataSchema))
          case _ =>
            val scan = SparkShimLoader.getSparkShims.getTextScan(
              session,
              fileIndex,
              tableMeta.schema,
              StructType(outputFieldTypes.toArray),
              tableMeta.partitionSchema,
              new CaseInsensitiveStringMap(JavaConverters.mapAsJavaMap(tableMeta.properties)),
              partitionPruningPred,
              Seq.empty
            )
            Some(GlutenTextBasedScanWrapper.wrap(scan, tableMeta.dataSchema))
        }
      case _ => None
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    val validationResult = super.doValidateInternal()
    if (!validationResult.isValid) {
      return validationResult
    }

    val tableMeta = relation.tableMeta
    val planOutput = output.asInstanceOf[Seq[AttributeReference]]
    var hasComplexType = false
    planOutput.foreach(
      x => {
        hasComplexType = if (!hasComplexType) {
          x.dataType.isInstanceOf[StructType] ||
          x.dataType.isInstanceOf[MapType] ||
          x.dataType.isInstanceOf[ArrayType]
        } else hasComplexType
      })

    tableMeta.storage.inputFormat match {
      case Some(inputFormat)
          if TEXT_INPUT_FORMAT_CLASS.isAssignableFrom(Utils.classForName(inputFormat)) =>
        tableMeta.storage.serde match {
          case Some("org.openx.data.jsonserde.JsonSerDe") | Some(
                "org.apache.hive.hcatalog.data.JsonSerDe") =>
            ValidationResult.ok
          case _ =>
            if (!hasComplexType) {
              ValidationResult.ok
            } else {
              ValidationResult.notOk("does not support complex type")
            }
        }
      case _ => ValidationResult.notOk("Hive scan is not defined")
    }
  }

  def createDefaultTextOption(): Map[String, String] = {
    var options: Map[String, String] = Map()
    options += ("field_delimiter" -> DEFAULT_FIELD_DELIMITER.toString)
    options += ("nullValue" -> NULL_VALUE.toString)
    options
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val transformCtx = super.doTransform(context)
    if (
      transformCtx.root != null
      && transformCtx.root.isInstanceOf[ReadRelNode]
      && scan.isDefined
      && scan.get.isInstanceOf[GlutenTextBasedScanWrapper]
    ) {
      val properties = relation.tableMeta.storage.properties ++ relation.tableMeta.properties
      var options: Map[String, String] = createDefaultTextOption()
      // property key string read from org.apache.hadoop.hive.serde.serdeConstants
      properties.foreach {
        case ("separatorChar", v) => options += ("field_delimiter" -> v)
        case ("field.delim", v) => options += ("field_delimiter" -> v)
        case ("quoteChar", v) => options += ("quote" -> v)
        case ("quote.delim", v) => options += ("quote" -> v)
        case ("skip.header.line.count", v) => options += ("header" -> v)
        case ("escapeChar", v) => options += ("escape" -> v)
        case ("escape.delim", v) => options += ("escape" -> v)
        case ("serialization.null.format", v) => options += ("nullValue" -> v)
        case (_, _) =>
      }
      val readRelNode = transformCtx.root.asInstanceOf[ReadRelNode]
      readRelNode.setDataSchema(relation.tableMeta.dataSchema)
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
}

object HiveTableScanExecTransformer {

  val NULL_VALUE: Char = 0x00
  val DEFAULT_FIELD_DELIMITER: Char = 0x01
  val TEXT_INPUT_FORMAT_CLASS: Class[TextInputFormat] =
    Utils.classForName("org.apache.hadoop.mapred.TextInputFormat")

  def isHiveTableScan(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[HiveTableScanExec]
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
