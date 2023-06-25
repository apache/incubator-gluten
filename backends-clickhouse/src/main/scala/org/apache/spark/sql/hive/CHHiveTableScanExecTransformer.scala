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
import io.glutenproject.execution.{HiveTableScanExecTransformer, TransformContext}
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
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

class CHHiveTableScanExecTransformer(
    requestedAttributes: Seq[Attribute],
    relation: HiveTableRelation,
    partitionPruningPred: Seq[Expression])(session: SparkSession)
  extends HiveTableScanExec(requestedAttributes, relation, partitionPruningPred)(session)
  with HiveTableScanExecTransformer {

  @transient lazy val scan: Option[FileScan] = getHiveTableFileScan

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetrics(sparkContext)

  override def filterExprs(): Seq[Expression] = Seq.empty

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[Seq[InputPartition]] = filteredPartitions

  override def getFlattenPartitions: Seq[InputPartition] = filteredPartitions.flatten

  override def getPartitionSchemas: StructType = relation.tableMeta.partitionSchema

  override def getScan: Option[FileScan] = scan

  override def getInputFilePaths: Seq[String] = {
    val inputPaths = scan match {
      case fileScan: FileScan => fileScan.fileIndex.inputFiles.toSeq
      case _ => Seq.empty
    }
    inputPaths
  }

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note
   *   Right now we support up to two RDDs
   */
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
    BackendsApiManager.getMetricsApiInstance.genBatchScanTransformerMetricsUpdater(metrics)

  override def supportsColumnar(): Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }

  private val filteredPartitions: Seq[Seq[InputPartition]] = {
    scan match {
      case Some(f) =>
        val dataSourceFilters = partitionPruningPred.flatMap {
          case DynamicPruningExpression(e) => DataSourceStrategy.translateRuntimeFilter(e)
          case _ => None
        }
        if (dataSourceFilters.nonEmpty) {
          // the cast is safe as runtime filters are only assigned if the scan can be filtered
          val filterableScan = f.asInstanceOf[SupportsRuntimeFiltering]
          filterableScan.filter(dataSourceFilters.toArray)
          // call toBatch again to get filtered partitions
          val newPartitions = f.toBatch.planInputPartitions()
          newPartitions.map(Seq(_))
        } else {
          f.toBatch().planInputPartitions().map(Seq(_))
        }
      case _ =>
        Seq.empty
    }
  }

  private def getHiveTableFileScan: Option[FileScan] = {
    val tableMeta = relation.tableMeta
    val fileIndex = new InMemoryFileIndex(
      session,
      Seq.apply(new Path(tableMeta.location)),
      Map.empty,
      Option.apply(tableMeta.schema))
    val planOutput = output.asInstanceOf[Seq[AttributeReference]]
    var hasComplexType = false
    val outputFieldTypes = new ArrayBuffer[StructField]()
    planOutput.foreach(
      x => {
        hasComplexType = if (!hasComplexType) {
          x.dataType.isInstanceOf[StructType] ||
          x.dataType.isInstanceOf[MapType] ||
          x.dataType.isInstanceOf[ArrayType]
        } else hasComplexType
        outputFieldTypes.append(StructField(x.name, x.dataType))
      })

    // scalastyle:on println
    tableMeta.storage.inputFormat match {
      case Some("org.apache.hadoop.mapred.TextInputFormat") =>
        tableMeta.storage.serde match {
          case Some("org.openx.data.jsonserde.JsonSerDe") =>
            Option.apply(
              JsonScan(
                session,
                fileIndex,
                tableMeta.schema,
                StructType(outputFieldTypes.toArray),
                tableMeta.partitionSchema,
                new CaseInsensitiveStringMap(JavaConverters.mapAsJavaMap(tableMeta.properties)),
                Array.empty,
                partitionPruningPred,
                Seq.empty
              ))
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
            if (!hasComplexType) {
              Option.apply(scan)
            } else {
              Option.empty
            }
        }
      case _ => Option.empty
    }
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val transformCtx = super.doTransform(context)
    if (
      transformCtx.root != null
      && transformCtx.root.isInstanceOf[ReadRelNode]
      && scan.isDefined && scan.get.isInstanceOf[TextScan]
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

      if (!options.contains("field_delimiter")) {
        val defaultDelimiter: Char = 0x01
        options += ("field_delimiter" -> defaultDelimiter.toString)
      }

      val readRelNode = transformCtx.root.asInstanceOf[ReadRelNode]
      readRelNode.setDataSchema(relation.tableMeta.dataSchema)
      readRelNode.setProperties(JavaConverters.mapAsJavaMap(options))
    }
    transformCtx
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[CHHiveTableScanExecTransformer]

  override def equals(other: Any): Boolean = other match {
    case that: CHHiveTableScanExecTransformer =>
      that.canEqual(this) &&
      scan == that.scan &&
      metrics == that.metrics &&
      filteredPartitions == that.filteredPartitions
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), scan, metrics, filteredPartitions)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def createDefaultTextOption(): Map[String, String] = {
    var options: Map[String, String] = Map()

    val defaultDelimiter: Char = 0x01
    options += ("field_delimiter" -> defaultDelimiter.toString)

    val nullValue: Char = 0x00
    options += ("nullValue" -> nullValue.toString)
    options
  }
}

object CHHiveTableScanExecTransformer {

  def transform(plan: SparkPlan): Option[HiveTableScanExecTransformer] = {
    if (!plan.isInstanceOf[HiveTableScanExec]) {
      return Option.empty
    }
    val hiveTableScan = plan.asInstanceOf[HiveTableScanExec]
    val hiveTableScanTransformer = new CHHiveTableScanExecTransformer(
      hiveTableScan.requestedAttributes,
      hiveTableScan.relation,
      hiveTableScan.partitionPruningPred)(hiveTableScan.session)
    if (hiveTableScanTransformer.scan == null) {
      Option.empty
    } else {
      Option.apply(hiveTableScanTransformer)
    }
  }
}
