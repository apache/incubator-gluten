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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.IcebergScanTransformer.{containsMetadataColumn, containsUuidOrFixedType}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.{LocalFilesNode, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType

import org.apache.iceberg.{BaseTable, MetadataColumns, SnapshotSummary}
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.spark.source.{GlutenIcebergSourceUtil, SparkTable}
import org.apache.iceberg.spark.source.metrics.NumSplits
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.{ListType, MapType, NestedField}

case class IcebergScanTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    @transient override val table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None)
  extends BatchScanExecTransformerBase(
    output = output,
    scan = scan,
    runtimeFilters = runtimeFilters,
    table = table,
    keyGroupedPartitioning = keyGroupedPartitioning,
    commonPartitionValues = commonPartitionValues
  ) {

  // PartitionReader reports the metric by currentMetricsValues,
  // but the implementation is different.
  // So use Metric to get NumSplits, NumDeletes is not reported by native metric
  private val numSplits = SQLMetrics.createMetric(sparkContext, new NumSplits().description())

  protected[this] def supportsBatchScan(scan: Scan): Boolean = {
    IcebergScanTransformer.supportsBatchScan(scan)
  }

  override def doValidateInternal(): ValidationResult = {
    val validationResult = super.doValidateInternal();
    if (!validationResult.ok()) {
      return validationResult
    }

    if (!BackendsApiManager.getSettings.supportIcebergEqualityDeleteRead()) {
      val notSupport = table match {
        case t: SparkTable =>
          t.table() match {
            case t: BaseTable =>
              t.operations()
                .current()
                .schema()
                .columns()
                .stream
                .anyMatch(c => containsUuidOrFixedType(c.`type`()) || containsMetadataColumn(c))
            case _ => false
          }
        case _ => false
      }
      if (notSupport) {
        return ValidationResult.failed("Contains not supported data type or metadata column")
      }
      // Delete from command read the _file metadata, which may be not successful.
      val readMetadata =
        scan.readSchema().fieldNames.exists(f => MetadataColumns.isMetadataColumn(f))
      if (readMetadata) {
        return ValidationResult.failed(s"Read the metadata column")
      }
      val containsEqualityDelete = table match {
        case t: SparkTable =>
          t.table() match {
            case t: BaseTable =>
              t.operations()
                .current()
                .currentSnapshot()
                .summary()
                .getOrDefault(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0")
                .toInt > 0
            case _ => false
          }
        case _ => false
      }
      if (containsEqualityDelete) {
        return ValidationResult.failed("Contains equality delete files")
      }

      if (hasRenamedColumn) {
        return ValidationResult.failed("The column is renamed, cannot read it.")
      }
    }

    ValidationResult.succeeded
  }

  override lazy val getPartitionSchema: StructType =
    GlutenIcebergSourceUtil.getReadPartitionSchema(scan)

  override def getDataSchema: StructType = new StructType()

  // TODO: get root paths from table.
  override def getRootPathsInternal: Seq[String] = Seq.empty

  override lazy val fileFormat: ReadFileFormat = GlutenIcebergSourceUtil.getFileFormat(scan)

  override def getSplitInfosWithIndex: Seq[SplitInfo] = {
    val splitInfos = getPartitionsWithIndex.zipWithIndex.map {
      case (partitions, index) =>
        GlutenIcebergSourceUtil.genSplitInfo(partitions, index, getPartitionSchema)
    }
    numSplits.add(splitInfos.map(s => s.asInstanceOf[LocalFilesNode].getPaths.size()).sum)
    splitInfos
  }

  override def getSplitInfosFromPartitions(partitions: Seq[InputPartition]): Seq[SplitInfo] = {
    val groupedPartitions = SparkShimLoader.getSparkShims
      .orderPartitions(
        this,
        scan,
        keyGroupedPartitioning,
        filteredPartitions,
        outputPartitioning,
        commonPartitionValues,
        applyPartialClustering,
        replicatePartitions)
      .flatten
    val splitInfos = groupedPartitions.zipWithIndex.map {
      case (p, index) =>
        GlutenIcebergSourceUtil.genSplitInfoForPartition(p, index, getPartitionSchema)
    }
    numSplits.add(splitInfos.map(s => s.asInstanceOf[LocalFilesNode].getPaths.size()).sum)
    splitInfos
  }

  override def doCanonicalize(): IcebergScanTransformer = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output)
    )
  }
  // Needed for tests
  private[execution] def getKeyGroupPartitioning: Option[Seq[Expression]] = keyGroupedPartitioning

  override def nodeName: String = "Iceberg" + super.nodeName

  private def hasRenamedColumn: Boolean = {
    scan
      .readSchema()
      .fieldNames
      .exists(
        name => {
          table match {
            case t: SparkTable =>
              t.table() match {
                case t: BaseTable =>
                  val id = t.operations().current().schema().findField(name).fieldId()
                  t.operations()
                    .current()
                    .schemas()
                    .stream()
                    .anyMatch(s => s.findField(id).name() != name)
                case _ => false
              }
            case _ => false
          }
        })
  }
}

object IcebergScanTransformer {
  def apply(batchScan: BatchScanExec): IcebergScanTransformer = {
    new IcebergScanTransformer(
      batchScan.output.map(a => a.withName(AvroSchemaUtil.makeCompatibleName(a.name))),
      batchScan.scan,
      batchScan.runtimeFilters,
      table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScan),
      keyGroupedPartitioning = SparkShimLoader.getSparkShims.getKeyGroupedPartitioning(batchScan),
      commonPartitionValues = SparkShimLoader.getSparkShims.getCommonPartitionValues(batchScan)
    )
  }

  def supportsBatchScan(scan: Scan): Boolean = {
    scan.getClass.getName == "org.apache.iceberg.spark.source.SparkBatchQueryScan"
  }

  private def containsUuidOrFixedType(dataType: Type): Boolean = {
    dataType match {
      case l: ListType => containsUuidOrFixedType(l.elementType)
      case m: MapType => containsUuidOrFixedType(m.keyType) || containsUuidOrFixedType(m.valueType)
      case s: org.apache.iceberg.types.Types.StructType =>
        s.fields().stream().anyMatch(f => containsUuidOrFixedType(f.`type`()))
      case t if t.typeId() == TypeID.UUID || t.typeId() == TypeID.FIXED => true
      case _ => false
    }
  }

  private def containsMetadataColumn(field: NestedField): Boolean = {
    field.`type`() match {
      case s: org.apache.iceberg.types.Types.StructType =>
        s.fields().stream().anyMatch(f => containsMetadataColumn(f))
      case _ => field.fieldId() >= (Integer.MAX_VALUE - 200)
    }
  }
}
