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

package io.glutenproject.execution

import com.google.common.collect.Lists
import com.google.protobuf.{Any, Empty, StringValue}
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{ColumnTypeNode, TypeBuilder, TypeNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.utils.BindReferencesUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{AliasAwareOutputPartitioning, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, CommandUtils, DataWritingCommand}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.SparkConf
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.execution.datasources.{FileFormat, FileFormatWriter, FileIndex, InsertIntoHadoopFsRelationCommand, PartitioningUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.getPartitionPathString
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode

import java.util
import java.util.UUID
import scala.util.control.Breaks.{break, breakable}

case class DataWritingCommandExecTransformer(
           cmd: DataWritingCommand, child: SparkPlan) extends UnaryExecNode
  with TransformSupport
  with GlutenPlan
  with PredicateHelper
  with AliasAwareOutputPartitioning
  with Logging with Serializable {

  val sparkConf: SparkConf = sparkContext.getConf

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note
   * Right now we support up to two RDDs
   */
  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }


  def getRelNode(context: SubstraitContext,
                 cmd: DataWritingCommand,
                 originalInputAttributes: Seq[Attribute],
                 operatorId: Long,
                 input: RelNode,
                 validation: Boolean): RelNode = {
    val typeNodes = ConverterUtils.getTypeNodeFromAttributes(originalInputAttributes)
    val nameList = new java.util.ArrayList[String]()
    val columnTypeNodes = new java.util.ArrayList[ColumnTypeNode]()
    nameList.addAll(OperatorsUtils.collectAttributesNamesDFS(originalInputAttributes))

    var writePath = ""
    var isParquet = false
    var isDWRF = false
    cmd match {
      case InsertIntoHadoopFsRelationCommand(
      outputPath, _, _, partitionColumns, _, fileFormat, _, _, mode, _, _, _) =>

        fileFormat.getClass.getSimpleName match {
          case "ParquetFileFormat" =>
            isParquet = true
          case "DwrfFileFormat" =>
            isDWRF = true
          case _ =>
        }
        writePath = outputPath.toString

        for (attr <- originalInputAttributes) {
          if (partitionColumns.find(_.equals(attr)).isDefined) {
            columnTypeNodes.add(new ColumnTypeNode(1))
          } else {
            columnTypeNodes.add(new ColumnTypeNode(0))
          }
        }
      case _ =>
    }

    if (!validation) {
      RelBuilder.makeWriteRel(
        input, typeNodes, nameList, columnTypeNodes, writePath,
        OperatorsUtils.createExtensionNode(
          genFormatParametersBuilder(isParquet, isDWRF), originalInputAttributes),
        context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeWriteRel(
        input, typeNodes, nameList, columnTypeNodes, writePath,
        extensionNode, context, operatorId)
    }
  }

  def genFormatParametersBuilder(isPARQUET: Boolean,
                                 isDWRF: Boolean = false): Any.Builder = {
    // Start with "FormatParameters:"
    val formatParametersStr = new StringBuffer("WriteFormatParameters:")
    // isParquet: 0 for DWRF, 1 for Parquet
    formatParametersStr.append("isPARQUET=").append(if (isPARQUET) 1 else 0).append("\n")
      .append("isDWRF=").append(if (isDWRF) 1 else 0).append("\n")

    val message = StringValue
      .newBuilder()
      .setValue(formatParametersStr.toString)
      .build()
    Any.newBuilder
      .setValue(message.toByteString)
      .setTypeUrl("/google.protobuf.StringValue")
  }

  override def doValidateInternal(): Boolean = {
    // format only support parquet and dwrf
    if (!BackendsApiManager.getSettings.supportWriteExec() ||
      !BackendsApiManager.getSettings.supportedFileFormatWrite(SQLConf.get, cmd)) {
      return false
    }
     val substraitContext = new SubstraitContext
     val operatorId = substraitContext.nextOperatorId
     val relNode = try {
       getRelNode(
         substraitContext, cmd, child.output, operatorId, null, validation = true)
     } catch {
       case e: Throwable =>
         logValidateFailure(
           s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}", e)
         return false
     }
     // Then, validate the generated plan in native engine.
     if (relNode != null && GlutenConfig.getConf.enableNativeValidation) {
       val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
       BackendsApiManager.getValidatorApiInstance.doValidate(planNode)
     } else {
       true
     }
   }

  override def doTransform(context: SubstraitContext): TransformContext = {

    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }
    val operatorId = context.nextOperatorId

    val (currRel, inputAttributes) = if (childCtx != null) {
      (getRelNode(
        context, cmd, child.output, operatorId, childCtx.root, validation = false),
        childCtx.outputAttributes)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)
      (getRelNode(
        context, cmd, child.output, operatorId, readRel, validation = false),
        child.output)
    }
    assert(currRel != null, "Write Rel should be valid")

    val outputAttrs = BindReferencesUtil.bindReferencesWithNullable(output, inputAttributes)
    TransformContext(inputAttributes, outputAttrs, currRel)
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = child match {
    case c: TransformSupport =>
      c.getBuildPlans
    case _ =>
      Seq()
  }

  override def getStreamedLeafPlan: SparkPlan = child match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def metricsUpdater(): MetricsUpdater = {
    null
  }

  override protected def outputExpressions: Seq[NamedExpression] = output

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def output: Seq[Attribute] = cmd.output

  override protected def withNewChildInternal(
        newChild: SparkPlan): DataWritingCommandExecTransformer = {
    copy(child = newChild)
  }
}

object DataWritingCommandExecTransformer {

  def getCustomPartitionLocations(
                                   fs: FileSystem,
                                   table: CatalogTable,
                                   qualifiedOutputPath: Path,
                                   partitions: Seq[CatalogTablePartition]):
  Map[TablePartitionSpec, String] = {
    partitions.flatMap { p =>
      val defaultLocation = qualifiedOutputPath.suffix(
        "/" + PartitioningUtils.getPathFragment(p.spec, table.partitionSchema)).toString
      val catalogLocation = new Path(p.location).makeQualified(
        fs.getUri, fs.getWorkingDirectory).toString
      if (catalogLocation != defaultLocation) {
        Some(p.spec -> catalogLocation)
      } else {
        None
      }
    }.toMap
  }

  def deleteMatchingPartitions(
                                fs: FileSystem,
                                qualifiedOutputPath: Path,
                                customPartitionLocations: Map[TablePartitionSpec, String],
                                committer: FileCommitProtocol,
                                staticPartitions: TablePartitionSpec,
                                partitionColumns: Seq[Attribute]): Unit = {
    val staticPartitionPrefix = if (staticPartitions.nonEmpty) {
      "/" + partitionColumns.flatMap { p =>
        staticPartitions.get(p.name).map(getPartitionPathString(p.name, _))
      }.mkString("/")
    } else {
      ""
    }
    // first clear the path determined by the static partition keys (e.g. /table/foo=1)
    val staticPrefixPath = qualifiedOutputPath.suffix(staticPartitionPrefix)
    if (fs.exists(staticPrefixPath) && !committer.deleteWithJob(fs, staticPrefixPath, true)) {
      // throw QueryExecutionErrors.cannotClearOutputDirectoryError(staticPrefixPath)
    }
    // now clear all custom partition locations (e.g. /custom/dir/where/foo=2/bar=4)
    for ((spec, customLoc) <- customPartitionLocations) {
      assert(
        (staticPartitions.toSet -- spec).isEmpty,
        "Custom partition location did not match static partitioning keys")
      val path = new Path(customLoc)
      if (fs.exists(path) && !committer.deleteWithJob(fs, path, true)) {
        // throw QueryExecutionErrors.cannotClearPartitionDirectoryError(path)
      }
    }
  }


  def updateMetaInfo(sparkSession: SparkSession, cmd: DataWritingCommand): Unit = {
    cmd match {
      case InsertIntoHadoopFsRelationCommand(
      outputPath,
      staticPartitions,
      ifPartitionNotExists,
      partitionColumns,
      bucketSpec,
      fileFormat,
      options,
      query,
      mode,
      catalogTable,
      fileIndex,
      outputColumnNames) =>
//        // Most formats don't do well with duplicate columns, so lets not allow that
//        checkColumnNameDuplication(
//          outputColumnNames,
//          s"when inserting into $outputPath",
//          sparkSession.sessionState.conf.caseSensitiveAnalysis)

        val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
        val fs = outputPath.getFileSystem(hadoopConf)
        val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

        val partitionsTrackedByCatalog =
          sparkSession.sessionState.conf.manageFilesourcePartitions &&
          catalogTable.isDefined &&
          catalogTable.get.partitionColumnNames.nonEmpty &&
          catalogTable.get.tracksPartitionsInCatalog

        var initialMatchingPartitions: Seq[TablePartitionSpec] = Nil
        var customPartitionLocations: Map[TablePartitionSpec, String] = Map.empty
        var matchingPartitions: Seq[CatalogTablePartition] = Seq.empty

        // When partitions are tracked by the catalog, compute all custom partition locations that
        // may be relevant to the insertion job.
        if (partitionsTrackedByCatalog) {
          matchingPartitions = sparkSession.sessionState.catalog.listPartitions(
            catalogTable.get.identifier, Some(staticPartitions))
          initialMatchingPartitions = matchingPartitions.map(_.spec)
          customPartitionLocations = getCustomPartitionLocations(
            fs, catalogTable.get, qualifiedOutputPath, matchingPartitions)
        }
        lazy val parameters = CaseInsensitiveMap(options)
        lazy val dynamicPartitionOverwrite: Boolean = {
          val partitionOverwriteMode = parameters.get("partitionOverwriteMode")
            // scalastyle:off caselocale
            .map(mode => PartitionOverwriteMode.withName(mode.toUpperCase))
            // scalastyle:on caselocale
            .getOrElse(SQLConf.get.partitionOverwriteMode)
          val enableDynamicOverwrite = partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC
          // This config only makes sense when we are overwriting a partitioned dataset with dynamic
          // partition columns.
          enableDynamicOverwrite && mode == SaveMode.Overwrite &&
            staticPartitions.size < partitionColumns.length
        }

        val jobId = java.util.UUID.randomUUID().toString
        val committer = FileCommitProtocol.instantiate(
          sparkSession.sessionState.conf.fileCommitProtocolClass,
          jobId = jobId,
          outputPath = outputPath.toString,
          dynamicPartitionOverwrite = dynamicPartitionOverwrite)

        val doInsertion = if (mode == SaveMode.Append) {
          true
        } else {
          val pathExists = fs.exists(qualifiedOutputPath)
          (mode, pathExists) match {
            case (SaveMode.ErrorIfExists, true) =>
              false
              // throw QueryCompilationErrors.outputPathAlreadyExistsError(qualifiedOutputPath)
            case (SaveMode.Overwrite, true) =>
              if (ifPartitionNotExists && matchingPartitions.nonEmpty) {
                false
              } else if (dynamicPartitionOverwrite) {
                // For dynamic partition overwrite, do not delete partition directories ahead.
                true
              } else {
                deleteMatchingPartitions(
                  fs, qualifiedOutputPath,
                  customPartitionLocations, committer,
                  staticPartitions, partitionColumns)
                true
              }
            case (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
              true
            case (SaveMode.Ignore, exists) =>
              !exists
            case (s, exists) =>
              false
              // throw QueryExecutionErrors.unsupportedSaveModeError(s.toString, exists)
          }
        }

        if (doInsertion) {

          def refreshUpdatedPartitions(updatedPartitionPaths: Set[String]): Unit = {
            val updatedPartitions = updatedPartitionPaths.map(PartitioningUtils.parsePathFragment)
            if (partitionsTrackedByCatalog) {
              val newPartitions = updatedPartitions -- initialMatchingPartitions
              if (newPartitions.nonEmpty) {
                AlterTableAddPartitionCommand(
                  catalogTable.get.identifier, newPartitions.toSeq.map(p => (p, None)),
                  ifNotExists = true).run(sparkSession)
              }
              // For dynamic partition overwrite,
              // we never remove partitions but only update existing
              // ones.
              if (mode == SaveMode.Overwrite && !dynamicPartitionOverwrite) {
                val deletedPartitions = initialMatchingPartitions.toSet -- updatedPartitions
                if (deletedPartitions.nonEmpty) {
                  AlterTableDropPartitionCommand(
                    catalogTable.get.identifier, deletedPartitions.toSeq,
                    ifExists = true, purge = false,
                    retainData = true /* already deleted */).run(sparkSession)
                }
              }
            }
          }

          // For dynamic partition overwrite,
          // FileOutputCommitter's output path is staging path, files
          // will be renamed from staging path to final output path during commit job
          val committerOutputPath = if (dynamicPartitionOverwrite) {
            FileCommitProtocol.getStagingDir(outputPath.toString, jobId)
              .makeQualified(fs.getUri, fs.getWorkingDirectory)
          } else {
            qualifiedOutputPath
          }

//          val updatedPartitionPaths =
//            FileFormatWriter.write(
//              sparkSession = sparkSession,
//              plan = child,
//              fileFormat = fileFormat,
//              committer = committer,
//              outputSpec = FileFormatWriter.OutputSpec(
//                committerOutputPath.toString, customPartitionLocations, outputColumns),
//              hadoopConf = hadoopConf,
//              partitionColumns = partitionColumns,
//              bucketSpec = bucketSpec,
//              statsTrackers = Seq(basicWriteJobStatsTracker(hadoopConf)),
//              options = options)
           val updatedPartitionPaths = Set.empty[String]

          // update metastore partition metadata
          if (updatedPartitionPaths.isEmpty && staticPartitions.nonEmpty
            && partitionColumns.length == staticPartitions.size) {
            // Avoid empty static partition can't loaded to datasource table.
            val staticPathFragment =
              PartitioningUtils.getPathFragment(staticPartitions, partitionColumns)
            refreshUpdatedPartitions(Set(staticPathFragment))
          } else {
            refreshUpdatedPartitions(updatedPartitionPaths)
          }

          // refresh cached files in FileIndex
          fileIndex.foreach(_.refresh())
          // refresh data cache if table is cached
          sparkSession.sharedState.cacheManager.recacheByPath(sparkSession, outputPath, fs)

          if (catalogTable.nonEmpty) {
            CommandUtils.updateTableStats(sparkSession, catalogTable.get)
          }

        } else {
          println("Skipping insertion into a relation that already exists.")
        }

      case _ =>
    }
  }
}
