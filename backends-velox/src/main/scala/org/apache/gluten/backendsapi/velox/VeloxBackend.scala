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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.GlutenBuildInfo._
import org.apache.gluten.GlutenConfig
import org.apache.gluten.backend.Backend
import org.apache.gluten.backendsapi._
import org.apache.gluten.columnarbatch.VeloxBatch
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.expression.WindowFunctionsBuilder
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionFunc}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat.{DwrfReadFormat, OrcReadFormat, ParquetReadFormat}
import org.apache.gluten.utils._

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Alias, CumeDist, DenseRank, Descending, Expression, Lag, Lead, NamedExpression, NthValue, NTile, PercentRank, RangeFrame, Rank, RowNumber, SortOrder, SpecialFrameBoundary, SpecifiedWindowFrame}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, ApproximatePercentile, Percentile}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.execution.{ColumnarCachedBatchSerializer, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.hive.execution.HiveFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.Path

import scala.util.control.Breaks.breakable

class VeloxBackend extends SubstraitBackend {
  import VeloxBackend._
  override def name(): String = VeloxBackend.BACKEND_NAME
  override def buildInfo(): Backend.BuildInfo =
    Backend.BuildInfo("Velox", VELOX_BRANCH, VELOX_REVISION, VELOX_REVISION_TIME)
  override def convFuncOverride(): ConventionFunc.Override = new ConvFunc()
  override def iteratorApi(): IteratorApi = new VeloxIteratorApi
  override def sparkPlanExecApi(): SparkPlanExecApi = new VeloxSparkPlanExecApi
  override def transformerApi(): TransformerApi = new VeloxTransformerApi
  override def validatorApi(): ValidatorApi = new VeloxValidatorApi
  override def metricsApi(): MetricsApi = new VeloxMetricsApi
  override def listenerApi(): ListenerApi = new VeloxListenerApi
  override def ruleApi(): RuleApi = new VeloxRuleApi
  override def settings(): BackendSettingsApi = VeloxBackendSettings
}

object VeloxBackend {
  val BACKEND_NAME: String = "velox"
  val CONF_PREFIX: String = GlutenConfig.prefixOf(BACKEND_NAME)

  private class ConvFunc() extends ConventionFunc.Override {
    override def batchTypeOf: PartialFunction[SparkPlan, Convention.BatchType] = {
      case a: AdaptiveSparkPlanExec if a.supportsColumnar =>
        VeloxBatch
      case i: InMemoryTableScanExec
          if i.supportsColumnar && i.relation.cacheBuilder.serializer
            .isInstanceOf[ColumnarCachedBatchSerializer] =>
        VeloxBatch
    }
  }
}

object VeloxBackendSettings extends BackendSettingsApi {
  val SHUFFLE_SUPPORTED_CODEC = Set("lz4", "zstd")
  val GLUTEN_VELOX_UDF_LIB_PATHS = VeloxBackend.CONF_PREFIX + ".udfLibraryPaths"
  val GLUTEN_VELOX_DRIVER_UDF_LIB_PATHS = VeloxBackend.CONF_PREFIX + ".driver.udfLibraryPaths"
  val GLUTEN_VELOX_INTERNAL_UDF_LIB_PATHS = VeloxBackend.CONF_PREFIX + ".internal.udfLibraryPaths"
  val GLUTEN_VELOX_UDF_ALLOW_TYPE_CONVERSION = VeloxBackend.CONF_PREFIX + ".udfAllowTypeConversion"

  /** The columnar-batch type this backend is by default using. */
  override def primaryBatchType: Convention.BatchType = VeloxBatch

  override def validateScanExec(
      format: ReadFileFormat,
      fields: Array[StructField],
      rootPaths: Seq[String],
      properties: Map[String, String]): ValidationResult = {

    def validateScheme(): Option[String] = {
      val filteredRootPaths = distinctRootPaths(rootPaths)
      if (
        filteredRootPaths.nonEmpty && !VeloxFileSystemValidationJniWrapper
          .allSupportedByRegisteredFileSystems(filteredRootPaths.toArray)
      ) {
        Some(s"Scheme of [$filteredRootPaths] is not supported by registered file systems.")
      } else {
        None
      }
    }

    def validateFormat(): Option[String] = {
      def validateTypes(validatorFunc: PartialFunction[StructField, String]): Option[String] = {
        // Collect unsupported types.
        val unsupportedDataTypeReason = fields.collect(validatorFunc)
        if (unsupportedDataTypeReason.nonEmpty) {
          Some(
            s"Found unsupported data type in $format: ${unsupportedDataTypeReason.mkString(", ")}.")
        } else {
          None
        }
      }

      def isCharType(stringType: StringType, metadata: Metadata): Boolean = {
        val charTypePattern = "char\\((\\d+)\\)".r
        GlutenConfig.getConf.forceOrcCharTypeScanFallbackEnabled && charTypePattern
          .findFirstIn(
            CharVarcharUtils
              .getRawTypeString(metadata)
              .getOrElse(stringType.catalogString))
          .isDefined
      }

      format match {
        case ParquetReadFormat =>
          val typeValidator: PartialFunction[StructField, String] = {
            // Parquet timestamp is not fully supported yet
            case StructField(_, TimestampType, _, _)
                if GlutenConfig.getConf.forceParquetTimestampTypeScanFallbackEnabled =>
              "TimestampType(force fallback)"
          }
          val parquetOptions = new ParquetOptions(CaseInsensitiveMap(properties), SQLConf.get)
          if (parquetOptions.mergeSchema) {
            // https://github.com/apache/incubator-gluten/issues/7174
            Some(s"not support when merge schema is true")
          } else {
            validateTypes(typeValidator)
          }
        case DwrfReadFormat => None
        case OrcReadFormat =>
          if (!GlutenConfig.getConf.veloxOrcScanEnabled) {
            Some(s"Velox ORC scan is turned off, ${GlutenConfig.VELOX_ORC_SCAN_ENABLED.key}")
          } else {
            val typeValidator: PartialFunction[StructField, String] = {
              case StructField(_, arrayType: ArrayType, _, _)
                  if arrayType.elementType.isInstanceOf[StructType] =>
                "StructType as element in ArrayType"
              case StructField(_, arrayType: ArrayType, _, _)
                  if arrayType.elementType.isInstanceOf[ArrayType] =>
                "ArrayType as element in ArrayType"
              case StructField(_, mapType: MapType, _, _)
                  if mapType.keyType.isInstanceOf[StructType] =>
                "StructType as Key in MapType"
              case StructField(_, mapType: MapType, _, _)
                  if mapType.valueType.isInstanceOf[ArrayType] =>
                "ArrayType as Value in MapType"
              case StructField(_, stringType: StringType, _, metadata)
                  if isCharType(stringType, metadata) =>
                CharVarcharUtils.getRawTypeString(metadata) + "(force fallback)"
              case StructField(_, TimestampType, _, _) => "TimestampType"
            }
            validateTypes(typeValidator)
          }
        case _ => Some(s"Unsupported file format for $format.")
      }
    }

    validateScheme().orElse(validateFormat()) match {
      case Some(reason) => ValidationResult.failed(reason)
      case _ => ValidationResult.succeeded
    }
  }

  def distinctRootPaths(paths: Seq[String]): Seq[String] = {
    // Skip native validation for local path, as local file system is always registered.
    // For evey file scheme, only one path is kept.
    paths
      .map(p => (new Path(p).toUri.getScheme, p))
      .groupBy(_._1)
      .filter(_._1 != "file")
      .map(_._2.head._2)
      .toSeq
  }

  override def supportWriteFilesExec(
      format: FileFormat,
      fields: Array[StructField],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String]): ValidationResult = {

    // Validate if HiveFileFormat write is supported based on output file type
    def validateHiveFileFormat(hiveFileFormat: HiveFileFormat): Option[String] = {
      // Reflect to get access to fileSinkConf which contains the output file format
      val fileSinkConfField = format.getClass.getDeclaredField("fileSinkConf")
      fileSinkConfField.setAccessible(true)
      val fileSinkConf = fileSinkConfField.get(hiveFileFormat)
      val tableInfoField = fileSinkConf.getClass.getDeclaredField("tableInfo")
      tableInfoField.setAccessible(true)
      val tableInfo = tableInfoField.get(fileSinkConf)
      val getOutputFileFormatClassNameMethod = tableInfo.getClass
        .getDeclaredMethod("getOutputFileFormatClassName")
      val outputFileFormatClassName = getOutputFileFormatClassNameMethod.invoke(tableInfo)

      // Match based on the output file format class name
      outputFileFormatClassName match {
        case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat" =>
          None
        case _ =>
          Some(
            "HiveFileFormat is supported only with Parquet as the output file type"
          ) // Unsupported format
      }
    }

    def validateCompressionCodec(): Option[String] = {
      // Velox doesn't support brotli and lzo.
      val unSupportedCompressions = Set("brotli", "lzo", "lz4raw", "lz4_raw")
      val compressionCodec = WriteFilesExecTransformer.getCompressionCodec(options)
      if (unSupportedCompressions.contains(compressionCodec)) {
        Some("Brotli, lzo, lz4raw and lz4_raw compression codec is unsupported in Velox backend.")
      } else {
        None
      }
    }

    // Validate if all types are supported.
    def validateDataTypes(): Option[String] = {
      val unsupportedTypes = format match {
        case _: ParquetFileFormat =>
          fields.flatMap {
            case StructField(_, _: YearMonthIntervalType, _, _) =>
              Some("YearMonthIntervalType")
            case StructField(_, _: StructType, _, _) =>
              Some("StructType")
            case _ => None
          }
        case _ =>
          fields.flatMap {
            field =>
              field.dataType match {
                case _: StructType => Some("StructType")
                case _: ArrayType => Some("ArrayType")
                case _: MapType => Some("MapType")
                case _: YearMonthIntervalType => Some("YearMonthIntervalType")
                case _ => None
              }
          }
      }
      if (unsupportedTypes.nonEmpty) {
        Some(unsupportedTypes.mkString("Found unsupported type:", ",", ""))
      } else {
        None
      }
    }

    def validateFieldMetadata(): Option[String] = {
      fields.find(_.metadata != Metadata.empty).map {
        filed =>
          s"StructField contain the metadata information: $filed, metadata: ${filed.metadata}"
      }
    }

    def validateFileFormat(): Option[String] = {
      format match {
        case _: ParquetFileFormat => None // Parquet is directly supported
        case h: HiveFileFormat if GlutenConfig.getConf.enableHiveFileFormatWriter =>
          validateHiveFileFormat(h) // Parquet via Hive SerDe
        case _ =>
          Some(
            "Only ParquetFileFormat and HiveFileFormat are supported."
          ) // Unsupported format
      }
    }

    def validateWriteFilesOptions(): Option[String] = {
      val maxRecordsPerFile = options
        .get("maxRecordsPerFile")
        .map(_.toLong)
        .getOrElse(SQLConf.get.maxRecordsPerFile)
      if (maxRecordsPerFile > 0) {
        Some("Unsupported native write: maxRecordsPerFile not supported.")
      } else {
        None
      }
    }

    def validateBucketSpec(): Option[String] = {
      if (bucketSpec.nonEmpty) {
        Some("Unsupported native write: bucket write is not supported.")
      } else {
        None
      }
    }

    validateCompressionCodec()
      .orElse(validateFileFormat())
      .orElse(validateFieldMetadata())
      .orElse(validateDataTypes())
      .orElse(validateWriteFilesOptions())
      .orElse(validateBucketSpec()) match {
      case Some(reason) => ValidationResult.failed(reason)
      case _ => ValidationResult.succeeded
    }
  }

  override def supportNativeWrite(fields: Array[StructField]): Boolean = {
    fields.map {
      field =>
        field.dataType match {
          case _: StructType | _: ArrayType | _: MapType => return false
          case _ =>
        }
    }
    true
  }

  override def supportNativeMetadataColumns(): Boolean = true

  override def supportNativeRowIndexColumn(): Boolean = true

  override def supportExpandExec(): Boolean = true

  override def supportSortExec(): Boolean = true

  override def supportSortMergeJoinExec(): Boolean = {
    GlutenConfig.getConf.enableColumnarSortMergeJoin
  }

  override def supportWindowGroupLimitExec(rankLikeFunction: Expression): Boolean = {
    rankLikeFunction match {
      case _: RowNumber => true
      case _ => false
    }
  }

  override def supportWindowExec(windowFunctions: Seq[NamedExpression]): Boolean = {
    var allSupported = true
    breakable {
      windowFunctions.foreach(
        func => {
          val windowExpression = func match {
            case alias: Alias =>
              val we = WindowFunctionsBuilder.extractWindowExpression(alias.child)
              if (we == null) {
                throw new GlutenNotSupportException(s"$func is not supported.")
              }
              we
            case _ => throw new GlutenNotSupportException(s"$func is not supported.")
          }

          def checkLimitations(swf: SpecifiedWindowFrame, orderSpec: Seq[SortOrder]): Unit = {
            def doCheck(bound: Expression): Unit = {
              bound match {
                case _: SpecialFrameBoundary =>
                case e if e.foldable =>
                  orderSpec.foreach(
                    order =>
                      order.direction match {
                        case Descending =>
                          throw new GlutenNotSupportException(
                            "DESC order is not supported when" +
                              " literal bound type is used!")
                        case _ =>
                      })
                  orderSpec.foreach(
                    order =>
                      order.dataType match {
                        case ByteType | ShortType | IntegerType | LongType | DateType =>
                        case _ =>
                          throw new GlutenNotSupportException(
                            "Only integral type & date type are" +
                              " supported for sort key when literal bound type is used!")
                      })
                case _ =>
              }
            }
            doCheck(swf.upper)
            doCheck(swf.lower)
          }

          windowExpression.windowSpec.frameSpecification match {
            case swf: SpecifiedWindowFrame =>
              swf.frameType match {
                case RangeFrame =>
                  checkLimitations(swf, windowExpression.windowSpec.orderSpec)
                case _ =>
              }
            case _ =>
          }
          windowExpression.windowFunction match {
            case _: RowNumber | _: Rank | _: CumeDist | _: DenseRank | _: PercentRank |
                _: NthValue | _: NTile | _: Lag | _: Lead =>
            case aggrExpr: AggregateExpression
                if !aggrExpr.aggregateFunction.isInstanceOf[ApproximatePercentile]
                  && !aggrExpr.aggregateFunction.isInstanceOf[Percentile] =>
            case _ =>
              allSupported = false
          }
        })
    }
    allSupported
  }

  override def supportColumnarShuffleExec(): Boolean = {
    GlutenConfig.getConf.enableColumnarShuffle && (GlutenConfig.getConf.isUseColumnarShuffleManager
      || GlutenConfig.getConf.isUseCelebornShuffleManager
      || GlutenConfig.getConf.isUseUniffleShuffleManager)
  }

  override def enableJoinKeysRewrite(): Boolean = false

  override def supportHashBuildJoinTypeOnLeft: JoinType => Boolean = {
    t =>
      if (super.supportHashBuildJoinTypeOnLeft(t)) {
        true
      } else {
        t match {
          // OPPRO-266: For Velox backend, build right and left are both supported for
          // LeftOuter.
          // TODO: Support LeftSemi after resolve issue
          // https://github.com/facebookincubator/velox/issues/9980
          case LeftOuter => true
          case _ => false
        }
      }
  }
  override def supportHashBuildJoinTypeOnRight: JoinType => Boolean = {
    t =>
      if (super.supportHashBuildJoinTypeOnRight(t)) {
        true
      } else {
        t match {
          // OPPRO-266: For Velox backend, build right and left are both supported for RightOuter.
          case RightOuter => true
          case _ => false
        }
      }
  }

  override def fallbackAggregateWithEmptyOutputChild(): Boolean = true

  override def recreateJoinExecOnFallback(): Boolean = true
  override def rescaleDecimalArithmetic(): Boolean = true

  override def shuffleSupportedCodec(): Set[String] = SHUFFLE_SUPPORTED_CODEC

  override def insertPostProjectForGenerate(): Boolean = true

  override def skipNativeCtas(ctas: CreateDataSourceTableAsSelectCommand): Boolean = true

  override def skipNativeInsertInto(insertInto: InsertIntoHadoopFsRelationCommand): Boolean = {
    insertInto.partitionColumns.nonEmpty &&
    insertInto.staticPartitions.size < insertInto.partitionColumns.size ||
    insertInto.bucketSpec.nonEmpty
  }

  override def alwaysFailOnMapExpression(): Boolean = true

  override def requiredChildOrderingForWindow(): Boolean = {
    GlutenConfig.getConf.veloxColumnarWindowType.equals("streaming")
  }

  override def requiredChildOrderingForWindowGroupLimit(): Boolean = false

  override def staticPartitionWriteOnly(): Boolean = true

  override def allowDecimalArithmetic: Boolean = true

  override def enableNativeWriteFiles(): Boolean = {
    GlutenConfig.getConf.enableNativeWriter.getOrElse(
      SparkShimLoader.getSparkShims.enableNativeWriteFilesByDefault()
    )
  }

  override def enableNativeArrowReadFiles(): Boolean = {
    GlutenConfig.getConf.enableNativeArrowReader
  }

  override def shouldRewriteCount(): Boolean = {
    // Velox backend does not support count if it has more that one child,
    // so we should rewrite it.
    true
  }

  override def supportCartesianProductExec(): Boolean = true

  override def supportBroadcastNestedLoopJoinExec(): Boolean = true

  override def supportSampleExec(): Boolean = true

  override def supportColumnarArrowUdf(): Boolean = true

  override def needPreComputeRangeFrameBoundary(): Boolean = true
}
