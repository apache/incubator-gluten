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

import org.apache.gluten.{GlutenConfig, VELOX_BRANCH, VELOX_REVISION, VELOX_REVISION_TIME}
import org.apache.gluten.backendsapi._
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.expression.WindowFunctionsBuilder
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat.{DwrfReadFormat, OrcReadFormat, ParquetReadFormat}
import org.apache.gluten.utils._

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Alias, CumeDist, DenseRank, Descending, Expression, Lag, Lead, NamedExpression, NthValue, NTile, PercentRank, RangeFrame, Rank, RowNumber, SortOrder, SpecialFrameBoundary, SpecifiedWindowFrame}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, ApproximatePercentile}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.execution.HiveFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.hadoop.fs.Path

import scala.util.control.Breaks.breakable

class VeloxBackend extends Backend {
  override def name(): String = VeloxBackend.BACKEND_NAME
  override def buildInfo(): BackendBuildInfo =
    BackendBuildInfo("Velox", VELOX_BRANCH, VELOX_REVISION, VELOX_REVISION_TIME)
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
  val BACKEND_NAME = "velox"
}

object VeloxBackendSettings extends BackendSettingsApi {

  val SHUFFLE_SUPPORTED_CODEC = Set("lz4", "zstd")

  val GLUTEN_VELOX_UDF_LIB_PATHS = getBackendConfigPrefix() + ".udfLibraryPaths"
  val GLUTEN_VELOX_DRIVER_UDF_LIB_PATHS = getBackendConfigPrefix() + ".driver.udfLibraryPaths"
  val GLUTEN_VELOX_INTERNAL_UDF_LIB_PATHS = getBackendConfigPrefix() + ".internal.udfLibraryPaths"
  val GLUTEN_VELOX_UDF_ALLOW_TYPE_CONVERSION = getBackendConfigPrefix() + ".udfAllowTypeConversion"

  val MAXIMUM_BATCH_SIZE: Int = 32768

  override def validateScan(
      format: ReadFileFormat,
      fields: Array[StructField],
      partTable: Boolean,
      rootPaths: Seq[String],
      paths: Seq[String]): ValidationResult = {
    val filteredRootPaths = distinctRootPaths(rootPaths)
    if (
      !filteredRootPaths.isEmpty && !VeloxFileSystemValidationJniWrapper
        .allSupportedByRegisteredFileSystems(filteredRootPaths.toArray)
    ) {
      return ValidationResult.failed(
        s"Scheme of [$filteredRootPaths] is not supported by registered file systems.")
    }
    // Validate if all types are supported.
    def validateTypes(validatorFunc: PartialFunction[StructField, String]): ValidationResult = {
      // Collect unsupported types.
      val unsupportedDataTypeReason = fields.collect(validatorFunc)
      if (unsupportedDataTypeReason.isEmpty) {
        ValidationResult.succeeded
      } else {
        ValidationResult.failed(
          s"Found unsupported data type in $format: ${unsupportedDataTypeReason.mkString(", ")}.")
      }
    }

    val parquetTypeValidatorWithComplexTypeFallback: PartialFunction[StructField, String] = {
      case StructField(_, arrayType: ArrayType, _, _) =>
        arrayType.simpleString + " is forced to fallback."
      case StructField(_, mapType: MapType, _, _) =>
        mapType.simpleString + " is forced to fallback."
      case StructField(_, structType: StructType, _, _) =>
        structType.simpleString + " is forced to fallback."
      case StructField(_, timestampType: TimestampType, _, _)
          if GlutenConfig.getConf.forceParquetTimestampTypeScanFallbackEnabled =>
        timestampType.simpleString + " is forced to fallback."
    }
    val orcTypeValidatorWithComplexTypeFallback: PartialFunction[StructField, String] = {
      case StructField(_, arrayType: ArrayType, _, _) =>
        arrayType.simpleString + " is forced to fallback."
      case StructField(_, mapType: MapType, _, _) =>
        mapType.simpleString + " is forced to fallback."
      case StructField(_, structType: StructType, _, _) =>
        structType.simpleString + " is forced to fallback."
      case StructField(_, stringType: StringType, _, metadata)
          if isCharType(stringType, metadata) =>
        CharVarcharUtils.getRawTypeString(metadata) + " not support"
      case StructField(_, TimestampType, _, _) => "TimestampType not support"
    }
    format match {
      case ParquetReadFormat =>
        val typeValidator: PartialFunction[StructField, String] = {
          // Parquet scan of nested array with struct/array as element type is unsupported in Velox.
          case StructField(_, arrayType: ArrayType, _, _)
              if arrayType.elementType.isInstanceOf[StructType] =>
            "StructType as element in ArrayType"
          case StructField(_, arrayType: ArrayType, _, _)
              if arrayType.elementType.isInstanceOf[ArrayType] =>
            "ArrayType as element in ArrayType"
          // Parquet scan of nested map with struct as key type,
          // or array type as value type is not supported in Velox.
          case StructField(_, mapType: MapType, _, _) if mapType.keyType.isInstanceOf[StructType] =>
            "StructType as Key in MapType"
          case StructField(_, mapType: MapType, _, _)
              if mapType.valueType.isInstanceOf[ArrayType] =>
            "ArrayType as Value in MapType"
          case StructField(_, TimestampType, _, _)
              if GlutenConfig.getConf.forceParquetTimestampTypeScanFallbackEnabled =>
            "TimestampType"
        }
        if (!GlutenConfig.getConf.forceComplexTypeScanFallbackEnabled) {
          validateTypes(typeValidator)
        } else {
          validateTypes(parquetTypeValidatorWithComplexTypeFallback)
        }
      case DwrfReadFormat => ValidationResult.succeeded
      case OrcReadFormat =>
        if (!GlutenConfig.getConf.veloxOrcScanEnabled) {
          ValidationResult.failed(s"Velox ORC scan is turned off.")
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
              CharVarcharUtils.getRawTypeString(metadata) + " not support"
            case StructField(_, TimestampType, _, _) => "TimestampType not support"
          }
          if (!GlutenConfig.getConf.forceComplexTypeScanFallbackEnabled) {
            validateTypes(typeValidator)
          } else {
            validateTypes(orcTypeValidatorWithComplexTypeFallback)
          }
        }
      case _ => ValidationResult.failed(s"Unsupported file format for $format.")
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
      val unsupportedTypes = fields.flatMap {
        field =>
          field.dataType match {
            case _: StructType => Some("StructType")
            case _: ArrayType => Some("ArrayType")
            case _: MapType => Some("MapType")
            case _: YearMonthIntervalType => Some("YearMonthIntervalType")
            case _ => None
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

  override def supportNativeInputFileRelatedExpr(): Boolean = true

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
            case alias: Alias => WindowFunctionsBuilder.extractWindowExpression(alias.child)
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
                if !aggrExpr.aggregateFunction.isInstanceOf[ApproximatePercentile] =>
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

  /** Get the config prefix for each backend */
  override def getBackendConfigPrefix(): String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + VeloxBackend.BACKEND_NAME

  override def shuffleSupportedCodec(): Set[String] = SHUFFLE_SUPPORTED_CODEC

  override def resolveNativeConf(nativeConf: java.util.Map[String, String]): Unit = {}

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

  override def generateHdfsConfForLibhdfs(): Boolean = true

  override def needPreComputeRangeFrameBoundary(): Boolean = true
}
