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

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Alias, CumeDist, DenseRank, Descending, Expression, Lag, Lead, Literal, MakeYMInterval, NamedExpression, NthValue, NTile, PercentRank, Rand, RangeFrame, Rank, RowNumber, SortOrder, SpecialFrameBoundary, SpecifiedWindowFrame, Uuid}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, ApproximatePercentile, Count, Sum}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

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
  override def broadcastApi(): BroadcastApi = new VeloxBroadcastApi
  override def settings(): BackendSettingsApi = VeloxBackendSettings
}

object VeloxBackend {
  val BACKEND_NAME = "velox"
}

object VeloxBackendSettings extends BackendSettingsApi {

  val SHUFFLE_SUPPORTED_CODEC = Set("lz4", "zstd")

  val GLUTEN_VELOX_UDF_LIB_PATHS = getBackendConfigPrefix() + ".udfLibraryPaths"
  val GLUTEN_VELOX_DRIVER_UDF_LIB_PATHS = getBackendConfigPrefix() + ".driver.udfLibraryPaths"

  val MAXIMUM_BATCH_SIZE: Int = 32768

  override def supportFileFormatRead(
      format: ReadFileFormat,
      fields: Array[StructField],
      partTable: Boolean,
      paths: Seq[String]): ValidationResult = {
    // Validate if all types are supported.
    def validateTypes(validatorFunc: PartialFunction[StructField, String]): ValidationResult = {
      // Collect unsupported types.
      val unsupportedDataTypeReason = fields.collect(validatorFunc)
      if (unsupportedDataTypeReason.isEmpty) {
        ValidationResult.ok
      } else {
        ValidationResult.notOk(
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
        }
        if (!GlutenConfig.getConf.forceComplexTypeScanFallbackEnabled) {
          validateTypes(typeValidator)
        } else {
          validateTypes(parquetTypeValidatorWithComplexTypeFallback)
        }
      case DwrfReadFormat => ValidationResult.ok
      case OrcReadFormat =>
        if (!GlutenConfig.getConf.veloxOrcScanEnabled) {
          ValidationResult.notOk(s"Velox ORC scan is turned off.")
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
      case _ => ValidationResult.notOk(s"Unsupported file format for $format.")
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

  override def supportWriteFilesExec(
      format: FileFormat,
      fields: Array[StructField],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String]): ValidationResult = {

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
    def validateDateTypes(): Option[String] = {
      val unsupportedTypes = fields.flatMap {
        field =>
          field.dataType match {
            case _: TimestampType => Some("TimestampType")
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
        case _: ParquetFileFormat => None
        case _: FileFormat => Some("Only parquet fileformat is supported in Velox backend.")
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
      .orElse(validateDateTypes())
      .orElse(validateWriteFilesOptions())
      .orElse(validateBucketSpec()) match {
      case Some(reason) => ValidationResult.notOk(reason)
      case _ => ValidationResult.ok
    }
  }

  override def supportNativeWrite(fields: Array[StructField]): Boolean = {
    fields.map {
      field =>
        field.dataType match {
          case _: TimestampType | _: StructType | _: ArrayType | _: MapType => return false
          case _ =>
        }
    }
    true
  }

  override def supportNativeMetadataColumns(): Boolean = true

  override def supportExpandExec(): Boolean = true

  override def supportSortExec(): Boolean = true

  override def supportSortMergeJoinExec(): Boolean = {
    GlutenConfig.getConf.enableColumnarSortMergeJoin
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

          // Block the offloading by checking Velox's current limitations
          // when literal bound type is used for RangeFrame.
          def checkLimitations(swf: SpecifiedWindowFrame, orderSpec: Seq[SortOrder]): Unit = {
            def doCheck(bound: Expression, isUpperBound: Boolean): Unit = {
              bound match {
                case _: Literal =>
                  throw new GlutenNotSupportException(
                    "Window frame of type RANGE does" +
                      " not support constant arguments in velox backend")
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
                  val rawValue = e.eval().toString.toLong
                  if (isUpperBound && rawValue < 0) {
                    throw new GlutenNotSupportException("Negative upper bound is not supported!")
                  } else if (!isUpperBound && rawValue > 0) {
                    throw new GlutenNotSupportException("Positive lower bound is not supported!")
                  }
                case _ =>
              }
            }
            doCheck(swf.upper, true)
            doCheck(swf.lower, false)
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
    GlutenConfig.getConf.isUseColumnarShuffleManager ||
    GlutenConfig.getConf.isUseCelebornShuffleManager ||
    GlutenConfig.getConf.isUseUniffleShuffleManager
  }

  override def enableJoinKeysRewrite(): Boolean = false

  override def supportHashBuildJoinTypeOnLeft: JoinType => Boolean = {
    t =>
      if (super.supportHashBuildJoinTypeOnLeft(t)) {
        true
      } else {
        t match {
          // OPPRO-266: For Velox backend, build right and left are both supported for
          // LeftOuter and LeftSemi.
          // FIXME Hongze 22/12/06
          //  HashJoin.scala in shim was not always loaded by class loader.
          //  The file should be removed and we temporarily disable the improvement
          //  introduced by OPPRO-266 by commenting out the following prerequisite
          //  condition.
//          case LeftOuter | LeftSemi => true
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
          // FIXME Hongze 22/12/06
          //  HashJoin.scala in shim was not always loaded by class loader.
          //  The file should be removed and we temporarily disable the improvement
          //  introduced by OPPRO-266 by commenting out the following prerequisite
          //  condition.
//          case RightOuter => true
          case _ => false
        }
      }
  }

  /**
   * Check whether a plan needs to be offloaded even though they have empty input schema, e.g,
   * Sum(1), Count(1), rand(), etc.
   * @param plan:
   *   The Spark plan to check.
   */
  private def mayNeedOffload(plan: SparkPlan): Boolean = {
    def checkExpr(expr: Expression): Boolean = {
      expr match {
        // Block directly falling back the below functions by FallbackEmptySchemaRelation.
        case alias: Alias => checkExpr(alias.child)
        case _: Rand | _: Uuid | _: MakeYMInterval => true
        case _ => false
      }
    }

    plan match {
      case exec: HashAggregateExec if exec.aggregateExpressions.nonEmpty =>
        // Check Sum(1) or Count(1).
        exec.aggregateExpressions.forall(
          expression => {
            val aggFunction = expression.aggregateFunction
            aggFunction match {
              case _: Sum | _: Count =>
                aggFunction.children.size == 1 && aggFunction.children.head.equals(Literal(1))
              case _ => false
            }
          })
      case p: ProjectExec if p.projectList.nonEmpty =>
        p.projectList.forall(checkExpr(_))
      case _ =>
        false
    }
  }

  override def fallbackOnEmptySchema(plan: SparkPlan): Boolean = {
    // Count(1) and Sum(1) are special cases that Velox backend can handle.
    // Do not fallback it and its children in the first place.
    !mayNeedOffload(plan)
  }

  override def fallbackAggregateWithEmptyOutputChild(): Boolean = true

  override def recreateJoinExecOnFallback(): Boolean = true
  override def rescaleDecimalLiteral(): Boolean = true

  /** Get the config prefix for each backend */
  override def getBackendConfigPrefix(): String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + VeloxBackend.BACKEND_NAME

  override def rescaleDecimalIntegralExpression(): Boolean = true

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

  override def requiredChildOrderingForWindow(): Boolean = true

  override def staticPartitionWriteOnly(): Boolean = true

  override def supportTransformWriteFiles: Boolean = true

  override def allowDecimalArithmetic: Boolean = SQLConf.get.decimalOperationsAllowPrecisionLoss

  override def enableNativeWriteFiles(): Boolean = {
    GlutenConfig.getConf.enableNativeWriter.getOrElse(
      SparkShimLoader.getSparkShims.enableNativeWriteFilesByDefault()
    )
  }

  override def shouldRewriteCount(): Boolean = {
    // Velox backend does not support count if it has more that one child,
    // so we should rewrite it.
    true
  }

  override def supportCartesianProductExec(): Boolean = true

  override def supportBroadcastNestedLoopJoinExec(): Boolean = true

  override def shouldRewriteTypedImperativeAggregate(): Boolean = {
    // The intermediate type of collect_list, collect_set in Velox backend is not consistent with
    // vanilla Spark, we need to rewrite the aggregate to get the correct data type.
    true
  }

  override def shouldRewriteCollect(): Boolean = true
}
