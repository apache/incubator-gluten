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
package io.glutenproject.backendsapi.velox

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi._
import io.glutenproject.expression.WindowFunctionsBuilder
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat.{DwrfReadFormat, OrcReadFormat, ParquetReadFormat}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, CumeDist, DenseRank, Descending, Expression, Literal, NamedExpression, PercentRank, RangeFrame, Rank, RowNumber, SortOrder, SpecialFrameBoundary, SpecifiedWindowFrame}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.types._

import scala.util.control.Breaks.{break, breakable}

class VeloxBackend extends Backend {
  override def name: String = GlutenConfig.GLUTEN_VELOX_BACKEND
  override def iteratorApi(): IteratorApi = new VeloxIteratorApi
  override def sparkPlanExecApi(): SparkPlanExecApi = new VeloxSparkPlanExecApi
  override def transformerApi(): TransformerApi = new VeloxTransformerApi
  override def validatorApi(): ValidatorApi = new VeloxValidatorApi
  override def metricsApi(): MetricsApi = new VeloxMetricsApi
  override def settings(): BackendSettings = VeloxBackendSettings

  override def contextApi(): ContextApi = new VeloxContextApi
}

object VeloxBackendSettings extends BackendSettings {
  override def supportFileFormatRead(format: ReadFileFormat,
                                     fields: Array[StructField],
                                     partTable: Boolean,
                                     paths: Seq[String]): Boolean = {
    // Validate if all types are supported.
    def validateTypes: Boolean = {
      // Collect unsupported types.
      fields.map(_.dataType).collect {
        case _: ByteType =>
        case _: ArrayType =>
        case _: MapType =>
        case _: StructType =>
      }.isEmpty
    }

    def validateFilePath: Boolean = {
      // Fallback to vanilla spark when the input path
      // does not contain the partition info.
      if (partTable && !paths.forall(_.contains("="))) {
        return false
      }
      true
    }

    format match {
      case ParquetReadFormat => validateTypes && validateFilePath
      case DwrfReadFormat => true
      case OrcReadFormat => fields.map(_.dataType).collect {
        case _: TimestampType =>
      }.isEmpty
      case _ => false
    }
  }

  override def supportExpandExec(): Boolean = true

  override def supportSortExec(): Boolean = true

  override def supportWindowExec(windowFunctions: Seq[NamedExpression]): Boolean = {
    var allSupported = true
    breakable {
      windowFunctions.foreach(func => {
        val windowExpression = func match {
          case alias: Alias => WindowFunctionsBuilder.extractWindowExpression(alias.child)
          case _ => throw new UnsupportedOperationException(s"$func is not supported.")
        }

        // Block the offloading by checking Velox's current limitations
        // when literal bound type is used for RangeFrame.
        def checkLimitations(swf: SpecifiedWindowFrame, orderSpec: Seq[SortOrder]): Unit = {
          def doCheck(bound: Expression, isUpperBound: Boolean): Unit = {
            bound match {
              case _: SpecialFrameBoundary =>
              case e if e.foldable =>
                orderSpec foreach (order =>
                  order.direction match {
                    case Descending =>
                      throw new UnsupportedOperationException("DESC order is not supported when" +
                          " literal bound type is used!")
                    case _ =>
                  })
                orderSpec foreach (order =>
                  order.dataType match {
                    case ByteType | ShortType | IntegerType | LongType | DateType =>
                    case _ =>
                      throw new UnsupportedOperationException("Only integral type & date type are" +
                          " supported for sort key when literal bound type is used!")
                  })
                val rawValue = e.eval().toString.toLong
                if (isUpperBound && rawValue < 0) {
                  throw new UnsupportedOperationException("Negative upper bound is not supported!")
                } else if (!isUpperBound && rawValue > 0) {
                  throw new UnsupportedOperationException("Positive lower bound is not supported!")
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
          case _: RowNumber | _: AggregateExpression | _: Rank | _: CumeDist | _: DenseRank |
               _: PercentRank =>
          case _ =>
            allSupported = false
            break
        }})
    }
    allSupported
  }

  override def supportColumnarShuffleExec(): Boolean = {
    GlutenConfig.getConf.isUseColumnarShuffleManager ||
      GlutenConfig.getConf.isUseCelebornShuffleManager
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

  override def disableVanillaColumnarReaders(): Boolean = true

  /**
   * Check whether plan is Count(1).
   * @param plan: The Spark plan to check.
   * @return Whether plan is an Aggregation of Count(1).
   */
  private def isCount1(plan: SparkPlan): Boolean = {
    plan match {
      case exec: HashAggregateExec if exec.aggregateExpressions.forall(expression =>
        expression.aggregateFunction.isInstanceOf[Count] &&
          expression.aggregateFunction.asInstanceOf[Count].children.forall(child =>
            child.isInstanceOf[Literal] && child.asInstanceOf[Literal].value == 1)) =>
        true
      case _ =>
        false
    }
  }

  /**
   * Check whether plan is Sum(1).
   * @param plan: The Spark plan to check.
   * @return Whether plan is an Aggregation of Sum(1).
   */
  private def isSum1(plan: SparkPlan): Boolean = {
    plan match {
      case exec: HashAggregateExec if exec.aggregateExpressions.forall(expression =>
        expression.aggregateFunction.isInstanceOf[Sum] &&
          expression.aggregateFunction.asInstanceOf[Sum].children.forall(child =>
            child.isInstanceOf[Literal] && child.asInstanceOf[Literal].value == 1)) =>
        true
      case _ =>
        false
    }
  }
  
  override def fallbackOnEmptySchema(plan: SparkPlan): Boolean = {
    // Count(1) is a special case to handle. Do not fallback it and its children in the first place.
    !(isCount1(plan) || isSum1(plan))
  }

  override def fallbackAggregateWithChild(): Boolean = true

  override def recreateJoinExecOnFallback(): Boolean = true
  override def removeHashColumnFromColumnarShuffleExchangeExec(): Boolean = true
  override def rescaleDecimalLiteral(): Boolean = true

  override def replaceSortAggWithHashAgg: Boolean = GlutenConfig.getConf.forceToUseHashAgg

  /**
   * Get the config prefix for each backend
   */
  override def getBackendConfigPrefix(): String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + GlutenConfig.GLUTEN_VELOX_BACKEND

  override def rescaleDecimalIntegralExpression(): Boolean = true
}
