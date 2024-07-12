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
package org.apache.spark.sql

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.{ProjectExecTransformer, WholeStageTransformerSuite}
import org.apache.gluten.extension.GlutenPlan
import org.apache.gluten.utils.{BackendTestUtils, SystemParameters}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._

class GlutenExpressionDataTypesValidation extends WholeStageTransformerSuite {
  protected val resourcePath: String = null
  protected val fileFormat: String = null
  override protected val logLevel: String = "INFO"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set("spark.gluten.ui.enabled", "false")
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set("spark.gluten.sql.enable.native.validation", "false")
        .set(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getClickHouseLibPath)
    }
    conf
  }

  private case class DummyPlan() extends LeafExecNode {
    override def output: Seq[Attribute] = Seq()

    override val metrics: Map[String, SQLMetric] = Map.empty

    override def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException(
      "Just a dummy plan.")
  }

  private val allPrimitiveDataTypes: Seq[DataType] =
    Seq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      DecimalType(5, 1),
      StringType,
      BinaryType,
      DateType,
      TimestampType)

  private val allComplexDataTypes: Seq[DataType] = Seq(
    // Currently, only check certain inner types, assuming they are representative
    // for checking most expressions.
    ArrayType(IntegerType),
    MapType(StringType, IntegerType),
    StructType(Seq(StructField("a", StringType), StructField("b", IntegerType)))
  )

  def generateChildExpression(t: DataType): Expression = {
    t match {
      case _: IntegralType => Literal(null, t)
      case _: FractionalType => Literal(null, t)
      case StringType | BinaryType => Literal("123")
      case DateType => Literal(null, t)
      case TimestampType => Literal(null, t)
      case ArrayType(_, _) => Literal(null, t)
      case MapType(_, _, _) => Literal(null, t)
      case StructType(_) => Literal(null, t)
      case _ => throw new UnsupportedOperationException("Not supported type: " + t)
    }
  }
  def generateGlutenProjectPlan(expr: Expression): GlutenPlan = {
    val namedExpr = Seq(Alias(expr, "r")())
    ProjectExecTransformer(namedExpr, DummyPlan())
  }

  test("cast") {
    for (from <- allPrimitiveDataTypes ++ allComplexDataTypes) {
      for (to <- allPrimitiveDataTypes ++ allComplexDataTypes) {
        if (to != from) {
          val castExpr = Cast(generateChildExpression(from), to)
          if (castExpr.checkInputDataTypes().isSuccess) {
            val glutenProject = generateGlutenProjectPlan(castExpr)
            if (glutenProject.doValidate().ok()) {
              logInfo("## cast validation passes: cast from " + from + " to " + to)
            } else {
              logInfo("!! cast validation fails: cast from " + from + " to " + to)
            }
          }
        }
      }
    }
  }

  test("unary expressions with expected input types") {
    val functionRegistry = spark.sessionState.functionRegistry
    val sparkBuiltInFunctions = functionRegistry.listFunction()
    for (func <- sparkBuiltInFunctions) {
      val builder = functionRegistry.lookupFunctionBuilder(func).get
      var expr: Expression = null
      try {
        // Instantiate an expression with null input. Just for obtaining the instance for checking
        // its allowed input types.
        expr = builder(Seq(null))
      } catch {
        // Ignore the exception as some expression builders require more than one input.
        case _: Throwable =>
      }
      if (
        expr != null && expr.isInstanceOf[ExpectsInputTypes] && expr.isInstanceOf[UnaryExpression]
      ) {
        val acceptedTypes = allPrimitiveDataTypes.filter(
          expr.asInstanceOf[ExpectsInputTypes].inputTypes.head.acceptsType(_))
        if (acceptedTypes.isEmpty) {
          logWarning("Any given type is not accepted for " + expr.getClass.getSimpleName)
        }
        acceptedTypes.foreach(
          t => {
            val child = generateChildExpression(t)
            // Builds an expression whose child's type is really accepted in Spark.
            val targetExpr = builder(Seq(child))
            val glutenProject = generateGlutenProjectPlan(targetExpr)
            if (glutenProject.doValidate().ok()) {
              logInfo("## validation passes: " + targetExpr.getClass.getSimpleName + "(" + t + ")")
            } else {
              logInfo("!! validation fails: " + targetExpr.getClass.getSimpleName + "(" + t + ")")
            }
          })
      }
    }
  }

}
