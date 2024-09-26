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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.extension.{GlutenPlan, ValidationResult}
import org.apache.gluten.extension.columnar.validator.Validator.Passed
import org.apache.gluten.extension.columnar.validator.Validators.FallbackComplexExpressions
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.iterator.Iterators
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CaseWhen, Coalesce, Expression, If, LambdaFunction, MutableProjection, NamedExpression, NaNvl, ScalaUDF, UnsafeProjection}
import org.apache.spark.sql.execution.{ExplainUtils, ProjectExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.{MutableColumnarRow, WritableColumnVector}
import org.apache.spark.sql.hive.HiveUdfUtil
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType, TimestampType, YearMonthIntervalType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import scala.collection.mutable.ListBuffer

/**
 * Change the Project to ProjectExecTransformer + SparkPartialProjectColumnarExec e.g. sum(myudf(a)
 * + b + hash(c)), child is (a, b,c ) SparkPartialProjectColumnarExec (a, b, c, myudf(a)),
 * ProjectExecTransformer(myudf(a) + b + hash(c))
 *
 * @param original
 *   extract the ScalaUDF from original project list as Alias in UnsafeProjection and
 *   AttributeReference in SparkPartialProjectColumnarExec output
 * @param child
 *   child plan
 */
case class SparkPartialProjectColumnarExec(original: ProjectExec, child: SparkPlan)(
    replacedAliasUdf: ListBuffer[Alias])
  extends UnaryExecNode
  with GlutenPlan {

  private val debug = GlutenConfig.getConf.debug

  private val projectAttributes: ListBuffer[Attribute] = ListBuffer()
  private val projectIndexInChild: ListBuffer[Int] = ListBuffer()
  private var UDFAttrNotExists = false
  private var hasComplexDataType = replacedAliasUdf.exists(a => !validateDataType(a.dataType))
  if (!hasComplexDataType) {
    getProjectIndexInChildOutput(replacedAliasUdf)
  }

  @transient override lazy val metrics = Map(
    "time" -> SQLMetrics.createTimingMetric(sparkContext, "time of project"),
    "column_to_row_time" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of velox to Arrow ColumnarBatch"),
    "row_to_column_time" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "time of Arrow ColumnarBatch to velox")
  )

  override def output: Seq[Attribute] = child.output ++ replacedAliasUdf.map(_.toAttribute)

  final override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }

  final override protected def otherCopyArgs: Seq[AnyRef] = {
    replacedAliasUdf :: Nil
  }

  final override lazy val supportsColumnar: Boolean = true

  private def validateExpression(expr: Expression): Boolean = {
    expr.deterministic && !expr.isInstanceOf[LambdaFunction] && expr.children
      .forall(validateExpression)
  }

  private def validateDataType(dataType: DataType): Boolean = {
    dataType match {
      case _: BooleanType => true
      case _: ByteType => true
      case _: ShortType => true
      case _: IntegerType => true
      case _: LongType => true
      case _: FloatType => true
      case _: DoubleType => true
      case _: StringType => true
      case _: TimestampType => true
      case _: DateType => true
      case _: BinaryType => true
      case _: DecimalType => true
      case YearMonthIntervalType.DEFAULT => true
      case _: NullType => true
      case _ => false
    }
  }

  private def getProjectIndexInChildOutput(exprs: Seq[Expression]): Unit = {
    exprs.foreach {
      case a: AttributeReference =>
        val index = child.output.indexWhere(s => s.exprId.equals(a.exprId))
        // Some child operator as HashAggregateTransformer will not have udf child column
        if (index < 0) {
          UDFAttrNotExists = true
          log.debug(s"Expression $a should exist in child output ${child.output}")
          return
        } else if (!validateDataType(a.dataType)) {
          hasComplexDataType = true
          log.debug(s"Expression $a contains unsupported data type ${a.dataType}")
        } else if (!projectIndexInChild.contains(index)) {
          projectAttributes.append(a.toAttribute)
          projectIndexInChild.append(index)
        }
      case p => getProjectIndexInChildOutput(p.children)
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!GlutenConfig.getConf.enableColumnarPartialProject) {
      return ValidationResult.failed("Config disable this feature")
    }
    if (UDFAttrNotExists) {
      ValidationResult.failed("Attribute in the UDF does not exists in its child")
    } else if (hasComplexDataType) {
      ValidationResult.failed("Attribute in the UDF contains unsupported type")
    } else if (projectAttributes.size == child.output.size) {
      ValidationResult.failed("UDF need all the columns in child output")
    } else if (original.output.isEmpty) {
      ValidationResult.failed("Project fallback because output is empty")
    } else if (replacedAliasUdf.isEmpty) {
      ValidationResult.failed("No UDF")
    } else if (replacedAliasUdf.size > original.output.size) {
      // e.g. udf1(col) + udf2(col), it will introduce 2 cols for r2c
      ValidationResult.failed("Number of RowToColumn columns is more than ProjectExec")
    } else if (!original.projectList.forall(validateExpression(_))) {
      ValidationResult.failed("Contains expression not supported")
    } else if (isComplexExpression()) {
      ValidationResult.failed("Fallback by complex expression")
    } else {
      ValidationResult.succeeded
    }
  }

  private def isComplexExpression(): Boolean = {
    new FallbackComplexExpressions(GlutenConfig.getConf.fallbackExpressionsThreshold)
      .validate(original) match {
      case Passed => false
      case _ => true
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val totalTime = longMetric("time")
    val c2r = longMetric("column_to_row_time")
    val r2c = longMetric("row_to_column_time")
    val isMutable = canUseMutableProjection()
    child.executeColumnar().mapPartitions {
      batches =>
        val res: Iterator[Iterator[ColumnarBatch]] = new Iterator[Iterator[ColumnarBatch]] {
          override def hasNext: Boolean = batches.hasNext

          override def next(): Iterator[ColumnarBatch] = {
            val batch = batches.next()
            if (batch.numRows == 0) {
              Iterator.empty
            } else {
              val start = System.currentTimeMillis()
              val childData = ColumnarBatches.select(batch, projectIndexInChild.toArray)
              val projectedBatch = if (isMutable) {
                getProjectedBatchArrow(childData, c2r, r2c)
              } else getProjectedBatch(childData, c2r, r2c)
              val batchIterator = projectedBatch.map {
                b =>
//                  print("batch 1" + ColumnarBatches.toString(batch, 0, 20) + "\n")
//                  print("batch 2" + ColumnarBatches.toString(b, 0, 20) + "\n")
                  val compositeBatch = if (b.numCols() != 0) {
                    val handle = ColumnarBatches.compose(batch, b)
                    b.close()
                    ColumnarBatches.create(handle)
                  } else {
                    b.close()
                    ColumnarBatches.retain(batch)
                    batch
                  }
                  if (debug && compositeBatch.numCols() != output.length) {
                    throw new IllegalStateException(
                      s"Composite batch column number is ${compositeBatch.numCols()}, " +
                        s"output size is ${output.length}, " +
                        s"original batch column number is ${batch.numCols()}")
                  }
                  compositeBatch
              }
              childData.close()
              totalTime += System.currentTimeMillis() - start
              batchIterator
            }
          }
        }
        Iterators
          .wrap(res.flatten)
          .protectInvocationFlow() // Spark may call `hasNext()` again after a false output which
          // is not allowed by Gluten iterators. E.g. GroupedIterator#fetchNextGroupIterator
          .recyclePayload(_.close())
          .create()

    }
  }

  // scalastyle:off line.size.limit
  // String type cannot use MutableProjection
  // Otherwise will throw java.lang.UnsupportedOperationException: Datatype not supported StringType
  // at org.apache.spark.sql.execution.vectorized.MutableColumnarRow.update(MutableColumnarRow.java:224)
  // at org.apache.spark.sql.catalyst.expressions.GeneratedClass$SpecificMutableProjection.apply(Unknown Source)
  // scalastyle:on line.size.limit
  private def canUseMutableProjection(): Boolean = {
    replacedAliasUdf.forall(
      r =>
        r.dataType match {
          case StringType | BinaryType => false
          case _ => true
        })
  }

  /**
   * add c2r and r2c for unsupported expression child data c2r get Iterator[InternalRow], then call
   * Spark project, then r2c
   */
  private def getProjectedBatch(
      childData: ColumnarBatch,
      c2r: SQLMetric,
      r2c: SQLMetric): Iterator[ColumnarBatch] = {
    // select part of child output and child data
    val proj = UnsafeProjection.create(replacedAliasUdf, projectAttributes)
    val numOutputRows = new SQLMetric("numOutputRows")
    val numInputBatches = new SQLMetric("numInputBatches")
    val rows = VeloxColumnarToRowExec
      .toRowIterator(
        Iterator.single[ColumnarBatch](childData),
        projectAttributes,
        numOutputRows,
        numInputBatches,
        c2r)
      .map(proj)

    val schema =
      SparkShimLoader.getSparkShims.structFromAttributes(replacedAliasUdf.map(_.toAttribute))
    RowToVeloxColumnarExec.toColumnarBatchIterator(
      rows,
      schema,
      numOutputRows,
      numInputBatches,
      r2c,
      childData.numRows())
    // TODO: should check the size <= 1, but now it has bug, will change iterator to empty
  }

  private def getProjectedBatchArrow(
      childData: ColumnarBatch,
      c2r: SQLMetric,
      r2c: SQLMetric): Iterator[ColumnarBatch] = {
    // select part of child output and child data
    val proj = MutableProjection.create(replacedAliasUdf, projectAttributes)
    val numRows = childData.numRows()
    val start = System.currentTimeMillis()
    val arrowBatch =
      ColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance(), childData)
    c2r += System.currentTimeMillis() - start

    val schema =
      SparkShimLoader.getSparkShims.structFromAttributes(replacedAliasUdf.map(_.toAttribute))
    val vectors: Array[WritableColumnVector] = ArrowWritableColumnVector
      .allocateColumns(numRows, schema)
      .map {
        vector =>
          vector.setValueCount(numRows)
          vector
      }
    val targetRow = new MutableColumnarRow(vectors)
    for (i <- 0 until numRows) {
      targetRow.rowId = i
      proj.target(targetRow).apply(arrowBatch.getRow(i))
    }
    val targetBatch = new ColumnarBatch(vectors.map(_.asInstanceOf[ColumnVector]), numRows)
    val start2 = System.currentTimeMillis()
    val veloxBatch =
      ColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), targetBatch)
    r2c += System.currentTimeMillis() - start2
    Iterators
      .wrap(Iterator.single(veloxBatch))
      .recycleIterator({
        arrowBatch.close()
        targetBatch.close()
      })
      .create()
    // TODO: should check the size <= 1, but now it has bug, will change iterator to empty
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("ScalaUDF", replacedAliasUdf)}
       |${ExplainUtils.generateFieldString("ProjectOutput", projectAttributes)}
       |${ExplainUtils.generateFieldString("ProjectInputIndex", projectIndexInChild)}
       |""".stripMargin
  }

  override def simpleString(maxFields: Int): String =
    super.simpleString(maxFields) + " PartialProject " + replacedAliasUdf

  override protected def withNewChildInternal(
      newChild: SparkPlan): SparkPartialProjectColumnarExec = {
    copy(child = newChild)(replacedAliasUdf)
  }
}

object SparkPartialProjectColumnarExec {

  val projectPrefix = "_SparkPartialProject"

  private def containsUDF(expr: Expression): Boolean = {
    if (expr == null) return false
    expr match {
      case _: ScalaUDF => true
      case h if HiveUdfUtil.isHiveUdf(h) => true
      case p => p.children.exists(c => containsUDF(c))
    }
  }

  private def replaceByAlias(expr: Expression, replacedAliasUdf: ListBuffer[Alias]): Expression = {
    val replaceIndex = replacedAliasUdf.indexWhere(r => r.child.equals(expr))
    if (replaceIndex == -1) {
      val replace = Alias(expr, s"$projectPrefix${replacedAliasUdf.size}")()
      replacedAliasUdf.append(replace)
      replace.toAttribute
    } else {
      replacedAliasUdf(replaceIndex).toAttribute
    }
  }

  private def isConditionalExpression(expr: Expression): Boolean = expr match {
    case _: If => true
    case _: CaseWhen => true
    case _: NaNvl => true
    case _: Coalesce => true
    case _ => false
  }

  private def replaceExpressionUDF(
      expr: Expression,
      replacedAliasUdf: ListBuffer[Alias]): Expression = {
    if (expr == null) return null
    expr match {
      case u: ScalaUDF =>
        replaceByAlias(u, replacedAliasUdf)
      case h if HiveUdfUtil.isHiveUdf(h) =>
        replaceByAlias(h, replacedAliasUdf)
      case au @ Alias(_: ScalaUDF, _) =>
        val replaceIndex = replacedAliasUdf.indexWhere(r => r.exprId == au.exprId)
        if (replaceIndex == -1) {
          replacedAliasUdf.append(au)
          au.toAttribute
        } else {
          replacedAliasUdf(replaceIndex).toAttribute
        }
      // Alias(HiveSimpleUDF) not exists, only be Alias(ToPrettyString(HiveSimpleUDF)),
      // so don't process this condition
      case x if isConditionalExpression(x) =>
        // For example:
        // myudf is udf((x: Int) => x + 1)
        // if (isnull(cast(l_extendedprice#9 as bigint))) null
        // else myudf(knownnotnull(cast(l_extendedprice#9 as bigint)))
        // if we extract else branch, and use the data child l_extendedprice,
        // the result is incorrect for null value
        if (containsUDF(expr)) {
          replaceByAlias(expr, replacedAliasUdf)
        } else expr
      case p => p.withNewChildren(p.children.map(c => replaceExpressionUDF(c, replacedAliasUdf)))
    }
  }

  def create(original: ProjectExec): ProjectExecTransformer = {
    val replacedAliasUdf: ListBuffer[Alias] = ListBuffer()
    val newProjectList = original.projectList.map {
      p => replaceExpressionUDF(p, replacedAliasUdf).asInstanceOf[NamedExpression]
    }
    val partialProject = SparkPartialProjectColumnarExec(original, original.child)(replacedAliasUdf)
    ProjectExecTransformer(newProjectList, partialProject)
  }
}
