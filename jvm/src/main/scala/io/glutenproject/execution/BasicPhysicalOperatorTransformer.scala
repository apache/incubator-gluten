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

import scala.collection.JavaConverters._

import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.GlutenConfig
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.vectorized.ExpressionEvaluator
import java.util

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.util.StructTypeFWD
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class FilterExecBaseTransformer(
                                          condition: Expression,
                                          child: SparkPlan) extends UnaryExecNode
  with TransformSupport
  with PredicateHelper
  with AliasAwareOutputPartitioning
  with Logging {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_filter"))
  val sparkConf: SparkConf = sparkContext.getConf
  val numOutputBatches: SQLMetric = longMetric("numOutputBatches")
  val numOutputRows: SQLMetric = longMetric("numOutputRows")
  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }
  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  def doValidate(): Boolean

  def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
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

  override def updateMetrics(outNumBatches: Long, outNumRows: Long): Unit = {
    numOutputBatches += outNumBatches
    numOutputRows += outNumRows
  }

  override def getChild: SparkPlan = child

  def doTransform(context: SubstraitContext): TransformContext

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  // override def canEqual(that: Any): Boolean = false

  def getRelNode(args: java.lang.Object,
                 condExpr: Expression,
                 originalInputAttributes: Seq[Attribute],
                 input: RelNode,
                 validation: Boolean): RelNode = {
    if (condExpr == null) {
      return input
    }
    val columnarCondExpr: Expression = ExpressionConverter
      .replaceWithExpressionTransformer(condExpr, attributeSeq = originalInputAttributes)
    val condExprNode =
      columnarCondExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!validation) {
      RelBuilder.makeFilterRel(input, condExprNode)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(inputTypeNodeList).toProtobuf))
      RelBuilder.makeFilterRel(input, condExprNode, extensionNode)
    }
  }

  override protected def outputExpressions: Seq[NamedExpression] = output

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  protected override def doExecute()
  : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }
}

case class FilterExecTransformer(condition: Expression, child: SparkPlan)
  extends FilterExecBaseTransformer(condition, child) {

  override def doValidate(): Boolean = {
    if (condition == null) {
      // The computing of this Filter is not needed.
      return true
    }
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val relNode = try {
      getRelNode(substraitContext.registeredFunction, condition, child.output,
        null, validation = true)
    } catch {
      case e: Throwable =>
        logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
        return false
    }
    val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
    // Then, validate the generated plan in native engine.
    if (GlutenConfig.getConf.enableNativeValidation) {
      val validator = new ExpressionEvaluator()
      validator.doValidate(planNode.toProtobuf.toByteArray)
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
    if (condition == null) {
      // The computing for this filter is not needed.
      return childCtx
    }
    val currRel = if (childCtx != null) {
      getRelNode(context.registeredFunction, condition, child.output,
        childCtx.root, validation = false)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute](child.output.asJava)
      getRelNode(context.registeredFunction, condition, child.output,
        RelBuilder.makeReadRel(attrList, context), validation = false)
    }
    if (currRel == null) {
      return childCtx
    }
    val inputAttributes = if (childCtx != null) {
      // Use the outputAttributes of child context as inputAttributes.
      childCtx.outputAttributes
    } else {
      child.output
    }
    TransformContext(inputAttributes, output, currRel)
  }
}

case class ProjectExecTransformer(projectList: Seq[NamedExpression],
                                  child: SparkPlan) extends UnaryExecNode
  with TransformSupport
  with PredicateHelper
  with AliasAwareOutputPartitioning
  with Logging {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_project"))
  val sparkConf: SparkConf = sparkContext.getConf
  val numOutputBatches: SQLMetric = longMetric("numOutputBatches")
  val numOutputRows: SQLMetric = longMetric("numOutputRows")

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def doValidate(): Boolean = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val relNode = try {
      getRelNode(substraitContext.registeredFunction, projectList, child.output,
        null, validation = true)
    } catch {
      case e: Throwable =>
        logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
        return false
    }
    if (relNode == null) {
      return false
    }
    val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
    // Then, validate the generated plan in native engine.
    if (GlutenConfig.getConf.enableNativeValidation) {
      val validator = new ExpressionEvaluator()
      validator.doValidate(planNode.toProtobuf.toByteArray)
    } else {
      true
    }
  }

  def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
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

  override def updateMetrics(outNumBatches: Long, outNumRows: Long): Unit = {
    numOutputBatches += outNumBatches
    numOutputRows += outNumRows
  }

  override def getChild: SparkPlan = child

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }
    val currRel = if (childCtx != null) {
      getRelNode(context.registeredFunction, projectList, child.output,
        childCtx.root, validation = false)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context)
      getRelNode(context.registeredFunction, projectList, child.output,
        readRel, validation = false)
    }

    if (currRel == null) {
      return childCtx
    }
    val inputAttributes = if (childCtx != null) {
      // Use the outputAttributes of child context as inputAttributes.
      childCtx.outputAttributes
    } else {
      child.output
    }
    TransformContext(inputAttributes, output, currRel)
  }

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  // override def canEqual(that: Any): Boolean = false

  def getRelNode(args: java.lang.Object,
                 projectList: Seq[NamedExpression],
                 originalInputAttributes: Seq[Attribute],
                 input: RelNode,
                 validation: Boolean): RelNode = {
    if (projectList == null || projectList.isEmpty) {
      input
    } else {
      val columnarProjExprs: Seq[Expression] = projectList.map(expr => {
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, attributeSeq = originalInputAttributes)
      })
      val projExprNodeList = new java.util.ArrayList[ExpressionNode]()
      for (expr <- columnarProjExprs) {
        projExprNodeList.add(expr.asInstanceOf[ExpressionTransformer].doTransform(args))
      }
      if (!validation) {
        RelBuilder.makeProjectRel(input, projExprNodeList)
      } else {
        // Use a extension node to send the input types through Substrait plan for validation.
        val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
        for (attr <- originalInputAttributes) {
          inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        }
        val extensionNode = ExtensionBuilder.makeAdvancedExtension(
          Any.pack(TypeBuilder.makeStruct(inputTypeNodeList).toProtobuf))
        RelBuilder.makeProjectRel(input, projExprNodeList, extensionNode)
      }
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  protected override def doExecute()
  : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }
}

case class UnionExecTransformer(children: Seq[SparkPlan]) extends SparkPlan with TransformSupport {
  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(StructTypeFWD.merge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId,
          firstAttr.qualifier)
      }
    }
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    throw new UnsupportedOperationException(s"This operator doesn't support inputRDDs.")
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    throw new UnsupportedOperationException(s"This operator doesn't support getBuildPlans.")
  }

  override def getStreamedLeafPlan: SparkPlan = {
    throw new UnsupportedOperationException(s"This operator doesn't support getStreamedLeafPlan.")
  }

  override def getChild: SparkPlan = {
    throw new UnsupportedOperationException(s"This operator doesn't support getChild.")
  }

  override def doValidate(): Boolean = false

  override def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(s"This operator doesn't support doTransform.")
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  protected override def doExecute()
  : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }
}

/** Contains functions for the comparision and separation of the filter conditions
 * in Scan and Filter.
 * Contains the function to manually push down the conditions into Scan.
 */
object FilterHandler {
  /** Get the original filter conditions in Scan for the comparison with those in Filter.
   *
   * @param plan : the Spark plan
   * @return If the plan is FileSourceScanExec or BatchScanExec, return the filter conditions in it.
   *         Otherwise, return empty sequence.
   */
  def getScanFilters(plan: SparkPlan): Seq[Expression] = {
    plan match {
      case fileSourceScan: FileSourceScanExec =>
        fileSourceScan.dataFilters
      case batchScan: BatchScanExec =>
        batchScan.scan match {
          case scan: FileScan =>
            scan.dataFilters
          case _ =>
            throw new UnsupportedOperationException(
              s"${batchScan.scan.getClass.toString} is not supported")
        }
      case _ =>
        Seq()
    }
  }

  /** Flatten the condition connected with 'And'. Return the filter conditions with sequence.
   *
   * @param condition : the condition connected with 'And'
   * @return flattened conditions in sequence
   */
  def flattenCondition(condition: Expression): Seq[Expression] = {
    var expressions: Seq[Expression] = Seq()
    condition match {
      case and: And =>
        and.children.foreach(expression => {
          expressions ++= flattenCondition(expression)
        })
      case _ =>
        expressions = expressions :+ condition
    }
    expressions
  }

  /** Compare the semantics of the filter conditions pushed down to Scan and in the Filter.
   *
   * @param scanFilters : the conditions pushed down into Scan
   * @param filters     : the conditions in the Filter after the Scan
   * @return the filter conditions not pushed down into Scan.
   */
  def getLeftFilters(scanFilters: Seq[Expression], filters: Seq[Expression]): Seq[Expression] = {
    var leftFilters: Seq[Expression] = Seq()
    for (expression <- filters) {
      if (!scanFilters.exists(_.semanticEquals(expression))) {
        leftFilters = leftFilters :+ expression.clone()
      }
    }
    leftFilters
  }

  // Separate and compare the filter conditions in Scan and Filter.
  // Push down the left conditions in Filter into Scan.
  def applyFilterPushdownToScan(plan: FilterExec): SparkPlan = plan.child match {
    case fileSourceScan: FileSourceScanExec =>
      val leftFilters =
        getLeftFilters(fileSourceScan.dataFilters, flattenCondition(plan.condition))
      new FileSourceScanExecTransformer(
        fileSourceScan.relation,
        fileSourceScan.output,
        fileSourceScan.requiredSchema,
        fileSourceScan.partitionFilters,
        fileSourceScan.optionalBucketSet,
        fileSourceScan.optionalNumCoalescedBuckets,
        fileSourceScan.dataFilters ++ leftFilters,
        fileSourceScan.tableIdentifier,
        fileSourceScan.disableBucketedScan)
    case batchScan: BatchScanExec =>
      batchScan.scan match {
        case scan: FileScan =>
          val leftFilters =
            getLeftFilters(scan.dataFilters, flattenCondition(plan.condition))
          new BatchScanExecTransformer(batchScan.output, batchScan.scan, leftFilters)
        case _ =>
          throw new UnsupportedOperationException(
            s"${batchScan.scan.getClass.toString} is not supported.")
      }
    case other =>
      throw new UnsupportedOperationException(s"${other.getClass.toString} is not supported.")
  }
}
