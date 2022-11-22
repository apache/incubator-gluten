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

import java.util

import scala.collection.JavaConverters._

import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.utils.BindReferencesUtil
import io.glutenproject.vectorized.{ExpressionEvaluator, OperatorMetrics}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanExecBase, FileScan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.util.StructTypeFWD
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class FilterExecBaseTransformer(condition: Expression,
                                         child: SparkPlan) extends UnaryExecNode
  with TransformSupport
  with PredicateHelper
  with AliasAwareOutputPartitioning
  with Logging {

  override lazy val metrics = Map(
    "inputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
    "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
    "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
    "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
    "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
    "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
    "count" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
    "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_filter"),
    "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
    "numMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of memory allocations"))

  val inputRows: SQLMetric = longMetric("inputRows")
  val inputVectors: SQLMetric = longMetric("inputVectors")
  val inputBytes: SQLMetric = longMetric("inputBytes")
  val rawInputRows: SQLMetric = longMetric("rawInputRows")
  val rawInputBytes: SQLMetric = longMetric("rawInputBytes")
  val outputRows: SQLMetric = longMetric("outputRows")
  val outputVectors: SQLMetric = longMetric("outputVectors")
  val outputBytes: SQLMetric = longMetric("outputBytes")
  val count: SQLMetric = longMetric("count")
  val wallNanos: SQLMetric = longMetric("wallNanos")
  val peakMemoryBytes: SQLMetric = longMetric("peakMemoryBytes")
  val numMemoryAllocations: SQLMetric = longMetric("numMemoryAllocations")

  val sparkConf: SparkConf = sparkContext.getConf
  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }
  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  def doValidate(): Boolean

  override def isNullIntolerant(expr: Expression): Boolean = expr match {
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
    outputVectors += outNumBatches
    outputRows += outNumRows
  }

  override def updateNativeMetrics(operatorMetrics: OperatorMetrics): Unit = {
    if (operatorMetrics != null) {
      inputRows += operatorMetrics.inputRows
      inputVectors += operatorMetrics.inputVectors
      inputBytes += operatorMetrics.inputBytes
      rawInputRows += operatorMetrics.rawInputRows
      rawInputBytes += operatorMetrics.rawInputBytes
      outputRows += operatorMetrics.outputRows
      outputVectors += operatorMetrics.outputVectors
      outputBytes += operatorMetrics.outputBytes
      count += operatorMetrics.count
      wallNanos += operatorMetrics.wallNanos
      peakMemoryBytes += operatorMetrics.peakMemoryBytes
      numMemoryAllocations += operatorMetrics.numMemoryAllocations
    }
  }

  override def getChild: SparkPlan = child

  def doTransform(context: SubstraitContext): TransformContext

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  // override def canEqual(that: Any): Boolean = false

  def getRelNode(context: SubstraitContext,
                 condExpr: Expression,
                 originalInputAttributes: Seq[Attribute],
                 operatorId: Long,
                 input: RelNode,
                 validation: Boolean): RelNode = {
    val args = context.registeredFunction
    if (condExpr == null) {
      return input
    }
    val columnarCondExpr: Expression = ExpressionConverter
      .replaceWithExpressionTransformer(condExpr, attributeSeq = originalInputAttributes)
    val condExprNode =
      columnarCondExpr.asInstanceOf[ExpressionTransformer].doTransform(args)

    if (!validation) {
      RelBuilder.makeFilterRel(input, condExprNode, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeFilterRel(input, condExprNode, extensionNode, context, operatorId)
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
    val operatorId = substraitContext.nextOperatorId
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val relNode = try {
      getRelNode(
        substraitContext, condition, child.output, operatorId, null, validation = true)
    } catch {
      case e: Throwable =>
        logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
        return false
    }

    // For now arrow backend only support scan + filter pattern
    if (BackendsApiManager.getSettings.fallbackFilterWithoutConjunctiveScan()) {
      if (!(child.isInstanceOf[DataSourceScanExec] ||
        child.isInstanceOf[DataSourceV2ScanExecBase])) {
        return false
      }
    }

    // Then, validate the generated plan in native engine.
    if (GlutenConfig.getConf.enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
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

    val operatorId = context.nextOperatorId
    if (condition == null && childCtx != null) {
      // The computing for this filter is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val currRel = if (childCtx != null) {
      getRelNode(
        context, condition, child.output, operatorId, childCtx.root, validation = false)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute](child.output.asJava)
      getRelNode(context, condition, child.output, operatorId,
        RelBuilder.makeReadRel(attrList, context, operatorId), validation = false)
    }
    assert(currRel != null, "Filter rel should be valid.")
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

  override protected def withNewChildInternal(newChild: SparkPlan): FilterExecTransformer =
    copy(child = newChild)
}

case class ProjectExecTransformer(projectList: Seq[NamedExpression],
                                  child: SparkPlan) extends UnaryExecNode
  with TransformSupport
  with PredicateHelper
  with AliasAwareOutputPartitioning
  with Logging {

  override lazy val metrics = Map(
    "inputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
    "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
    "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
    "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
    "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
    "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
    "count" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
    "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_project"),
    "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
    "numMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of memory allocations"))

  val inputRows: SQLMetric = longMetric("inputRows")
  val inputVectors: SQLMetric = longMetric("inputVectors")
  val inputBytes: SQLMetric = longMetric("inputBytes")
  val rawInputRows: SQLMetric = longMetric("rawInputRows")
  val rawInputBytes: SQLMetric = longMetric("rawInputBytes")
  val outputRows: SQLMetric = longMetric("outputRows")
  val outputVectors: SQLMetric = longMetric("outputVectors")
  val outputBytes: SQLMetric = longMetric("outputBytes")
  val count: SQLMetric = longMetric("count")
  val wallNanos: SQLMetric = longMetric("wallNanos")
  val peakMemoryBytes: SQLMetric = longMetric("peakMemoryBytes")
  val numMemoryAllocations: SQLMetric = longMetric("numMemoryAllocations")

  val sparkConf: SparkConf = sparkContext.getConf

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def doValidate(): Boolean = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val operatorId = substraitContext.nextOperatorId
    val relNode = try {
      getRelNode(
        substraitContext, projectList, child.output, operatorId, null, validation = true)
    } catch {
      case e: Throwable =>
        logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
        return false
    }
    // Then, validate the generated plan in native engine.
    if (relNode != null && GlutenConfig.getConf.enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
      val validator = new ExpressionEvaluator()
      validator.doValidate(planNode.toProtobuf.toByteArray)
    } else {
      true
    }
  }

  override def isNullIntolerant(expr: Expression): Boolean = expr match {
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
    outputVectors += outNumBatches
    outputRows += outNumRows
  }

  override def updateNativeMetrics(operatorMetrics: OperatorMetrics): Unit = {
    if (operatorMetrics != null) {
      inputRows += operatorMetrics.inputRows
      inputVectors += operatorMetrics.inputVectors
      inputBytes += operatorMetrics.inputBytes
      rawInputRows += operatorMetrics.rawInputRows
      rawInputBytes += operatorMetrics.rawInputBytes
      outputRows += operatorMetrics.outputRows
      outputVectors += operatorMetrics.outputVectors
      outputBytes += operatorMetrics.outputBytes
      count += operatorMetrics.count
      wallNanos += operatorMetrics.wallNanos
      peakMemoryBytes += operatorMetrics.peakMemoryBytes
      numMemoryAllocations += operatorMetrics.numMemoryAllocations
    }
  }

  override def getChild: SparkPlan = child

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }
    val operatorId = context.nextOperatorId
    if ((projectList == null || projectList.isEmpty) && childCtx != null) {
      // The computing for this project is not needed.
      // the child may be an input adapter and childCtx is null. In this case we want to
      // make a read node with non-empty base_schema.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val (currRel, inputAttributes) = if (childCtx != null) {
      (getRelNode(
        context, projectList, child.output, operatorId, childCtx.root, validation = false),
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
        context, projectList, child.output, operatorId, readRel, validation = false),
        child.output)
    }
    assert(currRel != null, "Project Rel should be valid")

    val outputAttrs = BindReferencesUtil.bindReferencesWithNullable(output, inputAttributes)
    TransformContext(inputAttributes, outputAttrs, currRel)
  }

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  // override def canEqual(that: Any): Boolean = false

  def getRelNode(context: SubstraitContext,
                 projectList: Seq[NamedExpression],
                 originalInputAttributes: Seq[Attribute],
                 operatorId: Long,
                 input: RelNode,
                 validation: Boolean): RelNode = {
    val args = context.registeredFunction
    val columnarProjExprs: Seq[Expression] = projectList.map(expr => {
      ExpressionConverter
        .replaceWithExpressionTransformer(expr, attributeSeq = originalInputAttributes)
    })
    val projExprNodeList = new java.util.ArrayList[ExpressionNode]()
    for (expr <- columnarProjExprs) {
      projExprNodeList.add(expr.asInstanceOf[ExpressionTransformer].doTransform(args))
    }
    if (!validation) {
      RelBuilder.makeProjectRel(input, projExprNodeList, context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(input, projExprNodeList, extensionNode, context, operatorId)
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

  override protected def withNewChildInternal(newChild: SparkPlan): ProjectExecTransformer =
    copy(child = newChild)
}

// An alternatives for UnionExec.
case class UnionExecTransformer(children: Seq[SparkPlan]) extends SparkPlan {
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

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]
                                                ): UnionExecTransformer =
    copy(children = newChildren)

  def columnarInputRDD: RDD[ColumnarBatch] = {
    if (children.size == 0) {
      throw new IllegalArgumentException(s"Empty children")
    }
    children.map {
      case c => Seq(c.executeColumnar())
    }.reduce {
      (a, b) => a ++ b
    }.reduce(
      (a, b) => a.union(b)
    )
  }

  protected override def doExecute()
  : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = columnarInputRDD

  def doValidate(): Boolean = true
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
      // transform BroadcastExchangeExec to ColumnarBroadcastExchangeExec in partitionFilters
      val newPartitionFilters =
        ExpressionConverter.transformDynamicPruningExpr(fileSourceScan.partitionFilters)
      new FileSourceScanExecTransformer(
        fileSourceScan.relation,
        fileSourceScan.output,
        fileSourceScan.requiredSchema,
        newPartitionFilters,
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
          val newPartitionFilters =
            ExpressionConverter.transformDynamicPruningExpr(scan.partitionFilters)
          new BatchScanExecTransformer(batchScan.output, scan,
            leftFilters ++ newPartitionFilters)
        case _ =>
          throw new UnsupportedOperationException(
            s"${batchScan.scan.getClass.toString} is not supported.")
      }
    case other =>
      throw new UnsupportedOperationException(s"${other.getClass.toString} is not supported.")
  }
}
