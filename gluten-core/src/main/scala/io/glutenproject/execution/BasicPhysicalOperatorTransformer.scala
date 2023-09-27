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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.extension.{GlutenPlan, ValidationResult}
import io.glutenproject.extension.columnar.TransformHints
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanExecBase, FileScan}
import org.apache.spark.sql.utils.StructTypeFWD
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.Any

import java.util

import scala.collection.JavaConverters._

abstract class FilterExecTransformerBase(val cond: Expression, val input: SparkPlan)
  extends UnaryExecNode
  with TransformSupport
  with PredicateHelper
  with AliasAwareOutputPartitioning
  with Logging {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetrics(sparkContext)

  val sparkConf: SparkConf = sparkContext.getConf

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(cond).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

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

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetricsUpdater(metrics)

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  def getRelNode(
      context: SubstraitContext,
      condExpr: Expression,
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    if (condExpr == null) {
      return input
    }
    val condExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(condExpr, attributeSeq = originalInputAttributes)
      .doTransform(args)

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
    child.output.map {
      a =>
        if (a.nullable && notNullAttributes.contains(a.exprId)) {
          a.withNullability(false)
        } else {
          a
        }
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (cond == null) {
      // The computing of this Filter is not needed.
      return ValidationResult.ok
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val relNode =
      getRelNode(substraitContext, cond, child.output, operatorId, null, validation = true)
    // For now backends only support scan + filter pattern
    if (BackendsApiManager.getSettings.fallbackFilterWithoutConjunctiveScan()) {
      if (
        !(child.isInstanceOf[DataSourceScanExec] ||
          child.isInstanceOf[DataSourceV2ScanExecBase])
      ) {
        return ValidationResult
          .notOk("Filter operator's child is not DataSourceScanExec or DataSourceV2ScanExecBase")
      }
    }

    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }

    val operatorId = context.nextOperatorId(this.nodeName)
    if (cond == null && childCtx != null) {
      // The computing for this filter is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val currRel = if (childCtx != null) {
      getRelNode(context, cond, child.output, operatorId, childCtx.root, validation = false)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute](child.output.asJava)
      getRelNode(
        context,
        cond,
        child.output,
        operatorId,
        RelBuilder.makeReadRel(attrList, context, operatorId),
        validation = false)
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

  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }
}

case class ProjectExecTransformer private (projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode
  with TransformSupport
  with PredicateHelper
  with AliasAwareOutputPartitioning
  with Logging {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetrics(sparkContext)

  val sparkConf: SparkConf = sparkContext.getConf

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    val relNode =
      getRelNode(substraitContext, projectList, child.output, operatorId, null, validation = true)
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
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

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetricsUpdater(metrics)

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }
    val operatorId = context.nextOperatorId(this.nodeName)
    if ((projectList == null || projectList.isEmpty) && childCtx != null) {
      // The computing for this project is not needed.
      // the child may be an input adapter and childCtx is null. In this case we want to
      // make a read node with non-empty base_schema.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val (currRel, inputAttributes) = if (childCtx != null) {
      (
        getRelNode(
          context,
          projectList,
          child.output,
          operatorId,
          childCtx.root,
          validation = false),
        childCtx.outputAttributes)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)
      (
        getRelNode(context, projectList, child.output, operatorId, readRel, validation = false),
        child.output)
    }
    assert(currRel != null, "Project Rel should be valid")

    TransformContext(inputAttributes, output, currRel)
  }

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  // override def canEqual(that: Any): Boolean = false

  def getRelNode(
      context: SubstraitContext,
      projectList: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    val columnarProjExprs: Seq[ExpressionTransformer] = projectList.map(
      expr => {
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, attributeSeq = originalInputAttributes)
      })
    val projExprNodeList = new java.util.ArrayList[ExpressionNode]()
    for (expr <- columnarProjExprs) {
      projExprNodeList.add(expr.doTransform(args))
    }
    val emitStartIndex = originalInputAttributes.size
    if (!validation) {
      RelBuilder.makeProjectRel(input, projExprNodeList, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        input,
        projExprNodeList,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ProjectExecTransformer =
    copy(child = newChild)
}
object ProjectExecTransformer {
  private def processProjectExecTransformer(
      project: ProjectExecTransformer): ProjectExecTransformer = {
    // Special treatment for Project containing all bloom filters:
    // If more than one bloom filter, Spark will merge them into a named_struct
    // and then broadcast. The named_struct looks like {'bloomFilter',BF1,'bloomFilter',BF2},
    // with two bloom filters sharing same name. This will cause problem for some backends,
    // e.g. ClickHouse, which cannot tolerate duplicate type names in struct type
    // So we need to rename 'bloomFilter' to make them unique.
    if (project.projectList.size == 1) {
      val newProjectListHead = project.projectList.head match {
        case alias @ Alias(cns @ CreateNamedStruct(children: Seq[Expression]), "mergedValue") =>
          if (
            !cns.nameExprs.forall(
              e =>
                e.isInstanceOf[Literal] && "bloomFilter".equals(e.asInstanceOf[Literal].toString()))
          ) {
            null
          } else {
            val newChildren = children.zipWithIndex.map {
              case _ @(_: Literal, index) =>
                val newLiteral = Literal("bloomFilter" + index / 2)
                newLiteral
              case other @ (_, _) => other._1
            }
            Alias.apply(CreateNamedStruct(newChildren), "mergedValue")(alias.exprId)
          }
        case _ => null
      }
      if (newProjectListHead == null) {
        project
      } else {
        ProjectExecTransformer(Seq(newProjectListHead), project.child)
      }
    } else {
      project
    }
  }

  def apply(projectList: Seq[NamedExpression], child: SparkPlan): ProjectExecTransformer =
    processProjectExecTransformer(new ProjectExecTransformer(projectList, child))
}

// An alternatives for UnionExec.
case class UnionExecTransformer(children: Seq[SparkPlan]) extends SparkPlan with GlutenPlan {
  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map {
      attrs =>
        val firstAttr = attrs.head
        val nullable = attrs.exists(_.nullable)
        val newDt = attrs.map(_.dataType).reduce(StructTypeFWD.unionLikeMerge)
        if (firstAttr.dataType == newDt) {
          firstAttr.withNullability(nullable)
        } else {
          AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
            firstAttr.exprId,
            firstAttr.qualifier)
        }
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): UnionExecTransformer =
    copy(children = newChildren)

  def columnarInputRDD: RDD[ColumnarBatch] = {
    if (children.isEmpty) {
      throw new IllegalArgumentException(s"Empty children")
    }
    children
      .map(c => Seq(c.executeColumnar()))
      .reduce((a, b) => a ++ b)
      .reduce((a, b) => a.union(b))
  }

  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = columnarInputRDD

  override protected def doValidateInternal(): ValidationResult = {
    if (!BackendsApiManager.getValidatorApiInstance.doSchemaValidate(schema)) {
      return ValidationResult.notOk(s"Found schema check failure for $schema in $nodeName")
    }
    ValidationResult.ok
  }
}

/**
 * Contains functions for the comparision and separation of the filter conditions in Scan and
 * Filter. Contains the function to manually push down the conditions into Scan.
 */
object FilterHandler {

  /**
   * Get the original filter conditions in Scan for the comparison with those in Filter.
   *
   * @param plan
   *   : the Spark plan
   * @return
   *   If the plan is FileSourceScanExec or BatchScanExec, return the filter conditions in it.
   *   Otherwise, return empty sequence.
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

  /**
   * Flatten the condition connected with 'And'. Return the filter conditions with sequence.
   *
   * @param condition
   *   : the condition connected with 'And'
   * @return
   *   flattened conditions in sequence
   */
  def flattenCondition(condition: Expression): Seq[Expression] = {
    var expressions: Seq[Expression] = Seq()
    condition match {
      case and: And =>
        and.children.foreach(
          expression => {
            expressions ++= flattenCondition(expression)
          })
      case _ =>
        expressions = expressions :+ condition
    }
    expressions
  }

  /**
   * Compare the semantics of the filter conditions pushed down to Scan and in the Filter.
   *
   * @param scanFilters
   *   : the conditions pushed down into Scan
   * @param filters
   *   : the conditions in the Filter after the Scan
   * @return
   *   the filter conditions not pushed down into Scan.
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
  def applyFilterPushdownToScan(plan: FilterExec, reuseSubquery: Boolean): SparkPlan =
    plan.child match {
      case fileSourceScan: FileSourceScanExec =>
        val leftFilters =
          getLeftFilters(fileSourceScan.dataFilters, flattenCondition(plan.condition))
        // transform BroadcastExchangeExec to ColumnarBroadcastExchangeExec in partitionFilters
        val newPartitionFilters =
          ExpressionConverter.transformDynamicPruningExpr(
            fileSourceScan.partitionFilters,
            reuseSubquery)
        new FileSourceScanExecTransformer(
          fileSourceScan.relation,
          fileSourceScan.output,
          fileSourceScan.requiredSchema,
          newPartitionFilters,
          fileSourceScan.optionalBucketSet,
          fileSourceScan.optionalNumCoalescedBuckets,
          fileSourceScan.dataFilters ++ leftFilters,
          fileSourceScan.tableIdentifier,
          fileSourceScan.disableBucketedScan
        )
      case batchScan: BatchScanExec =>
        batchScan.scan match {
          case scan: FileScan =>
            val leftFilters =
              getLeftFilters(scan.dataFilters, flattenCondition(plan.condition))
            val newPartitionFilters =
              ExpressionConverter.transformDynamicPruningExpr(scan.partitionFilters, reuseSubquery)
            new BatchScanExecTransformer(batchScan.output, scan, leftFilters ++ newPartitionFilters)
          case _ =>
            if (batchScan.runtimeFilters.isEmpty) {
              throw new UnsupportedOperationException(
                s"${batchScan.scan.getClass.toString} is not supported.")
            } else {
              // IF filter expressions aren't empty, we need to transform the inner operators.
              val newSource = batchScan.copy(runtimeFilters = ExpressionConverter
                .transformDynamicPruningExpr(batchScan.runtimeFilters, reuseSubquery))
              TransformHints.tagNotTransformable(
                newSource,
                "The scan in BatchScanExec is not a FileScan")
              newSource
            }
        }
      case other =>
        throw new UnsupportedOperationException(s"${other.getClass.toString} is not supported.")
    }
}
