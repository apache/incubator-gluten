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

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.extension.{GlutenPlan, ValidationResult}
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanExecBase, FileScan}
import org.apache.spark.sql.utils.StructTypeFWD
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

abstract class FilterExecTransformerBase(val cond: Expression, val input: SparkPlan)
  extends UnaryTransformSupport
  with OrderPreservingNodeShim
  with PartitioningPreservingNodeShim
  with PredicateHelper
  with Logging {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetrics(sparkContext)

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(cond).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFilterTransformerMetricsUpdater(metrics)

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
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeFilterRel(input, condExprNode, extensionNode, context, operatorId)
    }
  }

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

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = child.output

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
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    if (cond == null && childCtx != null) {
      // The computing for this filter is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val currRel =
      getRelNode(context, cond, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Filter rel should be valid.")
    TransformContext(childCtx.outputAttributes, output, currRel)
  }
}

case class ProjectExecTransformer private (
    projectList: Seq[NamedExpression],
    child: SparkPlan,
    isPostProject: Boolean = false)
  extends UnaryTransformSupport
  with OrderPreservingNodeShim
  with PartitioningPreservingNodeShim
  with PredicateHelper
  with Logging {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetrics(sparkContext)

  override def nodeName: String = if (isPostProject) {
    "Post" + super.nodeName
  } else {
    super.nodeName
  }

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode =
      getRelNode(
        substraitContext,
        projectList,
        getInputAttributes(child),
        operatorId,
        null,
        validation = true)
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetricsUpdater(metrics)

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    if ((projectList == null || projectList.isEmpty) && childCtx != null) {
      // The computing for this project is not needed.
      // the child may be an input adapter and childCtx is null. In this case we want to
      // make a read node with non-empty base_schema.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val currRel =
      getRelNode(
        context,
        projectList,
        getInputAttributes(child),
        operatorId,
        childCtx.root,
        validation = false)
    assert(currRel != null, "Project Rel should be valid")
    TransformContext(childCtx.outputAttributes, output, currRel)
  }

  private def getInputAttributes(child: SparkPlan) = child match {
    case agg: HashAggregateExecBaseTransformer if isPostProject =>
      agg.allAggregateResultAttributes
    case _ => child.output
  }

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  def getRelNode(
      context: SubstraitContext,
      projectList: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    val columnarProjExprs: Seq[ExpressionTransformer] = ExpressionConverter
      .replaceWithExpressionTransformer(projectList, attributeSeq = originalInputAttributes)
    val projExprNodeList = columnarProjExprs.map(_.doTransform(args)).asJava
    val emitStartIndex = originalInputAttributes.size
    if (!validation) {
      RelBuilder.makeProjectRel(input, projExprNodeList, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        input,
        projExprNodeList,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): ProjectExecTransformer =
    copy(child = newChild)
}
object ProjectExecTransformer {
  private def processProjectExecTransformer(
      projectList: Seq[NamedExpression]): Seq[NamedExpression] = {

    // When there is a MergeScalarSubqueries which will create the named_struct with the same name,
    // looks like {'bloomFilter', BF1, 'bloomFilter', BF2}
    // or {'count(1)', count(1)#111L, 'avg(a)', avg(a)#222L, 'count(1)', count(1)#333L},
    // it will cause problem for some backends, e.g. ClickHouse,
    // which cannot tolerate duplicate type names in struct type,
    // so we need to rename 'nameExpr' in the named_struct to make them unique
    // after executing the MergeScalarSubqueries.
    var needToReplace = false
    val newProjectList = projectList.map {
      case alias @ Alias(cns @ CreateNamedStruct(children: Seq[Expression]), "mergedValue") =>
        // check whether there are some duplicate names
        if (cns.nameExprs.distinct.size == cns.nameExprs.size) {
          alias
        } else {
          val newChildren = children
            .grouped(2)
            .flatMap {
              case Seq(name: Literal, value: NamedExpression) =>
                val newLiteral = Literal(name.toString() + "#" + value.exprId.id)
                Seq(newLiteral, value)
              case Seq(name, value) => Seq(name, value)
            }
            .toSeq
          needToReplace = true
          Alias.apply(CreateNamedStruct(newChildren), "mergedValue")(alias.exprId)
        }
      case other: NamedExpression => other
    }

    if (!needToReplace) {
      projectList
    } else {
      newProjectList
    }
  }

  def apply(projectList: Seq[NamedExpression], child: SparkPlan): ProjectExecTransformer =
    new ProjectExecTransformer(processProjectExecTransformer(projectList), child)
}

// An alternatives for UnionExec.
case class UnionExecTransformer(children: Seq[SparkPlan]) extends SparkPlan with GlutenPlan {
  children.foreach(
    child =>
      child match {
        case w: WholeStageTransformer =>
          w.setOutputSchemaForPlan(output)
        case _ =>
      })

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
object FilterHandler extends PredicateHelper {

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
   * Compare the semantics of the filter conditions pushed down to Scan and in the Filter.
   *
   * @param scanFilters
   *   : the conditions pushed down into Scan
   * @param filters
   *   : the conditions in the Filter after the Scan
   * @return
   *   the filter conditions not pushed down into Scan.
   */
  def getRemainingFilters(scanFilters: Seq[Expression], filters: Seq[Expression]): Seq[Expression] =
    (ExpressionSet(filters) -- ExpressionSet(scanFilters)).toSeq

  // Separate and compare the filter conditions in Scan and Filter.
  // Push down the remaining conditions in Filter into Scan.
  def applyFilterPushdownToScan(filter: FilterExec, reuseSubquery: Boolean): SparkPlan =
    filter.child match {
      case fileSourceScan: FileSourceScanExec =>
        val remainingFilters =
          getRemainingFilters(
            fileSourceScan.dataFilters,
            splitConjunctivePredicates(filter.condition))
        ScanTransformerFactory.createFileSourceScanTransformer(
          fileSourceScan,
          reuseSubquery,
          extraFilters = remainingFilters)
      case batchScan: BatchScanExec =>
        val remainingFilters = batchScan.scan match {
          case fileScan: FileScan =>
            getRemainingFilters(fileScan.dataFilters, splitConjunctivePredicates(filter.condition))
          case _ =>
            // TODO: For data lake format use pushedFilters in SupportsPushDownFilters
            splitConjunctivePredicates(filter.condition)
        }
        ScanTransformerFactory.createBatchScanTransformer(
          batchScan,
          reuseSubquery,
          pushdownFilters = remainingFilters)
      case other =>
        throw new UnsupportedOperationException(s"${other.getClass.toString} is not supported.")
    }
}
