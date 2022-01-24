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

package com.intel.oap.execution

import scala.collection.mutable.ListBuffer

import com.google.common.collect.Lists
import com.intel.oap.expression._
import com.intel.oap.substrait.expression.{AggregateFunctionNode, ExpressionBuilder, ExpressionNode}
import com.intel.oap.substrait.rel.{RelBuilder, RelNode}
import com.intel.oap.substrait.SubstraitContext
import com.intel.oap.GazelleJniConfig
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Columnar Based HashAggregateExec.
 */
case class HashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
    extends BaseAggregateExec
    with TransformSupport {

  val sparkConf = sparkContext.getConf

  override def supportsColumnar: Boolean = GazelleJniConfig.getConf.enableColumnarIterator

  val resAttributes: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  // Members declared in org.apache.spark.sql.execution.AliasAwareOutputPartitioning
  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  // Members declared in org.apache.spark.sql.execution.CodegenSupport
  protected def doProduce(ctx: CodegenContext): String = throw new UnsupportedOperationException()

  // Members declared in org.apache.spark.sql.execution.SparkPlan
  protected override def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] =
    throw new UnsupportedOperationException()

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "time in aggregation process"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_hashagg"))

  val numOutputRows = longMetric("numOutputRows")
  val numOutputBatches = longMetric("numOutputBatches")
  val numInputBatches = longMetric("numInputBatches")
  val aggTime = longMetric("aggTime")
  val totalTime = longMetric("processTime")
  numOutputRows.set(0)
  numOutputBatches.set(0)
  numInputBatches.set(0)

  override def doValidate(): Boolean = {
    var isPartial = true
    aggregateExpressions.foreach(aggExpr => {
      aggExpr.mode match {
        case Partial =>
        case _ => isPartial = false
      }
    })
    isPartial
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
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

  override def updateMetrics(out_num_rows: Long, process_time: Long): Unit = {
    val numOutputRows = longMetric("numOutputRows")
    val procTime = longMetric("processTime")
    procTime.set(process_time / 1000000)
    numOutputRows += out_num_rows
  }

  override def getChild: SparkPlan = child

  // override def canEqual(that: Any): Boolean = false

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }
    val (relNode, inputAttributes) = if (childCtx != null) {
      (getAggRel(context.registeredFunction, childCtx.root), childCtx.outputAttributes)
    } else {
      (getAggRel(context.registeredFunction), child.output)
    }
    TransformContext(inputAttributes, output, relNode)
  }

  override def verboseString(maxFields: Int): String = toString(verbose = true, maxFields)

  override def simpleString(maxFields: Int): String = toString(verbose = false, maxFields)

  private def toString(verbose: Boolean, maxFields: Int): String = {
    val allAggregateExpressions = aggregateExpressions
    val keyString = truncatedString(groupingExpressions, "[", ", ", "]", maxFields)
    val functionString = truncatedString(allAggregateExpressions, "[", ", ", "]", maxFields)
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    if (verbose) {
      s"HashAggregateTransformer(keys=$keyString, functions=$functionString, output=$outputString)"
    } else {
      s"HashAggregateTransformer(keys=$keyString, functions=$functionString)"
    }
  }

  private def getAggRel(args: java.lang.Object, input: RelNode = null): RelNode = {
    // Get the grouping idx
    val originalInputAttributes = child.output
    val groupingAttributes = groupingExpressions.map(expr => {
      ConverterUtils.getAttrFromExpr(expr).toAttribute
    })
    val groupingList = new util.ArrayList[ExpressionNode]()

    // Get the aggregate function nodes
    val aggregateFunctionList = new util.ArrayList[AggregateFunctionNode]()
    groupingExpressions.foreach(expr => {
      val groupingExpr: Expression = ExpressionConverter
        .replaceWithExpressionTransformer(expr, originalInputAttributes)
      val exprNode = groupingExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
      groupingList.add(exprNode)
      val outputTypeNode = ConverterUtils.getTypeNode(expr.dataType, expr.nullable)
      val aggFunctionNode = ExpressionBuilder.makeAggregateFunction(
        Lists.newArrayList(exprNode), outputTypeNode)
      aggregateFunctionList.add(aggFunctionNode)
    })
    aggregateExpressions.foreach(aggExpr => {
      val aggregatFunc = aggExpr.aggregateFunction
      val functionId = AggregateFunctionsBuilder.create(args, aggregatFunc)
      val mode = modeToKeyWord(aggExpr.mode)
      val childrenNodeList = new util.ArrayList[ExpressionNode]()
      aggExpr.mode match {
        case Partial =>
          val childrenNodes = aggregatFunc.children.toList.map(expr => {
            val aggExpr: Expression = ExpressionConverter
              .replaceWithExpressionTransformer(expr, originalInputAttributes)
            aggExpr.asInstanceOf[ExpressionTransformer].doTransform(args)
          })
          for (node <- childrenNodes) {
            childrenNodeList.add(node)
          }
        case other =>
      }
      // FIXME: return type of a aggregateFunciton
      val outputTypeNode = ConverterUtils.getTypeNode(
        aggregatFunc.dataType, aggregatFunc.nullable)
      val aggFunctionNode = ExpressionBuilder
        .makeAggregateFunction(functionId, childrenNodeList, mode, outputTypeNode)
      aggregateFunctionList.add(aggFunctionNode)
    })
    // Get Aggregate Rel
    RelBuilder.makeAggregateRel(input, groupingList, aggregateFunctionList)
  }

  private def modeToKeyWord(aggregateMode: AggregateMode): String = {
    aggregateMode match {
      case Partial => "PARTIAL"
      case PartialMerge => "PARTIAL_MERGE"
      case Final => "FINAL"
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
  }

  /**
   * This method calculates the output attributes of Aggregation.
  */
  def getAttrForAggregateExpr(aggregateExpressions: Seq[AggregateExpression],
                              aggregateAttributeList: Seq[Attribute]): List[Attribute] = {
    var aggregateAttr = new ListBuffer[Attribute]()
    val size = aggregateExpressions.size
    var res_index = 0
    for (expIdx <- 0 until size) {
      val exp: AggregateExpression = aggregateExpressions(expIdx)
      val mode = exp.mode
      val aggregateFunc = exp.aggregateFunction
      aggregateFunc match {
        case Average(_) =>
          mode match {
            case Partial =>
              val avg = aggregateFunc.asInstanceOf[Average]
              val aggBufferAttr = avg.inputAggBufferAttributes
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              res_index += 2
            case PartialMerge =>
              val avg = aggregateFunc.asInstanceOf[Average]
              val aggBufferAttr = avg.inputAggBufferAttributes
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              res_index += 1
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Sum(_) =>
          mode match {
            case Partial | PartialMerge =>
              val sum = aggregateFunc.asInstanceOf[Sum]
              val aggBufferAttr = sum.inputAggBufferAttributes
              if (aggBufferAttr.size == 2) {
                // decimal sum check sum.resultType
                val sum_attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
                aggregateAttr += sum_attr
                val isempty_attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(1))
                aggregateAttr += isempty_attr
                res_index += 2
              } else {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
                aggregateAttr += attr
                res_index += 1
              }
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Count(_) =>
          mode match {
            case Partial | PartialMerge =>
              val count = aggregateFunc.asInstanceOf[Count]
              val aggBufferAttr = count.inputAggBufferAttributes
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
              aggregateAttr += attr
              res_index += 1
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Max(_) =>
          mode match {
            case Partial | PartialMerge =>
              val max = aggregateFunc.asInstanceOf[Max]
              val aggBufferAttr = max.inputAggBufferAttributes
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
              aggregateAttr += attr
              res_index += 1
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case Min(_) =>
          mode match {
            case Partial | PartialMerge =>
              val min = aggregateFunc.asInstanceOf[Min]
              val aggBufferAttr = min.inputAggBufferAttributes
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(0))
              aggregateAttr += attr
              res_index += 1
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case StddevSamp(_, _) =>
          mode match {
            case Partial =>
              val stddevSamp = aggregateFunc.asInstanceOf[StddevSamp]
              val aggBufferAttr = stddevSamp.inputAggBufferAttributes
              for (index <- aggBufferAttr.indices) {
                val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
                aggregateAttr += attr
              }
              res_index += 3
            case PartialMerge =>
              throw new UnsupportedOperationException("not currently supported: PartialMerge.")
            case Final =>
              aggregateAttr += aggregateAttributeList(res_index)
              res_index += 1
            case other =>
              throw new UnsupportedOperationException(s"not currently supported: $other.")
          }
        case other =>
          throw new UnsupportedOperationException(s"not currently supported: $other.")
      }
    }
    aggregateAttr.toList
  }
}
