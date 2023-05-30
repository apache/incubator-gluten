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
import io.glutenproject.GlutenConfig
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.RelBuilder
import io.glutenproject.vectorized.GlutenNativeExpressionEvaluator

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression}
import org.apache.spark.sql.execution.SparkPlan

case class GlutenFilterExecTransformer(condition: Expression,
                                      child: SparkPlan)
  extends FilterExecBaseTransformer(condition, child) with TransformSupport {

  override def doValidateInternal(): Boolean = {
    val leftCondition = getLeftCondition
    if (leftCondition == null) {
      // All the filters can be pushed down and the computing of this Filter
      // is not needed.
      return true
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val relNode = try {
      getRelNode(
        substraitContext, leftCondition, child.output, operatorId, null, validation = true)
    } catch {
      case e: Throwable =>
        logValidateFailure(
          s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}", e)
        return false
    }
    val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
    // Then, validate the generated plan in native engine.
    if (GlutenConfig.getConf.enableNativeValidation) {
      val validator = new GlutenNativeExpressionEvaluator()
      validator.doValidate(planNode.toProtobuf.toByteArray)
    } else {
      true
    }
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val leftCondition = getLeftCondition
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }

    val operatorId = context.nextOperatorId(this.nodeName)
    if (leftCondition == null) {
      // The computing for this filter is not needed.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val currRel = if (childCtx != null) {
      getRelNode(
        context, leftCondition, child.output, operatorId, childCtx.root, validation = false)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute](child.output.asJava)
      getRelNode(context, leftCondition, child.output, operatorId,
        RelBuilder.makeReadRel(attrList, context, operatorId), validation = false)
    }
    assert(currRel != null, "Filter rel should be valid.")
    val inputAttributes = if (childCtx != null) {
      // Use the outputAttributes of child context as inputAttributes.
      childCtx.outputAttributes
    } else {
      child.output
    }
    TransformContext(inputAttributes, output, currRel)
  }

  private def getLeftCondition: Expression = {
    val scanFilters = child match {
      // Get the filters including the manually pushed down ones.
      case batchScanTransformer: BatchScanExecTransformer =>
        batchScanTransformer.filterExprs()
      case fileScanTransformer: FileSourceScanExecTransformer =>
        fileScanTransformer.filterExprs()
      // For fallback scan, we need to keep original filter.
      case _ =>
        Seq.empty[Expression]
    }
    if (scanFilters.isEmpty) {
      condition
    } else {
      val leftFilters = FilterHandler.getLeftFilters(
        scanFilters, FilterHandler.flattenCondition(condition))
      leftFilters.reduceLeftOption(And).orNull
    }
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): GlutenFilterExecTransformer = copy(child = newChild)
}
