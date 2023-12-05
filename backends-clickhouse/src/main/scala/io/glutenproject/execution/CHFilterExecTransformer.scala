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

import io.glutenproject.extension.ValidationResult
import io.glutenproject.substrait.SubstraitContext

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.execution.SparkPlan

case class CHFilterExecTransformer(condition: Expression, child: SparkPlan)
  extends FilterExecTransformerBase(condition, child) {

  override protected def doValidateInternal(): ValidationResult = {
    val leftCondition = getLeftCondition
    if (leftCondition == null) {
      // All the filters can be pushed down and the computing of this Filter
      // is not needed.
      return ValidationResult.ok
    }
    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val relNode =
      getRelNode(substraitContext, leftCondition, child.output, operatorId, null, validation = true)
    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)
    val leftCondition = getLeftCondition

    val operatorId = context.nextOperatorId(this.nodeName)
    if (leftCondition == null) {
      // The computing for this filter is not needed.
      context.registerEmptyRelToOperator(operatorId)
      // Since some columns' nullability will be removed after this filter, we need to update the
      // outputAttributes of child context.
      TransformContext(childCtx.inputAttributes, output, childCtx.root)
    }

    val currRel = getRelNode(
      context,
      leftCondition,
      child.output,
      operatorId,
      childCtx.root,
      validation = false)
    assert(currRel != null, "Filter rel should be valid.")
    TransformContext(childCtx.outputAttributes, output, currRel)
  }

  private def getLeftCondition: Expression = {
    val scanFilters = child match {
      // Get the filters including the manually pushed down ones.
      case basicScanTransformer: BasicScanExecTransformer =>
        basicScanTransformer.filterExprs()
      // In ColumnarGuardRules, the child is still row-based. Need to get the original filters.
      case _ =>
        FilterHandler.getScanFilters(child)
    }
    if (scanFilters.isEmpty) {
      condition
    } else {
      val leftFilters =
        FilterHandler.getLeftFilters(scanFilters, FilterHandler.flattenCondition(condition))
      leftFilters.reduceLeftOption(And).orNull
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CHFilterExecTransformer =
    copy(child = newChild)
}

case class FilterExecTransformer(condition: Expression, child: SparkPlan)
  extends FilterExecTransformerBase(condition, child)
  with TransformSupport {
  override protected def withNewChildInternal(newChild: SparkPlan): FilterExecTransformer =
    copy(child = newChild)
}
