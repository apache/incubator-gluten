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
import io.glutenproject.substrait.rel.RelBuilder

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression}
import org.apache.spark.sql.execution.SparkPlan

import java.util

import scala.collection.JavaConverters._

case class FilterExecTransformer(condition: Expression, child: SparkPlan)
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
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
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
        context,
        leftCondition,
        child.output,
        operatorId,
        childCtx.root,
        validation = false)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute](child.output.asJava)
      getRelNode(
        context,
        leftCondition,
        child.output,
        operatorId,
        RelBuilder.makeReadRel(attrList, context, operatorId),
        validation = false)
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
      val leftFilters =
        FilterHandler.getLeftFilters(scanFilters, FilterHandler.flattenCondition(condition))
      leftFilters.reduceLeftOption(And).orNull
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): FilterExecTransformer =
    copy(child = newChild)
}
