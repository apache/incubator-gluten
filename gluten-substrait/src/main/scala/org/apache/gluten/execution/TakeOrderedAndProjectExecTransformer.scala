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

import org.apache.gluten.extension.{GlutenPlan, ValidationResult}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, SparkPlan, UnaryExecNode}

// The node is used for fallback validation, will be rewritten before execution.
case class TakeOrderedAndProjectExecTransformer(
    limit: Long,
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan,
    offset: Int = 0)
  extends UnaryExecNode
  with GlutenPlan {
  override def outputPartitioning: Partitioning = SinglePartition
  override def outputOrdering: Seq[SortOrder] = sortOrder
  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"TakeOrderedAndProjectExecTransformer (limit=$limit, " +
      s"orderBy=$orderByString, output=$outputString)"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (offset != 0) {
      return ValidationResult.failed(s"Native TopK does not support offset: $offset")
    }

    var tagged: ValidationResult = null
    val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, sortOrder)
    if (orderingSatisfies) {
      val limitPlan = LimitTransformer(child, offset, limit)
      tagged = limitPlan.doValidate()
    } else {
      // Here we are validating sort + limit which is a kind of whole stage transformer,
      // because we would call sort.doTransform in limit.
      // So, we should add adapter to make it work.
      val inputTransformer =
        ColumnarCollapseTransformStages.wrapInputIteratorTransformer(child)
      val sortPlan = SortExecTransformer(sortOrder, false, inputTransformer)
      val sortValidation = sortPlan.doValidate()
      if (!sortValidation.ok()) {
        return sortValidation
      }
      val limitPlan = LimitTransformer(sortPlan, offset, limit)
      tagged = limitPlan.doValidate()
    }

    if (tagged.ok()) {
      val projectPlan = ProjectExecTransformer(projectList, child)
      tagged = projectPlan.doValidate()
    }
    tagged
  }
}
