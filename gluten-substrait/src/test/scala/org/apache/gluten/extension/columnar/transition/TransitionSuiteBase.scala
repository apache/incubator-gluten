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
package org.apache.gluten.extension.columnar.transition

import org.apache.gluten.extension.GlutenPlan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{BinaryExecNode, LeafExecNode, SparkPlan, UnaryExecNode}

trait TransitionSuiteBase {
  case class BatchLeaf(override val batchType0: Convention.BatchType)
    extends LeafExecNode
    with GlutenPlan {
    override def supportsColumnar: Boolean = true

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = List.empty
  }

  case class BatchUnary(
      override val batchType0: Convention.BatchType,
      override val child: SparkPlan)
    extends UnaryExecNode
    with GlutenPlan {
    override def supportsColumnar: Boolean = true

    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = child.output
  }

  case class BatchBinary(
      override val batchType0: Convention.BatchType,
      override val left: SparkPlan,
      override val right: SparkPlan)
    extends BinaryExecNode
    with GlutenPlan {
    override def supportsColumnar: Boolean = true

    override protected def withNewChildrenInternal(
        newLeft: SparkPlan,
        newRight: SparkPlan): SparkPlan = copy(left = newLeft, right = newRight)

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = left.output ++ right.output
  }

  case class RowLeaf() extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = List.empty
  }

  case class RowUnary(override val child: SparkPlan) extends UnaryExecNode {
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = child.output
  }

  case class RowBinary(override val left: SparkPlan, override val right: SparkPlan)
    extends BinaryExecNode {
    override protected def withNewChildrenInternal(
        newLeft: SparkPlan,
        newRight: SparkPlan): SparkPlan = copy(left = newLeft, right = newRight)

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()

    override def output: Seq[Attribute] = left.output ++ right.output
  }

}
