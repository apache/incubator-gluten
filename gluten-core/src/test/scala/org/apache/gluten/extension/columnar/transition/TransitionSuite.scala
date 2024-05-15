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

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.GlutenPlan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution._
import org.apache.spark.sql.test.SharedSparkSession

class TransitionSuite extends SharedSparkSession {
  import TransitionSuite._
  test("Trivial C2R") {
    val in = BatchLeaf(TypeA)
    val out = ConventionFunc.ignoreBackend {
      Transitions.insertTransitions(in, outputsColumnar = false)
    }
    assert(out == BatchToRow(TypeA, BatchLeaf(TypeA)))
  }

  test("Insert C2R") {
    val in = RowUnary(BatchLeaf(TypeA))
    val out = ConventionFunc.ignoreBackend {
      Transitions.insertTransitions(in, outputsColumnar = false)
    }
    assert(out == RowUnary(BatchToRow(TypeA, BatchLeaf(TypeA))))
  }

  test("Insert R2C") {
    val in = BatchUnary(TypeA, RowLeaf())
    val out = ConventionFunc.ignoreBackend {
      Transitions.insertTransitions(in, outputsColumnar = false)
    }
    assert(out == BatchToRow(TypeA, BatchUnary(TypeA, RowToBatch(TypeA, RowLeaf()))))
  }

  test("Insert C2R2C") {
    val in = BatchUnary(TypeA, BatchLeaf(TypeB))
    val out = ConventionFunc.ignoreBackend {
      Transitions.insertTransitions(in, outputsColumnar = false)
    }
    assert(
      out == BatchToRow(
        TypeA,
        BatchUnary(TypeA, RowToBatch(TypeA, BatchToRow(TypeB, BatchLeaf(TypeB))))))
  }

  test("Insert C2C") {
    val in = BatchUnary(TypeA, BatchLeaf(TypeC))
    val out = ConventionFunc.ignoreBackend {
      Transitions.insertTransitions(in, outputsColumnar = false)
    }
    assert(
      out == BatchToRow(
        TypeA,
        BatchUnary(TypeA, BatchToBatch(from = TypeC, to = TypeA, BatchLeaf(TypeC)))))
  }

  test("No transitions found") {
    val in = BatchUnary(TypeA, BatchLeaf(TypeD))
    assertThrows[GlutenException] {
      ConventionFunc.ignoreBackend {
        Transitions.insertTransitions(in, outputsColumnar = false)
      }
    }
  }
}

object TransitionSuite {
  object TypeA extends Convention.BatchType {
    fromRow(
      () =>
        (plan: SparkPlan) => {
          RowToBatch(this, plan)
        })

    toRow(
      () =>
        (plan: SparkPlan) => {
          BatchToRow(this, plan)
        })
  }

  object TypeB extends Convention.BatchType {
    fromRow(
      () =>
        (plan: SparkPlan) => {
          RowToBatch(this, plan)
        })

    toRow(
      () =>
        (plan: SparkPlan) => {
          BatchToRow(this, plan)
        })
  }

  object TypeC extends Convention.BatchType {
    fromRow(
      () =>
        (plan: SparkPlan) => {
          RowToBatch(this, plan)
        })

    toRow(
      () =>
        (plan: SparkPlan) => {
          BatchToRow(this, plan)
        })

    fromBatch(
      TypeA,
      () =>
        (plan: SparkPlan) => {
          BatchToBatch(TypeA, this, plan)
        })

    toBatch(
      TypeA,
      () =>
        (plan: SparkPlan) => {
          BatchToBatch(this, TypeA, plan)
        })
  }

  object TypeD extends Convention.BatchType {}

  case class RowToBatch(toBatchType: Convention.BatchType, override val child: SparkPlan)
    extends RowToColumnarTransition
    with GlutenPlan {
    override def supportsColumnar: Boolean = true
    override protected def batchType0(): Convention.BatchType = toBatchType
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
  }

  case class BatchToRow(fromBatchType: Convention.BatchType, override val child: SparkPlan)
    extends ColumnarToRowTransition {
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
  }

  case class BatchToBatch(
      from: Convention.BatchType,
      to: Convention.BatchType,
      override val child: SparkPlan)
    extends UnaryExecNode
    with GlutenPlan {
    override def supportsColumnar: Boolean = true
    override protected def batchType0(): Convention.BatchType = to
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
  }

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
