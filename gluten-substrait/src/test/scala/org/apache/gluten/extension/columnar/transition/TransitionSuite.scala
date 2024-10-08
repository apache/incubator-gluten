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
import org.apache.gluten.execution.ColumnarToColumnarExec
import org.apache.gluten.extension.GlutenPlan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.vectorized.ColumnarBatch

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

object TransitionSuite extends TransitionSuiteBase {
  object TypeA extends Convention.BatchType {
    fromRow(RowToBatch(this, _))
    toRow(BatchToRow(this, _))
  }

  object TypeB extends Convention.BatchType {
    fromRow(RowToBatch(this, _))
    toRow(BatchToRow(this, _))
  }

  object TypeC extends Convention.BatchType {
    fromRow(RowToBatch(this, _))
    toRow(BatchToRow(this, _))
    fromBatch(TypeA, BatchToBatch(TypeA, this, _))
    toBatch(TypeA, BatchToBatch(this, TypeA, _))
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
    extends ColumnarToColumnarExec(from, to) {
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] =
      throw new UnsupportedOperationException()
  }

}
