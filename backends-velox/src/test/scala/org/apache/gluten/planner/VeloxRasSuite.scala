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
package org.apache.gluten.planner

import org.apache.gluten.planner.property.Conventions
import org.apache.gluten.ras.Best.BestNotFoundException
import org.apache.gluten.ras.Ras
import org.apache.gluten.ras.RasSuiteBase._
import org.apache.gluten.ras.path.RasPath
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class VeloxRasSuite extends SharedSparkSession {
  import VeloxRasSuite._

  test("C2R, R2C - basic") {
    val in = RowUnary(RowLeaf(TRIVIAL_SCHEMA))
    val planner = newRas().newPlanner(in)
    val out = planner.plan()
    assert(out == RowUnary(RowLeaf(TRIVIAL_SCHEMA)))
  }

  test("C2R, R2C - explicitly requires any properties") {
    val in = RowUnary(RowLeaf(TRIVIAL_SCHEMA))
    val planner =
      newRas().newPlanner(in, PropertySet(List(Conventions.ANY)))
    val out = planner.plan()
    assert(out == RowUnary(RowLeaf(TRIVIAL_SCHEMA)))
  }

  test("C2R, R2C - requires columnar output") {
    val in = RowUnary(RowLeaf(TRIVIAL_SCHEMA))
    val planner =
      newRas().newPlanner(in, PropertySet(List(Conventions.VANILLA_COLUMNAR)))
    val out = planner.plan()
    assert(out == RowToColumnarExec(RowUnary(RowLeaf(TRIVIAL_SCHEMA))))
  }

  test("C2R, R2C - insert c2rs / r2cs") {
    val in =
      ColumnarUnary(
        RowUnary(
          RowUnary(ColumnarUnary(RowUnary(RowUnary(ColumnarUnary(RowLeaf(TRIVIAL_SCHEMA))))))))
    val planner =
      newRas().newPlanner(in, PropertySet(List(Conventions.ROW_BASED)))
    val out = planner.plan()
    assert(
      out == ColumnarToRowExec(
        ColumnarUnary(RowToColumnarExec(
          RowUnary(RowUnary(ColumnarToRowExec(ColumnarUnary(RowToColumnarExec(RowUnary(RowUnary(
            ColumnarToRowExec(ColumnarUnary(RowToColumnarExec(RowLeaf(TRIVIAL_SCHEMA)))))))))))))))
    val paths = planner.newState().memoState().collectAllPaths(RasPath.INF_DEPTH).toList
    val pathCount = paths.size
    assert(pathCount == 165)
  }

  test("C2R, R2C - Row unary convertible to Columnar") {
    object ConvertRowUnaryToColumnar extends RasRule[SparkPlan] {
      override def shift(node: SparkPlan): Iterable[SparkPlan] = node match {
        case RowUnary(child) => List(ColumnarUnary(child))
        case other => List.empty
      }

      override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
    }

    val in =
      ColumnarUnary(
        RowUnary(
          RowUnary(ColumnarUnary(RowUnary(RowUnary(ColumnarUnary(RowLeaf(TRIVIAL_SCHEMA))))))))
    val planner =
      newRas(List(ConvertRowUnaryToColumnar))
        .newPlanner(in, PropertySet(List(Conventions.ROW_BASED)))
    val out = planner.plan()
    assert(out == ColumnarToRowExec(ColumnarUnary(ColumnarUnary(ColumnarUnary(ColumnarUnary(
      ColumnarUnary(ColumnarUnary(ColumnarUnary(RowToColumnarExec(RowLeaf(TRIVIAL_SCHEMA)))))))))))
    val paths = planner.newState().memoState().collectAllPaths(RasPath.INF_DEPTH).toList
    val pathCount = paths.size
    assert(pathCount == 1094)
  }

  test("C2R, R2C - empty schema") {
    val in = RowUnary(RowLeaf(EMPTY_SCHEMA))

    val planner =
      newRas().newPlanner(in, PropertySet(List(Conventions.ANY)))
    val out = planner.plan()
    assert(out == RowUnary(RowLeaf(EMPTY_SCHEMA)))

    assertThrows[BestNotFoundException] {
      // Could not optimize to columnar output since R2C transitions for empty schema node
      // is not allowed.
      val planner2 =
        newRas().newPlanner(in, PropertySet(List(Conventions.VANILLA_COLUMNAR)))
      planner2.plan()
    }
  }
}

object VeloxRasSuite {
  def newRas(): Ras[SparkPlan] = {
    GlutenOptimization(List()).asInstanceOf[Ras[SparkPlan]]
  }

  def newRas(RasRules: Seq[RasRule[SparkPlan]]): Ras[SparkPlan] = {
    GlutenOptimization(RasRules).asInstanceOf[Ras[SparkPlan]]
  }

  val TRIVIAL_SCHEMA: Seq[AttributeReference] = List(AttributeReference("value", StringType)())

  val EMPTY_SCHEMA: Seq[AttributeReference] = List.empty

  case class RowLeaf(override val output: Seq[Attribute]) extends LeafExecNode {
    override def supportsColumnar: Boolean = false
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  }

  case class RowUnary(child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = false
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): RowUnary =
      copy(child = newChild)
  }

  case class ColumnarUnary(child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = true
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): ColumnarUnary =
      copy(child = newChild)
  }
}
