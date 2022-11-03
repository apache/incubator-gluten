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
package io.substrait.spark

import io.substrait.spark.logical.SubstraitRelVisitor

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType}

import io.substrait.relation

case class testRecord(A: Int, B: Long)

class PlannerTest extends SharedSparkSession {
  private val testRelation = LocalRelation($"a".int, $"b".long, $"c".int)

  test("simple") {

    val a = AttributeReference("A", IntegerType)()
    val b = AttributeReference("B", LongType)()
    val planUppercase = Project(
      Seq(Alias(Add(a, b), "x")()),
      LocalRelation.fromProduct(Seq(a, b), Seq(testRecord(1, 2L))))

    val qe = new QueryExecution(spark, planUppercase)
    val plan = qe.optimizedPlan
    val x = plan.expressions
    logInfo(plan.treeString)
  }

  test("VirtualTableScan") {
    val a = AttributeReference("A", IntegerType)()
    val b = AttributeReference("B", LongType)()
    val convert = new SubstraitRelVisitor()
    val x = convert.visit(LocalRelation.fromProduct(Seq(a, b), Seq(testRecord(1, 2L))))
    assert(x.isInstanceOf[relation.VirtualTableScan])
  }
}
