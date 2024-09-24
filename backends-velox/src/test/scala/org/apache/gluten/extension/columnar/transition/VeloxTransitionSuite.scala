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

import org.apache.gluten.backendsapi.velox.VeloxListenerApi
import org.apache.gluten.columnarbatch.{ArrowBatch, VeloxBatch}
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.{RowToVeloxColumnarExec, VeloxColumnarToRowExec}
import org.apache.gluten.test.MockVeloxBackend

import org.apache.spark.sql.execution.ColumnarToRowExec
import org.apache.spark.sql.test.SharedSparkSession

class VeloxTransitionSuite extends SharedSparkSession {
  import VeloxTransitionSuite._

  private val api = new VeloxListenerApi()

  test("Arrow C2R - outputs row") {
    val in = BatchLeaf(ArrowBatch)
    val out = Transitions.insertTransitions(in, outputsColumnar = false)
    assert(out == ColumnarToRowExec(BatchLeaf(ArrowBatch)))
  }

  test("Arrow C2R - requires row input") {
    val in = RowUnary(BatchLeaf(ArrowBatch))
    val out = Transitions.insertTransitions(in, outputsColumnar = false)
    assert(out == RowUnary(ColumnarToRowExec(BatchLeaf(ArrowBatch))))
  }

  test("Arrow R2C - requires Arrow input") {
    val in = BatchUnary(ArrowBatch, RowLeaf())
    assertThrows[GlutenException] {
      // No viable transitions.
      // FIXME: Support this case.
      Transitions.insertTransitions(in, outputsColumnar = false)
    }
  }

  test("Velox C2R - outputs row") {
    val in = BatchLeaf(VeloxBatch)
    val out = Transitions.insertTransitions(in, outputsColumnar = false)
    assert(out == VeloxColumnarToRowExec(BatchLeaf(VeloxBatch)))
  }

  test("Velox C2R - requires row input") {
    val in = RowUnary(BatchLeaf(VeloxBatch))
    val out = Transitions.insertTransitions(in, outputsColumnar = false)
    assert(out == RowUnary(VeloxColumnarToRowExec(BatchLeaf(VeloxBatch))))
  }

  test("Velox R2C - outputs Velox") {
    val in = RowLeaf()
    val out = Transitions.insertTransitions(in, outputsColumnar = true)
    assert(out == RowToVeloxColumnarExec(RowLeaf()))
  }

  test("Velox R2C - requires Velox input") {
    val in = BatchUnary(VeloxBatch, RowLeaf())
    val out = Transitions.insertTransitions(in, outputsColumnar = false)
    assert(out == VeloxColumnarToRowExec(BatchUnary(VeloxBatch, RowToVeloxColumnarExec(RowLeaf()))))
  }

  test("Arrow-to-Velox") {
    val in = BatchUnary(VeloxBatch, BatchLeaf(ArrowBatch))
    val out = Transitions.insertTransitions(in, outputsColumnar = false)
    // No explicit transition needed for Arrow-to-Velox.
    // FIXME: Add explicit transitions.
    //  See https://github.com/apache/incubator-gluten/issues/7313.
    assert(out == VeloxColumnarToRowExec(BatchUnary(VeloxBatch, BatchLeaf(ArrowBatch))))
  }

  test("Velox-to-Arrow") {
    val in = BatchUnary(ArrowBatch, BatchLeaf(VeloxBatch))
    assertThrows[GlutenException] {
      // No viable transitions.
      // FIXME: Support this case.
      Transitions.insertTransitions(in, outputsColumnar = false)
    }
  }

  override protected def beforeAll(): Unit = {
    api.onExecutorStart(MockVeloxBackend.mockPluginContext())
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    api.onExecutorShutdown()
  }
}

object VeloxTransitionSuite extends TransitionSuiteBase {}
