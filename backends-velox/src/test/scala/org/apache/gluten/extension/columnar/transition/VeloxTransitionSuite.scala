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

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.{ArrowJavaBatchType, ArrowNativeBatchType}
import org.apache.gluten.backendsapi.velox.{VeloxBatchType, VeloxCarrierRowType, VeloxListenerApi}
import org.apache.gluten.execution._
import org.apache.gluten.extension.columnar.transition.Convention.BatchType.VanillaBatchType
import org.apache.gluten.test.MockVeloxBackend

import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec}
import org.apache.spark.sql.test.SharedSparkSession

class VeloxTransitionSuite extends SharedSparkSession with TransitionSuiteBase {
  import TransitionSuiteBase._

  private val api = new VeloxListenerApi()

  test("Vanilla C2R - outputs row") {
    val in = BatchLeaf(VanillaBatchType)
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(out == ColumnarToRowExec(BatchLeaf(VanillaBatchType)))
  }

  test("Vanilla C2R - requires row input") {
    val in = RowUnary(Convention.RowType.VanillaRowType, BatchLeaf(VanillaBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == RowUnary(
        Convention.RowType.VanillaRowType,
        ColumnarToRowExec(BatchLeaf(VanillaBatchType))))
  }

  test("Vanilla R2C - requires vanilla input") {
    val in = BatchUnary(VanillaBatchType, RowLeaf(Convention.RowType.VanillaRowType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(out == ColumnarToRowExec(
      BatchUnary(VanillaBatchType, RowToColumnarExec(RowLeaf(Convention.RowType.VanillaRowType)))))
  }

  test("ArrowNative C2R - outputs row") {
    val in = BatchLeaf(ArrowNativeBatchType)
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(out == ColumnarToRowExec(LoadArrowDataExec(BatchLeaf(ArrowNativeBatchType))))
  }

  test("ArrowNative C2R - requires row input") {
    val in = RowUnary(Convention.RowType.VanillaRowType, BatchLeaf(ArrowNativeBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == RowUnary(
        Convention.RowType.VanillaRowType,
        ColumnarToRowExec(LoadArrowDataExec(BatchLeaf(ArrowNativeBatchType)))))
  }

  test("ArrowNative R2C - requires Arrow input") {
    val in = BatchUnary(ArrowNativeBatchType, RowLeaf(Convention.RowType.VanillaRowType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == ColumnarToRowExec(
        LoadArrowDataExec(BatchUnary(
          ArrowNativeBatchType,
          RowToVeloxColumnarExec(RowLeaf(Convention.RowType.VanillaRowType))))))
  }

  test("ArrowNative-to-Velox C2C") {
    val in = BatchUnary(VeloxBatchType, BatchLeaf(ArrowNativeBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    // No explicit transition needed for ArrowNative-to-Velox.
    // FIXME: Add explicit transitions.
    //  See https://github.com/apache/incubator-gluten/issues/7313.
    assert(
      out == VeloxColumnarToRowExec(
        BatchUnary(
          VeloxBatchType,
          ArrowColumnarToVeloxColumnarExec(BatchLeaf(ArrowNativeBatchType)))))
  }

  test("Velox-to-ArrowNative C2C") {
    val in = BatchUnary(ArrowNativeBatchType, BatchLeaf(VeloxBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == ColumnarToRowExec(
        LoadArrowDataExec(BatchUnary(ArrowNativeBatchType, BatchLeaf(VeloxBatchType)))))
  }

  test("Vanilla-to-ArrowNative C2C") {
    val in = BatchUnary(ArrowNativeBatchType, BatchLeaf(VanillaBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == ColumnarToRowExec(
        LoadArrowDataExec(BatchUnary(
          ArrowNativeBatchType,
          RowToVeloxColumnarExec(ColumnarToRowExec(BatchLeaf(VanillaBatchType)))))))
  }

  test("ArrowNative-to-Vanilla C2C") {
    val in = BatchUnary(VanillaBatchType, BatchLeaf(ArrowNativeBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == ColumnarToRowExec(
        BatchUnary(VanillaBatchType, LoadArrowDataExec(BatchLeaf(ArrowNativeBatchType)))))
  }

  test("ArrowJava C2R - outputs row") {
    val in = BatchLeaf(ArrowJavaBatchType)
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(out == ColumnarToRowExec(BatchLeaf(ArrowJavaBatchType)))
  }

  test("ArrowJava C2R - requires row input") {
    val in = RowUnary(Convention.RowType.VanillaRowType, BatchLeaf(ArrowJavaBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == RowUnary(
        Convention.RowType.VanillaRowType,
        ColumnarToRowExec(BatchLeaf(ArrowJavaBatchType))))
  }

  test("ArrowJava R2C - requires Arrow input") {
    val in = BatchUnary(ArrowJavaBatchType, RowLeaf(Convention.RowType.VanillaRowType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == ColumnarToRowExec(
        BatchUnary(
          ArrowJavaBatchType,
          LoadArrowDataExec(RowToVeloxColumnarExec(RowLeaf(Convention.RowType.VanillaRowType))))))
  }

  test("ArrowJava-to-Velox C2C") {
    val in = BatchUnary(VeloxBatchType, BatchLeaf(ArrowJavaBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == VeloxColumnarToRowExec(
        BatchUnary(
          VeloxBatchType,
          ArrowColumnarToVeloxColumnarExec(OffloadArrowDataExec(BatchLeaf(ArrowJavaBatchType))))))
  }

  test("Velox-to-ArrowJava C2C") {
    val in = BatchUnary(ArrowJavaBatchType, BatchLeaf(VeloxBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == ColumnarToRowExec(
        BatchUnary(ArrowJavaBatchType, LoadArrowDataExec(BatchLeaf(VeloxBatchType)))))
  }

  test("Vanilla-to-ArrowJava C2C") {
    val in = BatchUnary(ArrowJavaBatchType, BatchLeaf(VanillaBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == ColumnarToRowExec(BatchUnary(
        ArrowJavaBatchType,
        LoadArrowDataExec(RowToVeloxColumnarExec(ColumnarToRowExec(BatchLeaf(VanillaBatchType)))))))
  }

  test("ArrowJava-to-Vanilla C2C") {
    val in = BatchUnary(VanillaBatchType, BatchLeaf(ArrowJavaBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(out == ColumnarToRowExec(BatchUnary(VanillaBatchType, BatchLeaf(ArrowJavaBatchType))))
  }

  test("Velox C2R - outputs row") {
    val in = BatchLeaf(VeloxBatchType)
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(out == VeloxColumnarToRowExec(BatchLeaf(VeloxBatchType)))
  }

  test("Velox C2R - requires row input") {
    val in = RowUnary(Convention.RowType.VanillaRowType, BatchLeaf(VeloxBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == RowUnary(
        Convention.RowType.VanillaRowType,
        VeloxColumnarToRowExec(BatchLeaf(VeloxBatchType))))
  }

  test("Velox R2C - outputs Velox") {
    val in = RowLeaf(Convention.RowType.VanillaRowType)
    val out = BackendTransitions.insert(in, outputsColumnar = true)
    assert(out == RowToVeloxColumnarExec(RowLeaf(Convention.RowType.VanillaRowType)))
  }

  test("Velox R2C - requires Velox input") {
    val in = BatchUnary(VeloxBatchType, RowLeaf(Convention.RowType.VanillaRowType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == VeloxColumnarToRowExec(
        BatchUnary(
          VeloxBatchType,
          RowToVeloxColumnarExec(RowLeaf(Convention.RowType.VanillaRowType)))))
  }

  test("Vanilla-to-Velox C2C") {
    val in = BatchUnary(VeloxBatchType, BatchLeaf(VanillaBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == VeloxColumnarToRowExec(
        BatchUnary(
          VeloxBatchType,
          RowToVeloxColumnarExec(ColumnarToRowExec(BatchLeaf(VanillaBatchType))))))
  }

  test("Velox-to-Vanilla C2C") {
    val in = BatchUnary(VanillaBatchType, BatchLeaf(VeloxBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == ColumnarToRowExec(
        BatchUnary(VanillaBatchType, LoadArrowDataExec(BatchLeaf(VeloxBatchType)))))
  }

  test("Velox-to-CarrierRow C2R") {
    val in =
      RowToRow(VeloxCarrierRowType, Convention.RowType.VanillaRowType, BatchLeaf(VeloxBatchType))
    val out = BackendTransitions.insert(in, outputsColumnar = false)
    assert(
      out == RowToRow(
        VeloxCarrierRowType,
        Convention.RowType.VanillaRowType,
        VeloxColumnarToCarrierRowExec(BatchLeaf(VeloxBatchType))))
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    api.onExecutorStart(MockVeloxBackend.mockPluginContext())
  }

  override protected def afterAll(): Unit = {
    api.onExecutorShutdown()
    super.afterAll()
  }
}

object VeloxTransitionSuite extends TransitionSuiteBase {}
