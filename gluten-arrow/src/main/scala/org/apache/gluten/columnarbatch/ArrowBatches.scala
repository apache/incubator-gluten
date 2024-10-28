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
package org.apache.gluten.columnarbatch

import org.apache.gluten.execution.{LoadArrowDataExec, OffloadArrowDataExec}
import org.apache.gluten.extension.columnar.transition.{Convention, Transition}
import org.apache.gluten.extension.columnar.transition.Convention.BatchType.VanillaBatch

object ArrowBatches {

  /**
   * ArrowJavaBatch stands for Gluten's Java Arrow-based columnar batch implementation.
   *
   * ArrowJavaBatch should have [[org.apache.gluten.vectorized.ArrowWritableColumnVector]]s
   * populated in it. ArrowJavaBatch can be offloaded to ArrowNativeBatch through API in
   * [[ColumnarBatches]].
   *
   * ArrowJavaBatch is compatible with vanilla batch since it provides valid #get<type>(...)
   * implementations.
   */
  object ArrowJavaBatch extends Convention.BatchType {
    toBatch(VanillaBatch, Transition.empty)
  }

  /**
   * ArrowNativeBatch stands for Gluten's native Arrow-based columnar batch implementation.
   *
   * ArrowNativeBatch should have [[org.apache.gluten.columnarbatch.IndicatorVector]] set as the
   * first vector. ArrowNativeBatch can be loaded to ArrowJavaBatch through API in
   * [[ColumnarBatches]].
   */
  object ArrowNativeBatch extends Convention.BatchType {
    fromBatch(ArrowJavaBatch, OffloadArrowDataExec.apply)
    toBatch(ArrowJavaBatch, LoadArrowDataExec.apply)
  }
}
