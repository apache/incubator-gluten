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
package org.apache.gluten.extension.columnar

import org.apache.gluten.component.WithDummyBackend
import org.apache.gluten.extension.columnar.MiscColumnarRules.PreventBatchTypeMismatchInTableCache
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.extension.columnar.transition.TransitionSuiteBase.BatchToRow

import org.apache.spark.sql.test.SharedSparkSession

class MiscColumnarRulesSuite extends SharedSparkSession with WithDummyBackend {

  test("Fix ColumnarToRowRemovalGuard not able to be copied") {
    val dummyPlan =
      BatchToRow(
        Convention.BatchType.VanillaBatchType,
        Convention.RowType.VanillaRowType,
        spark.range(1).queryExecution.sparkPlan)
    val cloned =
      PreventBatchTypeMismatchInTableCache(isCalledByTableCachePlaning = true, Set.empty)
        .apply(dummyPlan)
        .clone()
    assert(cloned.isInstanceOf[ColumnarToRowRemovalGuard])
  }
}
