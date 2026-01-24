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

import org.apache.spark.sql.catalyst.expressions.{ArrayContains, MapContainsKey, MapKeys}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Rewrite pattern `array_contains(map_keys(m), k)` to `map_contains_key(m, k)`.
 *
 * This avoids materializing the intermediate array of keys and scanning it at runtime, while
 * preserving Spark's type coercion and null-handling semantics by relying on the built-in Catalyst
 * expressions.
 */
object ArrayContainsMapKeysRewriteRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved) {
      plan
    } else {
      plan.transformExpressionsUp {
        case ArrayContains(MapKeys(mapExpr), keyExpr) =>
          MapContainsKey(mapExpr, keyExpr)
      }
    }
  }
}
