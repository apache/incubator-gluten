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

import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.execution.SparkPlan

class ColumnarRuleExecutor(phase: String, rules: Seq[Rule[SparkPlan]])
  extends RuleExecutor[SparkPlan] {
  private val batch: Batch = Batch(s"Columnar (Phase [$phase])", Once, rules: _*)

  // TODO: Remove this exclusion then manage to pass Spark's idempotence check.
  override protected val excludedOnceBatches: Set[String] = Set(batch.name)

  override protected def batches: Seq[Batch] = Seq(batch)
}
