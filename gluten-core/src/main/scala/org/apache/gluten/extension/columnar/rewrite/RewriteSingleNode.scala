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
package org.apache.gluten.extension.columnar.rewrite

import org.apache.spark.sql.execution.SparkPlan

/**
 * Rewrites a plan node from vanilla Spark into its alternative representation.
 *
 * Gluten's planner will pick one that is considered the best executable plan between input plan and
 * the output plan.
 *
 * Note: Only the current plan node is supposed to be open to modification. Do not access or modify
 * the children node. Tree-walking is done by caller of this trait.
 *
 * TODO: Ideally for such API we'd better to allow multiple alternative outputs.
 */
trait RewriteSingleNode {
  def isRewritable(plan: SparkPlan): Boolean
  def rewrite(plan: SparkPlan): SparkPlan
}
