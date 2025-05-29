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

import org.apache.spark.sql.catalyst.expressions.GetTimestamp
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * When there is a format parameter, to_date and to_timestamp will be replaced by GetTimestamp. If
 * the date type of GetTimestamp is the same as its left, then GetTimestamp is redundant. Velox does
 * not support GetTimestamp(Timestamp, format). In this case, it needs to be removed.
 */
object EliminateRedundantGetTimestamp extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformExpressions {
      case getTimestamp: GetTimestamp if getTimestamp.left.dataType == getTimestamp.dataType =>
        getTimestamp.left
    }
  }
}
