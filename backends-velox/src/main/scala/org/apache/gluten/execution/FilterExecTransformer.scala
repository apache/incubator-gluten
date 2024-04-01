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
package org.apache.gluten.execution

import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.execution.SparkPlan

case class FilterExecTransformer(condition: Expression, child: SparkPlan)
  extends FilterExecTransformerBase(condition, child) {
  override protected def getRemainingCondition: Expression = {
    val scanFilters = child match {
      // Get the filters including the manually pushed down ones.
      case basicScanExecTransformer: BasicScanExecTransformer =>
        basicScanExecTransformer.filterExprs()
      // For fallback scan, we need to keep original filter.
      case _ =>
        Seq.empty[Expression]
    }
    if (scanFilters.isEmpty) {
      condition
    } else {
      val remainingFilters =
        FilterHandler.getRemainingFilters(scanFilters, splitConjunctivePredicates(condition))
      remainingFilters.reduceLeftOption(And).orNull
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): FilterExecTransformer =
    copy(child = newChild)
}
