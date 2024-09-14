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
package org.apache.gluten.utils

import org.apache.gluten.backend.Backend
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec

import scala.annotation.tailrec

object PlanUtil {
  private def isGlutenTableCacheInternal(i: InMemoryTableScanExec): Boolean = {
    Convention.get(i).batchType == Backend.get().batchType
  }

  @tailrec
  def isGlutenTableCache(plan: SparkPlan): Boolean = {
    plan match {
      case i: InMemoryTableScanExec =>
        isGlutenTableCacheInternal(i)
      case q: QueryStageExec =>
        // Compatible with Spark3.5 `TableCacheQueryStage`
        isGlutenTableCache(q.plan)
      case _ => false
    }
  }

  def isVanillaColumnarOp(plan: SparkPlan): Boolean = {
    Convention.get(plan).batchType == Convention.BatchType.VanillaBatch
  }

  def isGlutenColumnarOp(plan: SparkPlan): Boolean = {
    Convention.get(plan).batchType == Backend.get().batchType
  }
}
