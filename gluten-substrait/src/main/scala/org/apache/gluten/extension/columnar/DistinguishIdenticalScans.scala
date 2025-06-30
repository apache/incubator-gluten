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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, RDDScanExec, SparkPlan, SparkPlanUtil}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanExecBase}

import scala.collection.mutable

object DistinguishIdenticalScans extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    val seenScans = mutable.Set[SparkPlan]()

    def traverse(node: SparkPlan): (SparkPlan, Boolean) = {
      // 1. Recursively traverse children first (bottom-up).
      var childrenChanged = false
      val newChildren = node.children.map {
        child =>
          val (newChild, childChanged) = traverse(child)
          if (childChanged) {
            childrenChanged = true
          }
          newChild
      }

      // 2. Rebuild the parent node ONLY if a child was changed.
      val nodeWithNewChildren = if (childrenChanged) {
        SparkPlanUtil.withNewChildrenInternal(node, newChildren.toIndexedSeq)
      } else {
        node
      }

      // 3. Process the current node (after its children are finalized).
      val (finalNode, currentNodeChanged) = nodeWithNewChildren match {
        case scan: FileSourceScanExec => distinguish(scan)
        case scan: BatchScanExec => distinguish(scan)
        case scan: DataSourceV2ScanExecBase => distinguish(scan)
        case scan: RDDScanExec => distinguish(scan)
        // If it's not a scan, it doesn't change at this step.
        case other => (other, false)
      }

      // 4. The final 'changed' status is true if EITHER the children changed OR
      // the current node changed.
      val overallChanged = childrenChanged || currentNodeChanged
      (finalNode, overallChanged)
    }

    def distinguish[T <: SparkPlan](scan: T): (T, Boolean) = {
      if (seenScans.contains(scan)) {
        // A clone was made, so return the new object and 'true'.
        (scan.clone().asInstanceOf[T], true)
      } else {
        // First time seeing this scan. Add it to the set and signal no change.
        seenScans.add(scan)
        (scan, false)
      }
    }

    val (newPlan, _) = traverse(plan)
    newPlan
  }
}
