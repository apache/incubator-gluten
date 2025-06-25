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
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanExecBase}
import org.apache.spark.sql.execution.{FileSourceScanExec, RDDScanExec, SparkPlan, SparkPlanReflectionUtil}

import scala.collection.mutable

object DistinguishIdenticalScans extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    // Set to track seen scan objects
    val seenScans = mutable.Set[SparkPlan]()

    // Custom traversal to avoid transformUp's fastEquals optimization
    def traverse(node: SparkPlan): (SparkPlan, Boolean) = {
      // First, check if this node is a scan and handle scan distinction logic
      val (nodeToProcess, scanChanged) = node match {
        case scan: FileSourceScanExec =>
          distinguishScan(scan, seenScans)
        case scan: BatchScanExec =>
          distinguishScan(scan, seenScans)
        case scan: DataSourceV2ScanExecBase =>
          distinguishScan(scan, seenScans)
        case scan: RDDScanExec =>
          distinguishScan(scan, seenScans)
        case other => (other, false)
      }

      // Then, recursively process children of the (possibly cloned) node
      var childrenChanged = false
      val newChildren = nodeToProcess.children.map { child =>
        val (newChild, changed) = traverse(child)
        if (changed) childrenChanged = true
        newChild
      }

      // Create final node with new children if any children changed
      val finalNode = if (childrenChanged) {
        SparkPlanReflectionUtil.withNewChildrenInternal(nodeToProcess, newChildren.toIndexedSeq)
      } else {
        nodeToProcess
      }

      (finalNode, scanChanged || childrenChanged)
    }

    val (newPlan, _) = traverse(plan)
    newPlan
  }

  private def distinguishScan[T <: SparkPlan](
                                               scan: T,
                                               seenScans: mutable.Set[SparkPlan]): (SparkPlan, Boolean) = {
    if (seenScans.contains(scan)) {
      // Scan already seen, clone it to distinguish from the original
      val newScan = cloneScan(scan)
      (newScan, true)
    } else {
      // First time seeing this scan
      seenScans.add(scan)
      (scan, false)
    }
  }

  private def cloneScan(scan: SparkPlan): SparkPlan = {
    scan.clone()
  }
}
