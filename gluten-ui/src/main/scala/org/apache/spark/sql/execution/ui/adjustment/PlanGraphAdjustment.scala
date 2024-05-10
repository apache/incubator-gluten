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
package org.apache.spark.sql.execution.ui.adjustment

import org.apache.spark.sql.execution.ui.{SparkPlanGraphEdge, SparkPlanGraphNodeWrapper, SparkPlanGraphWrapper}
import org.apache.spark.sql.execution.ui.adjustment.PlanGraphAdjustment.register
import org.apache.spark.status.ElementTrackingStore

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait PlanGraphAdjustment {
  private val registered: AtomicBoolean = new AtomicBoolean(false)

  final def ensureRegistered(): Unit = {
    if (!registered.compareAndSet(false, true)) {
      return
    }
    register(this)
  }

  final protected def findChildren(
      graph: SparkPlanGraphWrapper,
      node: SparkPlanGraphNodeWrapper): Seq[(SparkPlanGraphNodeWrapper, SparkPlanGraphEdge)] = {
    val id = node.node.id
    val out = graph.edges
      .filter(_.toId == id)
      .flatMap {
        childEdge =>
          graph.nodes
            .filter(_.node != null)
            .find(_.node.id == childEdge.fromId)
            .map(childNode => (childNode, childEdge))
      }
    out
  }

  final protected def findParent(
      graph: SparkPlanGraphWrapper,
      node: SparkPlanGraphNodeWrapper): Option[(SparkPlanGraphNodeWrapper, SparkPlanGraphEdge)] = {
    val id = node.node.id
    val out = graph.edges
      .filter(_.fromId == id)
      .flatMap {
        parentEdge =>
          graph.nodes
            .filter(_.node != null)
            .find(_.node.id == parentEdge.toId)
            .map(parentNode => (parentNode, parentEdge))
      }
    if (out.isEmpty) {
      return None
    }
    if (out.size == 1) {
      return Some(out.head)
    }
    throw new IllegalStateException("Node has multiple parents, it should not happen: " + node)
  }

  def apply(from: SparkPlanGraphWrapper): SparkPlanGraphWrapper
}

object PlanGraphAdjustment {
  private val adjustments: ListBuffer[PlanGraphAdjustment] = mutable.ListBuffer()

  private[ui] def register(adjustment: PlanGraphAdjustment): Unit = synchronized {
    adjustments += adjustment
  }

  private[ui] def adjust(kvstore: ElementTrackingStore, executionId: Long): Unit = {
    val graph = kvstore.read(classOf[SparkPlanGraphWrapper], executionId)
    val out = adjustments.foldLeft(graph) {
      case (g, a) =>
        a.apply(g)
    }
    assert(out.executionId == executionId)
    kvstore.delete(classOf[SparkPlanGraphWrapper], executionId)
    kvstore.write(out)
  }
}
