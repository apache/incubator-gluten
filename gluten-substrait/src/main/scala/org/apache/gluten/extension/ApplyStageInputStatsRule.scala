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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.WholeStageTransformContext
import org.apache.gluten.substrait.rel.{InputIteratorRelNode, ReadRelNode, RelNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CoalescedMapperPartitionSpec, CoalescedPartitionSpec, FileSourceScanExec, PartialMapperPartitionSpec, PartialReducerPartitionSpec, ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, BroadcastStats, InputStats, ScanInputStats, ShuffleQueryStageExec, ShuffleStageWrapper, ShuffleStats, UnknownStats}
import org.apache.spark.sql.execution.adaptive.ShuffleStageWrapper.INPUT_STATS
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS

import java.util

import scala.collection.{mutable, Seq}
// In aqe running mode, we can get query stage runtime stats, such as shuffle row count and shuffle
// size. We can do some potential optimize by passing those info to lower bolt operator. And now we
// only support passing shuffle hash join's upstream info.
// Useless code, kept for further potential use.
case class ApplyStageInputStatsToBoltPreOverrideRule(session: SparkSession)
  extends Rule[SparkPlan] {
  // here we assume the plan should belong to independent stage.
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enablePassStageInputStats) {
      return plan
    }
    val inputStatsList = new util.ArrayList[InputStats]
    plan.transformUp {
      case s: ShuffleQueryStageExec
          if s.isMaterialized && s.mapStats.isDefined &&
            s.shuffle.shuffleOrigin == ENSURE_REQUIREMENTS =>
        logInfo("hit ApplyStageInputStatsToBoltRule ShuffleQueryStageExec with stats")
        val statsOpt = ShuffleStageWrapper.unapply(s)
        if (statsOpt.isDefined) {
          inputStatsList.add(
            InputStats(
              known = true,
              statsOpt.get.sizeInBytes,
              statsOpt.get.rowCount.getOrElse(0),
              statsOpt.get.mapOutputStatistics.map(_.bytesByPartitionId).getOrElse(Array()),
              ShuffleStats
            )
          )
        } else {
          logInfo("hit ApplyStageInputStatsToBoltRule ShuffleQueryStageExec but empty stats")
          inputStatsList.add(InputStats(known = false, 0, 0, Array(), UnknownStats))
        }
        s
      case b: BroadcastQueryStageExec if b.isMaterialized =>
        logInfo("hit ApplyStageInputStatsToBoltRule BroadcastQueryStageExec with stats")
        val statsOpt = ShuffleStageWrapper.unapply(b)
        if (statsOpt.isDefined) {
          inputStatsList.add(
            InputStats(
              known = true,
              statsOpt.get.sizeInBytes,
              statsOpt.get.rowCount.getOrElse(0),
              statsOpt.get.mapOutputStatistics.map(_.bytesByPartitionId).getOrElse(Array()),
              BroadcastStats
            )
          )
        } else {
          logInfo("hit ApplyStageInputStatsToBoltRule BroadcastQueryStageExec but empty stats")
          inputStatsList.add(InputStats(known = true, 0, 0, Array(), UnknownStats))
        }
        b
      case scan: FileSourceScanExec =>
        val logicalPlan = scan.logicalLink
        if (logicalPlan.isDefined) {
          logInfo("scan logical link:" + logicalPlan.get)
          logInfo("FileSourceScanTransformer logical plan " + logicalPlan.get)
          val maybeCatalogTable = logicalPlan.get.collectFirst {
            case LogicalRelation(_, _, catalogTable, _) => catalogTable
          }
          if (
            maybeCatalogTable.isDefined && maybeCatalogTable.get.isDefined &&
            maybeCatalogTable.get.get.stats.isDefined
          ) {
            val scanStats = maybeCatalogTable.get.get.stats.get
            logInfo("catalogTable " + scanStats)
            inputStatsList.add(
              InputStats(
                true,
                scanStats.sizeInBytes,
                scanStats.rowCount.getOrElse(0),
                Array(),
                ScanInputStats))
          } else {
            inputStatsList.add(InputStats(known = false, 0, 0, Array(), UnknownStats))
          }
        } else {
          inputStatsList.add(InputStats(known = false, 0, 0, Array(), UnknownStats))
          logInfo("no related catalogtable")
        }
        scan
    }
    if (!inputStatsList.isEmpty) {
      plan.setTagValue(INPUT_STATS, inputStatsList)
    }
    plan
  }
}

object ApplyStageInputStatsRule extends Logging {
  def setStageInputStatsToInputNode(
      ws: WholeStageTransformContext,
      index: Int,
      partitionLength: Int): Unit = {
    val relNodesQueue: mutable.Queue[RelNode] = mutable.Queue()
    val nodes = ws.root.getRelNodes
    if (nodes != null) {
      nodes.forEach(relNodesQueue.enqueue(_))
    }
    while (relNodesQueue.nonEmpty) {
      val node = relNodesQueue.dequeue()
      node match {
        case input: InputIteratorRelNode =>
          val stats = input.getInputStats
          if (null != stats) {
            val totalBytes = Math.max(stats.bytesByPartitionId.sum, 1)
            val estimatedRowCount =
              if (stats.inputStatsKind == BroadcastStats) {
                stats.rowCount.toLong
              } else {
                (1.0d * stats.rowCount.toLong / totalBytes * stats.bytesByPartitionId(index)).toLong
              }
            logInfo(s"pass input stats idx: $index with estimated row count $estimatedRowCount")
            input.setRowSize(estimatedRowCount)
          }
        case scan: ReadRelNode =>
          val stats = scan.getInputStats
          if (null != stats) {
            val estimatedRowCount = (1.0d * stats.rowCount.toLong / partitionLength).toLong
            logInfo(s"pass scan stats idx: $index with estimated row count $estimatedRowCount")
            scan.setRowSize(estimatedRowCount)
          }
        case _ =>
      }
      node.childNode().forEach(relNodesQueue.enqueue(_))
    }
  }

  /** Compute task stats for each partition */
  def recomputeInputStatsForAQEShuffleReadExec(
      original: InputStats,
      aqeShufflePartitions: Seq[ShufflePartitionSpec]): InputStats = {
    val originalBytesByPartition = original.bytesByPartitionId
    val totalBytes = originalBytesByPartition.sum
    val rowCount = original.rowCount
    val aqeReadPartitionsStats = new mutable.ArrayBuffer[Long]()
    aqeShufflePartitions.foreach {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        var newPartitionDataSize = 0L
        for (i <- startReducerIndex until endReducerIndex) {
          if (i < originalBytesByPartition.length) {
            newPartitionDataSize += originalBytesByPartition(i)
          } else {
            newPartitionDataSize += 0L
          }
        }
        aqeReadPartitionsStats += newPartitionDataSize
      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        // over estimated stats
        if (reducerIndex < originalBytesByPartition.length) {
          aqeReadPartitionsStats += originalBytesByPartition(reducerIndex)
        } else {
          aqeReadPartitionsStats += 0L
        }
      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        // unknown stats
        aqeReadPartitionsStats += 0L
      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        // unknown stats
        aqeReadPartitionsStats += 0L
    }
    InputStats(known = true, totalBytes, rowCount, aqeReadPartitionsStats.toArray, ShuffleStats)
  }
  def createInputStats(logicalLink: Option[LogicalPlan]): Option[InputStats] = {
    if (logicalLink.isEmpty) {
      logInfo(s"no logicalLink associated")
      return None
    }
    val logicalPlan = logicalLink.get
    logInfo("scan logical link:" + logicalPlan)
    logInfo("FileSourceScanTransformer logical plan " + logicalPlan)
    val maybeCatalogTable = logicalPlan.collectFirst {
      case LogicalRelation(_, _, catalogTable, _) => catalogTable
    }
    if (
      maybeCatalogTable.isDefined && maybeCatalogTable.get.isDefined &&
      maybeCatalogTable.get.get.stats.isDefined
    ) {
      val scanStats = maybeCatalogTable.get.get.stats.get
      logInfo("catalogTable " + scanStats)
      Some(
        InputStats(
          known = true,
          scanStats.sizeInBytes,
          scanStats.rowCount.getOrElse(0),
          Array(),
          ScanInputStats))
    } else {
      None
    }
  }
}
