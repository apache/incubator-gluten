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
package org.apache.spark.sql.hive

import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, GlutenSQLTestsBaseTrait, GlutenTestsBaseTrait}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.{ColumnarSubqueryBroadcastExec, InSubqueryExec, ReusedSubqueryExec, SparkPlan, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.hive.execution.HiveTableScanExec

abstract class GlutenDynamicPartitionPruningHiveScanSuiteBase
  extends DynamicPartitionPruningHiveScanSuiteBase
  with GlutenTestsBaseTrait {

  private var _spark: SparkSession = null

  override protected def spark: SparkSession = _spark

  override def sparkConf: SparkConf = {
    GlutenSQLTestsBaseTrait.nativeSparkConf(super.sparkConf, warehouse)
  }

  override def beforeAll(): Unit = {
    if (_spark == null) {
      _spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          _spark.sessionState.catalog.reset()
          _spark.stop()
          _spark = null
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  override protected def collectDynamicPruningExpressions(plan: SparkPlan): Seq[Expression] = {
    flatMap(plan) {
      case f: FileSourceScanExecTransformer =>
        f.partitionFilters.collect { case d: DynamicPruningExpression => d.child }
      case h: HiveTableScanExec =>
        h.partitionPruningPred.collect { case d: DynamicPruningExpression => d.child }
      case _ => Nil
    }
  }

  override def checkPartitionPruningPredicate(
      df: DataFrame,
      withSubquery: Boolean,
      withBroadcast: Boolean): Unit = {
    df.collect()

    val plan = df.queryExecution.executedPlan
    val dpExprs = collectDynamicPruningExpressions(plan)
    val hasSubquery = dpExprs.exists {
      case InSubqueryExec(_, _: SubqueryExec, _, _, _, _) => true
      case _ => false
    }
    val subqueryBroadcast = dpExprs.collect {
      case InSubqueryExec(_, b: ColumnarSubqueryBroadcastExec, _, _, _, _) => b
    }

    val hasFilter = if (withSubquery) "Should" else "Shouldn't"
    assert(
      hasSubquery == withSubquery,
      s"$hasFilter trigger DPP with a subquery duplicate:\n${df.queryExecution}")
    val hasBroadcast = if (withBroadcast) "Should" else "Shouldn't"
    assert(
      subqueryBroadcast.nonEmpty == withBroadcast,
      s"$hasBroadcast trigger DPP with a reused broadcast exchange:\n${df.queryExecution}")

    subqueryBroadcast.foreach {
      s =>
        s.child match {
          case _: ReusedExchangeExec => // reuse check ok.
          case a: AdaptiveSparkPlanExec =>
            val broadcastQueryStage = collectFirst(a) { case b: BroadcastQueryStageExec => b }
            val broadcastPlan = broadcastQueryStage.get.broadcast
            val hasReuse = find(plan) {
              case ReusedExchangeExec(_, e) => e eq broadcastPlan
              case b: BroadcastExchangeLike => b eq broadcastPlan
              case _ => false
            }.isDefined
            assert(hasReuse, s"$s\nshould have been reused in\n$plan")
          case BroadcastQueryStageExec(_, _: ReusedExchangeExec, _) => // reuse check ok.
          case b: BroadcastExchangeLike =>
            val hasReuse = plan.find {
              case ReusedExchangeExec(_, e) => e eq b
              case _ => false
            }.isDefined
            assert(hasReuse, s"$s\nshould have been reused in\n$plan")
          case _ =>
            fail(s"Invalid child node found in\n$s")
        }
    }

    val isMainQueryAdaptive = plan.isInstanceOf[AdaptiveSparkPlanExec]
    subqueriesAll(plan).filterNot(subqueryBroadcast.contains).foreach {
      s =>
        val subquery = s match {
          case r: ReusedSubqueryExec => r.child
          case o => o
        }
        assert(
          subquery.find(_.isInstanceOf[AdaptiveSparkPlanExec]).isDefined == isMainQueryAdaptive)
    }
  }

  override def checkDistinctSubqueries(df: DataFrame, n: Int): Unit = {
    df.collect()

    val buf = collectDynamicPruningExpressions(df.queryExecution.executedPlan).collect {
      case InSubqueryExec(_, b: ColumnarSubqueryBroadcastExec, _, _, _, _) =>
        b
    }
    assert(buf.distinct.size == n)
  }
}

class GlutenDynamicPartitionPruningHiveScanSuiteAEOff
  extends GlutenDynamicPartitionPruningHiveScanSuiteBase
  with DisableAdaptiveExecutionSuite

class GlutenDynamicPartitionPruningHiveScanSuiteAEOn
  extends GlutenDynamicPartitionPruningHiveScanSuiteBase
  with EnableAdaptiveExecutionSuite
