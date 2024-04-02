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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.WholeStageTransformer
import org.apache.gluten.extension.GlutenPlan
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.{AnalysisException, Dataset}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, LogicalPlan}
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.execution.GlutenExplainUtils._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// spotless:off
/**
 * A helper class to get the Gluten fallback summary from a Spark [[Dataset]].
 *
 * Note that, if AQE is enabled, but the query is not materialized, then this method will re-plan
 * the query execution with disabled AQE. It is a workaround to get the final plan, and it may
 * cause the inconsistent results with a materialized query. However, we have no choice.
 *
 * For example:
 *
 * {{{
 *   import org.apache.spark.sql.execution.GlutenImplicits._
 *   val df = spark.sql("SELECT * FROM t")
 *   df.fallbackSummary
 * }}}
 */
// spotless:on
object GlutenImplicits {

  case class FallbackSummary(
      numGlutenNodes: Int,
      numFallbackNodes: Int,
      physicalPlanDescription: Seq[String],
      fallbackNodeToReason: Seq[Map[String, String]]) {}

  private[sql] def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map {
      key =>
        if (conf.contains(key)) {
          Some(conf.getConfString(key))
        } else {
          None
        }
    }
    keys.zip(values).foreach {
      case (k, v) =>
        if (SQLConf.isStaticConfigKey(k)) {
          throw new AnalysisException(s"Cannot modify the value of a static config: $k")
        }
        conf.setConfString(k, v)
    }
    try f
    finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  implicit class DatasetTransformer[T](dateset: Dataset[T]) {
    private def isFinalAdaptivePlan(p: AdaptiveSparkPlanExec): Boolean = {
      val args = p.argString(Int.MaxValue)
      val index = args.indexOf("isFinalPlan=")
      assert(index >= 0)
      args.substring(index + "isFinalPlan=".length).trim.toBoolean
    }

    private def collectFallbackNodes(plan: QueryPlan[_]): FallbackInfo = {
      var numGlutenNodes = 0
      val fallbackNodeToReason = new mutable.HashMap[String, String]

      def collect(tmp: QueryPlan[_]): Unit = {
        tmp.foreachUp {
          case _: ExecutedCommandExec =>
          case _: CommandResultExec =>
          case _: V2CommandExec =>
          case _: DataWritingCommandExec =>
          case _: WholeStageCodegenExec =>
          case _: WholeStageTransformer =>
          case _: InputAdapter =>
          case _: ColumnarInputAdapter =>
          case _: InputIteratorTransformer =>
          case _: ColumnarToRowTransition =>
          case _: RowToColumnarTransition =>
          case p: ReusedExchangeExec =>
          case p: AdaptiveSparkPlanExec if isFinalAdaptivePlan(p) =>
            collect(p.executedPlan)
          case p: AdaptiveSparkPlanExec =>
            // if we are here that means we are inside table cache.
            val (innerNumGlutenNodes, innerFallbackNodeToReason) =
              withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
                // re-plan manually to skip cached data
                val newSparkPlan = QueryExecution.createSparkPlan(
                  dateset.sparkSession,
                  dateset.sparkSession.sessionState.planner,
                  p.inputPlan.logicalLink.get)
                val newExecutedPlan = QueryExecution.prepareExecutedPlan(
                  dateset.sparkSession,
                  newSparkPlan
                )
                processPlan(
                  newExecutedPlan,
                  new PlanStringConcat().append,
                  Some(plan => collectFallbackNodes(plan)))
              }
            numGlutenNodes += innerNumGlutenNodes
            fallbackNodeToReason.++=(innerFallbackNodeToReason)
          case p: QueryStageExec => collect(p.plan)
          case p: GlutenPlan =>
            numGlutenNodes += 1
            p.innerChildren.foreach(collect)
          case i: InMemoryTableScanExec =>
            if (PlanUtil.isGlutenTableCache(i)) {
              numGlutenNodes += 1
            } else {
              addFallbackNodeWithReason(i, "Columnar table cache is disabled", fallbackNodeToReason)
            }
            collect(i.relation.cachedPlan)
          case _: AQEShuffleReadExec => // Ignore
          case p: SparkPlan =>
            handleVanillaSparkPlan(p, fallbackNodeToReason)
            p.innerChildren.foreach(collect)
          case _ =>
        }
      }

      collect(plan)
      (numGlutenNodes, fallbackNodeToReason.toMap)
    }

    private def collectQueryExecutionFallbackSummary(qe: QueryExecution): FallbackSummary = {
      var totalNumGlutenNodes = 0
      var totalNumFallbackNodes = 0
      val totalPhysicalPlanDescription = new ArrayBuffer[String]()
      val totalFallbackNodeToReason = new ArrayBuffer[Map[String, String]]()

      def handlePlanWithAQEAndTableCache(
          plan: SparkPlan,
          logicalPlan: LogicalPlan,
          isMaterialized: Boolean): Unit = {
        val concat = new PlanStringConcat()
        val collectFallbackFunc = Some(plan => collectFallbackNodes(plan))
        val (numGlutenNodes, fallbackNodeToReason) = if (!isMaterialized) {
          withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
            // AQE is not materialized, so the columnar rules are not applied.
            // For this case, We apply columnar rules manually with disable AQE.
            val qe = dateset.sparkSession.sessionState.executePlan(logicalPlan)
            processPlan(qe.executedPlan, concat.append, collectFallbackFunc)
          }
        } else {
          processPlan(plan, concat.append, collectFallbackFunc)
        }
        totalNumGlutenNodes += numGlutenNodes
        totalNumFallbackNodes += fallbackNodeToReason.size
        totalPhysicalPlanDescription.append(concat.toString())
        totalFallbackNodeToReason.append(fallbackNodeToReason)
      }

      // For command-like query, e.g., `INSERT INTO TABLE ...`
      qe.commandExecuted.foreach {
        case r: CommandResult =>
          handlePlanWithAQEAndTableCache(r.commandPhysicalPlan, r.commandLogicalPlan, true)
        case _ => // ignore
      }

      // For query, e.g., `SELECT * FROM ...`
      if (qe.executedPlan.find(_.isInstanceOf[CommandResultExec]).isEmpty) {
        val isMaterialized = qe.executedPlan.find {
          case a: AdaptiveSparkPlanExec if isFinalAdaptivePlan(a) => true
          case _ => false
        }.isDefined
        handlePlanWithAQEAndTableCache(qe.executedPlan, qe.analyzed, isMaterialized)
      }

      FallbackSummary(
        totalNumGlutenNodes,
        totalNumFallbackNodes,
        totalPhysicalPlanDescription,
        totalFallbackNodeToReason
      )
    }

    def fallbackSummary(): FallbackSummary = {
      collectQueryExecutionFallbackSummary(dateset.queryExecution)
    }
  }
}
