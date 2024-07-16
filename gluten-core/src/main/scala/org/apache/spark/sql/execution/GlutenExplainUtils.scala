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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.GlutenFallbackReporter.FALLBACK_REASON_TAG
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}

import java.util
import java.util.Collections.newSetFromMap

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, BitSet}

// This file is copied from Spark `ExplainUtils` and changes:
// 1. add function `collectFallbackNodes`
// 2. remove `plan.verboseStringWithOperatorId`
// 3. remove codegen id
object GlutenExplainUtils extends AdaptiveSparkPlanHelper {
  type FallbackInfo = (Int, Map[String, String])

  def addFallbackNodeWithReason(
      p: SparkPlan,
      reason: String,
      fallbackNodeToReason: mutable.HashMap[String, String]): Unit = {
    p.getTagValue(QueryPlan.OP_ID_TAG).foreach {
      opId =>
        // e.g., 002 project, it is used to help analysis by `substring(4)`
        val formattedNodeName = f"$opId%03d ${p.nodeName}"
        fallbackNodeToReason.put(formattedNodeName, reason)
    }
  }

  def handleVanillaSparkPlan(
      p: SparkPlan,
      fallbackNodeToReason: mutable.HashMap[String, String]
  ): Unit = {
    p.logicalLink.flatMap(_.getTagValue(FALLBACK_REASON_TAG)) match {
      case Some(reason) => addFallbackNodeWithReason(p, reason, fallbackNodeToReason)
      case _ =>
        // If the SparkPlan does not have fallback reason, then there are two options:
        // 1. Gluten ignore that plan and it's a kind of fallback
        // 2. Gluten does not support it without the fallback reason
        addFallbackNodeWithReason(
          p,
          "Gluten does not touch it or does not support it",
          fallbackNodeToReason)
    }
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
        case _: ReusedExchangeExec =>
        case sub: AdaptiveSparkPlanExec if sub.isSubquery => collect(sub.executedPlan)
        case _: AdaptiveSparkPlanExec =>
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

  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Generate the two part explain output for this plan.
   *      1. First part explains the operator tree with each operator tagged with an unique
   *         identifier. 2. Second part explains each operator in a verbose manner.
   *
   * Note : This function skips over subqueries. They are handled by its caller.
   *
   * @param plan
   *   Input query plan to process
   * @param append
   *   function used to append the explain output
   * @param collectedOperators
   *   The IDs of the operators that are already collected and we shouldn't collect again.
   */
  private def processPlanSkippingSubqueries[T <: QueryPlan[T]](
      plan: T,
      append: String => Unit,
      collectedOperators: BitSet): Unit = {
    try {

      QueryPlan.append(plan, append, verbose = false, addSuffix = false, printOperatorId = true)

      append("\n")
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }

  // spotless:off
  // scalastyle:off
  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Generates the explain output for the input plan excluding the subquery plans.
   *   2. Generates the explain output for each subquery referenced in the plan.
   */
  def processPlan[T <: QueryPlan[T]](
      plan: T,
      append: String => Unit,
      collectFallbackFunc: Option[QueryPlan[_] => FallbackInfo] = None): FallbackInfo = synchronized {
    try {
      // Initialize a reference-unique set of Operators to avoid accdiental overwrites and to allow
      // intentional overwriting of IDs generated in previous AQE iteration
      val operators = newSetFromMap[QueryPlan[_]](new util.IdentityHashMap())
      // Initialize an array of ReusedExchanges to help find Adaptively Optimized Out
      // Exchanges as part of SPARK-42753
      val reusedExchanges = ArrayBuffer.empty[ReusedExchangeExec]

      var currentOperatorID = 0
      currentOperatorID =
        generateOperatorIDs(plan, currentOperatorID, operators, reusedExchanges, true)

      val subqueries = ArrayBuffer.empty[(SparkPlan, Expression, BaseSubqueryExec)]
      getSubqueries(plan, subqueries)

      currentOperatorID = subqueries.foldLeft(currentOperatorID) {
        (curId, plan) => generateOperatorIDs(plan._3.child, curId, operators, reusedExchanges, true)
      }

      // SPARK-42753: Process subtree for a ReusedExchange with unknown child
      val optimizedOutExchanges = ArrayBuffer.empty[Exchange]
      reusedExchanges.foreach {
        reused =>
          val child = reused.child
          if (!operators.contains(child)) {
            optimizedOutExchanges.append(child)
            currentOperatorID =
              generateOperatorIDs(child, currentOperatorID, operators, reusedExchanges, false)
          }
      }

      val collectedOperators = BitSet.empty
      processPlanSkippingSubqueries(plan, append, collectedOperators)

      var i = 0
      for (sub <- subqueries) {
        if (i == 0) {
          append("\n===== Subqueries =====\n\n")
        }
        i = i + 1
        append(
          s"Subquery:$i Hosting operator id = " +
            s"${getOpId(sub._1)} Hosting Expression = ${sub._2}\n")

        // For each subquery expression in the parent plan, process its child plan to compute
        // the explain output. In case of subquery reuse, we don't print subquery plan more
        // than once. So we skip [[ReusedSubqueryExec]] here.
        if (!sub._3.isInstanceOf[ReusedSubqueryExec]) {
          processPlanSkippingSubqueries(sub._3.child, append, collectedOperators)
        }
        append("\n")
      }

      i = 0
      optimizedOutExchanges.foreach {
        exchange =>
          if (i == 0) {
            append("\n===== Adaptively Optimized Out Exchanges =====\n\n")
          }
          i = i + 1
          append(s"Subplan:$i\n")
          processPlanSkippingSubqueries[SparkPlan](exchange, append, collectedOperators)
          append("\n")
      }

      (subqueries.filter(!_._3.isInstanceOf[ReusedSubqueryExec]).map(_._3.child) :+ plan)
        .map {
          plan =>
            if (collectFallbackFunc.isEmpty) {
              collectFallbackNodes(plan)
            } else {
              collectFallbackFunc.get.apply(plan)
            }
        }
        .reduce((a, b) => (a._1 + b._1, a._2 ++ b._2))
    } finally {
      removeTags(plan)
    }
  }
  // scalastyle:on
  // spotless:on

  /**
   * Traverses the supplied input plan in a bottom-up fashion and records the operator id via
   * setting a tag in the operator. Note :
   *   - Operator such as WholeStageCodegenExec and InputAdapter are skipped as they don't appear in
   *     the explain output.
   *   - Operator identifier starts at startOperatorID + 1
   *
   * @param plan
   *   Input query plan to process
   * @param startOperatorID
   *   The start value of operation id. The subsequent operations will be assigned higher value.
   * @param visited
   *   A unique set of operators visited by generateOperatorIds. The set is scoped at the callsite
   *   function processPlan. It serves two purpose: Firstly, it is used to avoid accidentally
   *   overwriting existing IDs that were generated in the same processPlan call. Secondly, it is
   *   used to allow for intentional ID overwriting as part of SPARK-42753 where an Adaptively
   *   Optimized Out Exchange and its subtree may contain IDs that were generated in a previous AQE
   *   iteration's processPlan call which would result in incorrect IDs.
   * @param reusedExchanges
   *   A unique set of ReusedExchange nodes visited which will be used to idenitfy adaptively
   *   optimized out exchanges in SPARK-42753.
   * @param addReusedExchanges
   *   Whether to add ReusedExchange nodes to reusedExchanges set. We set it to false to avoid
   *   processing more nested ReusedExchanges nodes in the subtree of an Adpatively Optimized Out
   *   Exchange.
   * @return
   *   The last generated operation id for this input plan. This is to ensure we always assign
   *   incrementing unique id to each operator.
   */
  private def generateOperatorIDs(
      plan: QueryPlan[_],
      startOperatorID: Int,
      visited: util.Set[QueryPlan[_]],
      reusedExchanges: ArrayBuffer[ReusedExchangeExec],
      addReusedExchanges: Boolean): Int = {
    var currentOperationID = startOperatorID
    // Skip the subqueries as they are not printed as part of main query block.
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return currentOperationID
    }

    def setOpId(plan: QueryPlan[_]): Unit = if (!visited.contains(plan)) {
      plan match {
        case r: ReusedExchangeExec if addReusedExchanges =>
          reusedExchanges.append(r)
        case _ =>
      }
      visited.add(plan)
      currentOperationID += 1
      plan.setTagValue(QueryPlan.OP_ID_TAG, currentOperationID)
    }

    plan.foreachUp {
      case _: WholeStageCodegenExec =>
      case _: InputAdapter =>
      case p: AdaptiveSparkPlanExec =>
        currentOperationID = generateOperatorIDs(
          p.executedPlan,
          currentOperationID,
          visited,
          reusedExchanges,
          addReusedExchanges)
        if (!p.executedPlan.fastEquals(p.initialPlan)) {
          currentOperationID = generateOperatorIDs(
            p.initialPlan,
            currentOperationID,
            visited,
            reusedExchanges,
            addReusedExchanges)
        }
        setOpId(p)
      case p: QueryStageExec =>
        currentOperationID = generateOperatorIDs(
          p.plan,
          currentOperationID,
          visited,
          reusedExchanges,
          addReusedExchanges)
        setOpId(p)
      case other: QueryPlan[_] =>
        setOpId(other)
        currentOperationID = other.innerChildren.foldLeft(currentOperationID) {
          (curId, plan) =>
            generateOperatorIDs(plan, curId, visited, reusedExchanges, addReusedExchanges)
        }
    }
    currentOperationID
  }

  /**
   * Given a input plan, returns an array of tuples comprising of :
   *   1. Hosting operator id. 2. Hosting expression 3. Subquery plan
   */
  private def getSubqueries(
      plan: => QueryPlan[_],
      subqueries: ArrayBuffer[(SparkPlan, Expression, BaseSubqueryExec)]): Unit = {
    plan.foreach {
      case a: AdaptiveSparkPlanExec =>
        getSubqueries(a.executedPlan, subqueries)
      case q: QueryStageExec =>
        getSubqueries(q.plan, subqueries)
      case p: SparkPlan =>
        p.expressions.foreach(_.collect {
          case e: PlanExpression[_] =>
            e.plan match {
              case s: BaseSubqueryExec =>
                subqueries += ((p, e, s))
                getSubqueries(s, subqueries)
              case _ =>
            }
        })
    }
  }

  /**
   * Returns the operator identifier for the supplied plan by retrieving the `operationId` tag
   * value.
   */
  private def getOpId(plan: QueryPlan[_]): String = {
    plan.getTagValue(QueryPlan.OP_ID_TAG).map(v => s"$v").getOrElse("unknown")
  }

  private def removeTags(plan: QueryPlan[_]): Unit = {
    def remove(p: QueryPlan[_], children: Seq[QueryPlan[_]]): Unit = {
      p.unsetTagValue(QueryPlan.OP_ID_TAG)
      children.foreach(removeTags)
    }

    plan.foreach {
      case p: AdaptiveSparkPlanExec => remove(p, Seq(p.executedPlan, p.initialPlan))
      case p: QueryStageExec => remove(p, Seq(p.plan))
      case plan: QueryPlan[_] => remove(plan, plan.innerChildren)
    }
  }
}
