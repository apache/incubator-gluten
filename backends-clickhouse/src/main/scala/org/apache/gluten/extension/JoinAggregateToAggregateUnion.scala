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

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.exception._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

private case class JoinKeys(
    leftKeys: Seq[AttributeReference],
    rightKeys: Seq[AttributeReference]) {}

private object RulePlanHelper {
  def transformDistinctToAggregate(distinct: Distinct): Aggregate = {
    Aggregate(distinct.child.output, distinct.child.output, distinct.child)
  }

  def extractDirectAggregate(plan: LogicalPlan): Option[Aggregate] = {
    plan match {
      case _ @SubqueryAlias(_, aggregate: Aggregate) => Some(aggregate)
      case project @ Project(projectList, aggregate: Aggregate) =>
        if (projectList.forall(_.isInstanceOf[AttributeReference])) {
          // Just a column prune projection, ignore it
          Some(aggregate)
        } else {
          None
        }
      case _ @SubqueryAlias(_, distinct: Distinct) =>
        Some(transformDistinctToAggregate(distinct))
      case distinct: Distinct =>
        Some(transformDistinctToAggregate(distinct))
      case aggregate: Aggregate => Some(aggregate)
      case _ => None
    }
  }
}

private object RuleExpressionHelper {

  def extractLiteral(e: Expression): Option[Literal] = {
    e match {
      case literal: Literal => Some(literal)
      case _ @Alias(literal: Literal, _) => Some(literal)
      case _ => None
    }
  }

  def makeFlagLiteral(): Literal = {
    Literal.create(true, BooleanType)
  }

  def makeNullLiteral(dataType: DataType): Literal = {
    Literal.create(null, dataType)
  }

  def makeNamedExpression(e: Expression, name: String): NamedExpression = {
    e match {
      case alias: Alias =>
        Alias(alias.child, name)()
      case _ => Alias(e, name)()
    }
  }

  def makeFirstAggregateExpression(e: Expression): AggregateExpression = {
    val aggregateFunction = First(e, true)
    AggregateExpression(aggregateFunction, Complete, false)
  }

  def extractJoinKeys(
      joinCondition: Option[Expression],
      leftAttributes: AttributeSet,
      rightAttributes: AttributeSet): Option[JoinKeys] = {
    val leftKeys = ArrayBuffer[AttributeReference]()
    val rightKeys = ArrayBuffer[AttributeReference]()
    def visitJoinExpression(e: Expression): Unit = {
      e match {
        case and: And =>
          visitJoinExpression(and.left)
          visitJoinExpression(and.right)
        case equalTo @ EqualTo(left: AttributeReference, right: AttributeReference) =>
          if (leftAttributes.contains(left)) {
            leftKeys += left
          }
          if (leftAttributes.contains(right)) {
            leftKeys += right
          }
          if (rightAttributes.contains(left)) {
            rightKeys += left
          }
          if (rightAttributes.contains(right)) {
            rightKeys += right
          }
        case _ =>
          throw new GlutenException(s"Unsupported join condition $e")
      }
    }

    joinCondition match {
      case Some(condition) =>
        try {
          visitJoinExpression(condition)
          if (leftKeys.length != rightKeys.length) {
            return None
          }
          if (!leftKeys.forall(k => k.qualifier.equals(leftKeys.head.qualifier))) {
            return None
          }
          // They must be the same sets or total different sets
          if (
            leftKeys.length != rightKeys.length ||
            !(leftKeys.forall(key => rightKeys.exists(_.equals(key))) ||
              leftKeys.forall(key => !rightKeys.exists(_.equals(key))))
          ) {
            return None
          }
          // ensure all join keys are in the same order
          val sortedKeys = leftKeys.zip(rightKeys).sortBy { case (leftKey, _) => leftKey.name }
          val leftSortedKeys = sortedKeys.map(_._1)
          val rightSortedKeys = sortedKeys.map(_._2)
          Some(JoinKeys(leftSortedKeys.toSeq, rightSortedKeys.toSeq))
        } catch {
          case e: GlutenException =>
            return None
        }
      case None =>
        return None
    }
  }
}

trait AggregateFunctionAnalyzer {
  def doValidate(): Boolean
  def getArgumentExpressions(): Option[Seq[Expression]]
  def ignoreNulls(): Boolean
  def buildUnionAggregateExpression(
      arguments: Seq[Expression],
      flag: Expression): AggregateExpression
}

case class DefaultAggregateFunctionAnalyzer() extends AggregateFunctionAnalyzer {
  override def doValidate(): Boolean = false
  override def getArgumentExpressions(): Option[Seq[Expression]] = None
  override def ignoreNulls(): Boolean = false
  override def buildUnionAggregateExpression(
      arguments: Seq[Expression],
      flag: Expression): AggregateExpression = {
    throw new GlutenException("Unsupported aggregate function")
  }
}

case class SumAnalyzer(aggregateExpression: AggregateExpression) extends AggregateFunctionAnalyzer {
  val sum = aggregateExpression.aggregateFunction.asInstanceOf[Sum]
  override def doValidate(): Boolean = {
    aggregateExpression.filter.isEmpty
  }

  override def getArgumentExpressions(): Option[Seq[Expression]] = {
    Some(Seq(sum.child))
  }

  override def ignoreNulls(): Boolean = true

  def buildUnionAggregateExpression(
      arguments: Seq[Expression],
      flag: Expression): AggregateExpression = {
    val newSum = sum.copy(child = arguments.head)
    aggregateExpression.copy(aggregateFunction = newSum)
  }
}

case class AverageAnalyzer(aggregateExpression: AggregateExpression)
  extends AggregateFunctionAnalyzer {
  val avg = aggregateExpression.aggregateFunction.asInstanceOf[Average]
  override def doValidate(): Boolean = {
    aggregateExpression.filter.isEmpty
  }

  override def getArgumentExpressions(): Option[Seq[Expression]] = {
    Some(Seq(avg.child))
  }

  override def ignoreNulls(): Boolean = true
  def buildUnionAggregateExpression(
      arguments: Seq[Expression],
      flag: Expression): AggregateExpression = {
    val newAvg = avg.copy(child = arguments.head)
    aggregateExpression.copy(aggregateFunction = newAvg)
  }
}

case class CountAnalyzer(aggregateExpression: AggregateExpression)
  extends AggregateFunctionAnalyzer {
  val count = aggregateExpression.aggregateFunction.asInstanceOf[Count]
  override def doValidate(): Boolean = {
    count.children.length == 1 && aggregateExpression.filter.isEmpty
  }

  override def getArgumentExpressions(): Option[Seq[Expression]] = {
    Some(count.children)
  }

  override def ignoreNulls(): Boolean = false
  def buildUnionAggregateExpression(
      arguments: Seq[Expression],
      flag: Expression): AggregateExpression = {
    val newCount = count.copy(children = arguments)
    aggregateExpression.copy(aggregateFunction = newCount)
  }
}

case class MinAnalyzer(aggregateExpression: AggregateExpression) extends AggregateFunctionAnalyzer {
  val min = aggregateExpression.aggregateFunction.asInstanceOf[Min]
  override def doValidate(): Boolean = {
    aggregateExpression.filter.isEmpty
  }

  override def getArgumentExpressions(): Option[Seq[Expression]] = {
    Some(Seq(min.child))
  }

  override def ignoreNulls(): Boolean = false
  def buildUnionAggregateExpression(
      arguments: Seq[Expression],
      flag: Expression): AggregateExpression = {
    val newMin = min.copy(child = arguments.head)
    aggregateExpression.copy(aggregateFunction = newMin)
  }
}

case class FirstAnalyzer(aggregateExpression: AggregateExpression)
  extends AggregateFunctionAnalyzer {
  val first = aggregateExpression.aggregateFunction.asInstanceOf[First]
  override def doValidate(): Boolean = {
    aggregateExpression.filter.isEmpty && first.ignoreNulls
  }

  override def getArgumentExpressions(): Option[Seq[Expression]] = {
    Some(Seq(first.child))
  }

  override def ignoreNulls(): Boolean = true
  def buildUnionAggregateExpression(
      arguments: Seq[Expression],
      flag: Expression): AggregateExpression = {
    val newFirst = first.copy(child = arguments.head)
    aggregateExpression.copy(aggregateFunction = newFirst)
  }
}

case class MaxAnalyzer(aggregateExpression: AggregateExpression) extends AggregateFunctionAnalyzer {
  val max = aggregateExpression.aggregateFunction.asInstanceOf[Max]
  override def doValidate(): Boolean = {
    aggregateExpression.filter.isEmpty
  }

  override def getArgumentExpressions(): Option[Seq[Expression]] = {
    Some(Seq(max.child))
  }

  override def ignoreNulls(): Boolean = false
  def buildUnionAggregateExpression(
      arguments: Seq[Expression],
      flag: Expression): AggregateExpression = {
    val newMax = max.copy(child = arguments.head)
    aggregateExpression.copy(aggregateFunction = newMax)
  }
}

object AggregateFunctionAnalyzer {
  def apply(e: Expression): AggregateFunctionAnalyzer = {
    val aggExpr = e match {
      case alias @ Alias(aggregate: AggregateExpression, _) =>
        Some(aggregate)
      case aggregate: AggregateExpression =>
        Some(aggregate)
      case _ => None
    }

    aggExpr match {
      case Some(agg) =>
        agg.aggregateFunction match {
          case sum: Sum => SumAnalyzer(agg)
          case avg: Average => AverageAnalyzer(agg)
          case count: Count => CountAnalyzer(agg)
          case max: Max => MaxAnalyzer(agg)
          case min: Min => MinAnalyzer(agg)
          case first: First => FirstAnalyzer(agg)
          case _ => DefaultAggregateFunctionAnalyzer()
        }
      case _ => DefaultAggregateFunctionAnalyzer()
    }
  }
}

case class JoinedAggregateAnalyzer(join: Join, subquery: LogicalPlan) extends Logging {
  def analysis(): Boolean = {
    if (!extractAggregateQuery(subquery)) {
      logDebug(s"xxx Not found aggregate query")
      return false
    }

    if (!extractGroupingKeys()) {
      logDebug(s"xxx Not found grouping keys")
      return false
    }

    if (!extractJoinKeys()) {
      logDebug(s"xxx Not found join keys")
      return false
    }

    if (
      joinKeys.length != aggregate.groupingExpressions.length ||
      !joinKeys.forall(k => outputGroupingKeys.exists(_.semanticEquals(k)))
    ) {
      logDebug(
        s"xxx Join keys and grouping keys are not matched. joinKeys: $joinKeys" +
          s" outputGroupingKeys: $outputGroupingKeys")
      return false
    }

    // Ensure the grouping keys are in the same order as the join keys
    val keysIndex = outputGroupingKeys.map {
      case key => joinKeys.indexWhere(_.semanticEquals(key))
    }
    outputGroupingKeys = outputGroupingKeys.zip(keysIndex).sortBy(_._2).map(_._1)
    groupingExpressions = groupingExpressions.zip(keysIndex).sortBy(_._2).map(_._1)

    aggregateExpressions =
      aggregate.aggregateExpressions.filter(_.find(_.isInstanceOf[AggregateExpression]).isDefined)
    aggregateFunctionAnalyzer = aggregateExpressions.map(AggregateFunctionAnalyzer(_))

    // If there is any const value in the aggregate expressions, return false
    if (aggregateExpressions.length + joinKeys.length != aggregate.aggregateExpressions.length) {
      logDebug(s"xxx Have const expression in aggregate expressions")
      return false
    }
    if (
      aggregateExpressions.zipWithIndex.exists {
        case (e, i) => !aggregateFunctionAnalyzer(i).doValidate
      }
    ) {
      logDebug(s"xxx Have invalid aggregate function in aggregate expressions")
      return false
    }

    val arguments =
      aggregateExpressions.zipWithIndex.map {
        case (_, i) => aggregateFunctionAnalyzer(i).getArgumentExpressions
      }
    if (arguments.exists(_.isEmpty)) {
      logDebug(s"xxx Get aggregate function arguments failed")
      return false
    }
    aggregateExpressionArguments = arguments.map(_.get)

    true
  }

  def extractAggregateQuery(plan: LogicalPlan): Boolean = {
    plan match {
      case _ @SubqueryAlias(_, agg: Aggregate) =>
        aggregate = agg
        true
      case agg: Aggregate =>
        aggregate = agg
        true
      case _ => false
    }
  }

  def getPrimeJoinKeys(): Seq[AttributeReference] = primeJoinKeys
  def getJoinKeys(): Seq[AttributeReference] = joinKeys
  def getAggregate(): Aggregate = aggregate
  def getGroupingKeys(): Seq[Attribute] = outputGroupingKeys
  def getGroupingExpressions(): Seq[NamedExpression] = groupingExpressions
  def getAggregateExpressions(): Seq[NamedExpression] = aggregateExpressions
  def getAggregateExpressionArguments(): Seq[Seq[Expression]] = aggregateExpressionArguments
  def getAggregateFunctionAnalyzers(): Seq[AggregateFunctionAnalyzer] = aggregateFunctionAnalyzer

  private var primeJoinKeys: Seq[AttributeReference] = Seq.empty
  private var joinKeys: Seq[AttributeReference] = Seq.empty
  private var aggregate: Aggregate = null
  private var groupingExpressions: Seq[NamedExpression] = null
  private var outputGroupingKeys: Seq[Attribute] = null
  private var aggregateExpressions: Seq[NamedExpression] = Seq.empty
  private var aggregateExpressionArguments: Seq[Seq[Expression]] = Seq.empty
  private var aggregateFunctionAnalyzer = Seq.empty[AggregateFunctionAnalyzer]

  private def extractJoinKeys(): Boolean = {
    val leftJoinKeys = ArrayBuffer[AttributeReference]()
    val subqueryKeys = ArrayBuffer[AttributeReference]()
    val leftOutputSet = join.left.outputSet
    val subqueryOutputSet = subquery.outputSet
    val joinKeysPair =
      RuleExpressionHelper.extractJoinKeys(join.condition, leftOutputSet, subqueryOutputSet)
    if (joinKeysPair.isEmpty) {
      false
    } else {
      primeJoinKeys = joinKeysPair.get.leftKeys
      joinKeys = joinKeysPair.get.rightKeys
      true
    }
  }

  def extractGroupingKeys(): Boolean = {
    val outputGroupingKeysBuffer = ArrayBuffer[Attribute]()
    val groupingExpressionsBuffer = ArrayBuffer[NamedExpression]()
    val indexedAggregateExpressions = aggregate.aggregateExpressions.zipWithIndex
    val aggregateOuput = aggregate.output

    def findMatchedAggregateExpression(e: Expression): Option[Tuple2[NamedExpression, Int]] = {
      indexedAggregateExpressions.find {
        case (aggExpr, i) =>
          aggExpr match {
            case alias: Alias =>
              alias.child.semanticEquals(e) || alias.semanticEquals(e)
            case _ => aggExpr.semanticEquals(e)
          }
      }
    }
    aggregate.groupingExpressions.map {
      e =>
        e match {
          case nameExpression: NamedExpression =>
            groupingExpressionsBuffer += nameExpression
            findMatchedAggregateExpression(nameExpression) match {
              case Some((aggExpr, i)) =>
                outputGroupingKeysBuffer += aggregateOuput(i)
              case None =>
                return false
            }
          case other =>
            findMatchedAggregateExpression(other) match {
              case Some((aggExpr, i)) =>
                outputGroupingKeysBuffer += aggregateOuput(i)
                groupingExpressionsBuffer += aggregate.aggregateExpressions(i)
              case None =>
                return false
            }
        }
    }
    outputGroupingKeys = outputGroupingKeysBuffer.toSeq
    groupingExpressions = groupingExpressionsBuffer.toSeq
    true
  }
}

object JoinedAggregateAnalyzer extends Logging {
  def build(join: Join, subquery: LogicalPlan): Option[JoinedAggregateAnalyzer] = {
    val analyzer = JoinedAggregateAnalyzer(join, subquery)
    if (analyzer.analysis()) {
      Some(analyzer)
    } else {
      None
    }
  }

  def haveSamePrimeJoinKeys(analzyers: Seq[JoinedAggregateAnalyzer]): Boolean = {
    val primeKeys = analzyers.map(_.getPrimeJoinKeys()).map(AttributeSet(_))
    primeKeys
      .slice(1, primeKeys.length)
      .forall(keys => keys.equals(primeKeys.head))
  }
}

/**
 * ReorderJoinSubqueries is step before JoinAggregateToAggregateUnion. It will reorder the join
 * subqueries to move the queries with the same prime keys, join keys and grouping keys together.
 * For example
 * ```
 *  select t1.k1, t1.k2, s1, s2 from (
 *    select k1, k2, count(v1) s1 from t1 group by k1, k2
 *  ) t1 left join (
 *    select * from t2
 *  ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
 *  left join (
 *    select k1, k2, count(v2) s2 from t3 group by k1, k2
 *  ) t3 on t1.k1 = t3.k1 and t1.k2 = t3.k2
 * ```
 * is rewritten into
 * ```
 *  select t1.k1, t1.k2, s1, s2 from (
 *    select k1, k2, count(v1) s1 from t1 group by k1, k2
 *  ) t1 left join (
 *    select k1, k2, count(v2) s2 from t3 group by k1, k2
 *  ) t3 on t1.k1 = t3.k1 and t1.k2 = t3.k2
 *  ) left join (
 *    select * from t2
 *  ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
 * ```
 */
case class ReorderJoinSubqueries() extends Logging {
  case class SameJoinKeysPlans(
      primeKeys: AttributeSet,
      plans: ListBuffer[Tuple2[LogicalPlan, Join]]) {}
  def apply(plan: LogicalPlan): LogicalPlan = {
    visitPlan(plan)
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case join: Join if join.joinType == LeftOuter && join.condition.isDefined =>
        val sameJoinKeysPlansList = ListBuffer[SameJoinKeysPlans]()
        val plan = visitJoin(join, sameJoinKeysPlansList)
        finishReorderJoinPlans(plan, sameJoinKeysPlansList)
      case _ =>
        plan.withNewChildren(plan.children.map(visitPlan))
    }
  }

  def findSameJoinKeysPlansIndex()(
      sameJoinKeysPlans: ListBuffer[SameJoinKeysPlans],
      primeKeys: Seq[AttributeReference]): Int = {
    val keysSet = AttributeSet(primeKeys)
    for (i <- 0 until sameJoinKeysPlans.length) {
      if (sameJoinKeysPlans(i).primeKeys.equals(keysSet)) {
        return i
      }
    }
    -1
  }

  def finishReorderJoinPlans(
      left: LogicalPlan,
      sameJoinKeysPlansList: ListBuffer[SameJoinKeysPlans]): LogicalPlan = {
    var finalPlan = left
    for (j <- sameJoinKeysPlansList.length - 1 to 0 by -1) {
      for (i <- sameJoinKeysPlansList(j).plans.length - 1 to 0 by -1) {
        val plan = sameJoinKeysPlansList(j).plans(i)._1
        val originalJoin = sameJoinKeysPlansList(j).plans(i)._2
        finalPlan = originalJoin.copy(left = finalPlan, right = plan)
      }
    }
    finalPlan
  }

  def visitJoin(
      plan: LogicalPlan,
      sameJoinKeysPlansList: ListBuffer[SameJoinKeysPlans]): LogicalPlan = {
    plan match {
      case join: Join if join.joinType == LeftOuter && join.condition.isDefined =>
        val joinKeys = RuleExpressionHelper.extractJoinKeys(
          join.condition,
          join.left.outputSet,
          join.right.outputSet)
        joinKeys match {
          case Some(keys) =>
            val index = findSameJoinKeysPlansIndex()(sameJoinKeysPlansList, keys.leftKeys)
            val newRight = visitPlan(join.right)
            if (index == -1) {
              sameJoinKeysPlansList += SameJoinKeysPlans(
                AttributeSet(keys.leftKeys),
                ListBuffer(Tuple2(newRight, join)))
            } else {
              if (index != sameJoinKeysPlansList.length - 1) {}
              val sameJoinKeysPlans = sameJoinKeysPlansList.remove(index)
              if (
                RulePlanHelper.extractDirectAggregate(newRight).isDefined ||
                RulePlanHelper.extractDirectAggregate(sameJoinKeysPlans.plans.last._1).isEmpty
              ) {
                sameJoinKeysPlans.plans += Tuple2(newRight, join)
              } else {
                sameJoinKeysPlans.plans.insert(0, Tuple2(newRight, join))
              }
              sameJoinKeysPlansList += sameJoinKeysPlans
            }
            visitJoin(join.left, sameJoinKeysPlansList)
          case None =>
            val joinLeft = visitPlan(join.left)
            val joinRight = visitPlan(join.right)
            join.copy(left = joinLeft, right = joinRight)
        }
      case subquery: SubqueryAlias if RulePlanHelper.extractDirectAggregate(subquery).isDefined =>
        val newAggregate = visitPlan(subquery.child)
        val groupingKeys = RulePlanHelper.extractDirectAggregate(subquery).get.groupingExpressions
        if (groupingKeys.forall(_.isInstanceOf[AttributeReference])) {
          val keys = groupingKeys.map(_.asInstanceOf[AttributeReference])
          val index = findSameJoinKeysPlansIndex()(sameJoinKeysPlansList, keys)
          if (index != -1 && index != sameJoinKeysPlansList.length - 1) {
            val sameJoinKeysPlans = sameJoinKeysPlansList.remove(index)
            sameJoinKeysPlansList += sameJoinKeysPlans
          }
        }
        subquery.copy(child = newAggregate)
      case aggregate: Aggregate =>
        val newAggregate = aggregate.withNewChildren(aggregate.children.map(visitPlan))
        val groupingKeys = aggregate.groupingExpressions
        if (groupingKeys.forall(_.isInstanceOf[AttributeReference])) {
          val keys = groupingKeys.map(_.asInstanceOf[AttributeReference])
          val index = findSameJoinKeysPlansIndex()(sameJoinKeysPlansList, keys)
          if (index != -1 && index != sameJoinKeysPlansList.length - 1) {
            val sameJoinKeysPlans = sameJoinKeysPlansList.remove(index)
            sameJoinKeysPlansList += sameJoinKeysPlans
          }
        }
        newAggregate
      case _ =>
        plan.withNewChildren(plan.children.map(visitPlan))
    }
  }

}

case class JoinAggregateToAggregateUnion(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {
  def isResolvedPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case insert: InsertIntoStatement => insert.query.resolved
      case _ => plan.resolved
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // It's experimental feature, disable it by default
    if (
      spark.conf
        .get(CHBackendSettings.GLUTEN_JOIN_AGGREGATE_TO_AGGREGATE_UNION, "false")
        .toBoolean && isResolvedPlan(plan)
    ) {
      val reorderedPlan = ReorderJoinSubqueries().apply(plan)
      val newPlan = visitPlan(reorderedPlan)
      logDebug(s"Rewrite plan from \n$plan to \n$newPlan")
      newPlan
    } else {
      plan
    }
  }

  /**
   * Use wide-table aggregation to eliminate multi-table joins. For example
   * ```
   *   select k1, k2, s1, s2 from (select k1, sum(a) as s1 from t1 group by k1) as t1
   *   left join (select k2, sum(b) as s2 from t2 group by k2) as t2
   *   on t1.k1 = t2.k2
   * ```
   * It's rewritten into
   * ```
   *   select k1, k2, s1, s2 from(
   *     select k1, k2, s1, if(isNull(flag2), null, s2) as s2 from (
   *        select k1, first(k2) as k2, sum(a) as s1, sum(b) as s2, first(flag1) as flag1,
   *               first(flag2) as flag2
   *        from (
   *          select k1, null as k2, a as a, null as b, true as flag1, null as flag2 from t1
   *          union all
   *          select k2, k2, null as a, b as b, null as flag1, true as flag2 from t2
   *        ) group by k1
   *     ) where flag1 is not null
   *   )
   * ```
   *
   * The first query is easier to write, but not as efficient as the second one.
   */
  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case join: Join =>
        if (join.joinType == LeftOuter && join.condition.isDefined) {
          val analyzedAggregates = ArrayBuffer[JoinedAggregateAnalyzer]()
          val remainedPlan = collectSameKeysJoinedAggregates(join, analyzedAggregates)
          if (analyzedAggregates.length == 0) {
            join.copy(left = visitPlan(join.left), right = visitPlan(join.right))
          } else if (analyzedAggregates.length == 1) {
            join.copy(left = visitPlan(join.left))
          } else {
            val unionedAggregates = unionAllJoinedAggregates(analyzedAggregates.toSeq)
            if (remainedPlan.isDefined) {
              // The left join clause is not an aggregate query.

              // Use the new right keys to build the join condition
              val newRightKeys =
                unionedAggregates.output.slice(0, analyzedAggregates.head.getPrimeJoinKeys.length)
              val newJoinCondition =
                buildJoinCondition(analyzedAggregates.head.getPrimeJoinKeys(), newRightKeys)
              val lastJoin = analyzedAggregates.head.join
              lastJoin.copy(
                left = visitPlan(lastJoin.left),
                right = unionedAggregates,
                condition = Some(newJoinCondition))
            } else {
              /*
               * The left join clause is also a aggregate query.
               * If flag_0 is null, filter out the rows.
               */
              buildPrimeJoinKeysFilterOnAggregateUnion(unionedAggregates, analyzedAggregates.toSeq)
            }
          }
        } else {
          plan.withNewChildren(plan.children.map(visitPlan))
        }
      case _ => plan.withNewChildren(plan.children.map(visitPlan))
    }
  }

  def unionAllJoinedAggregates(analyzedAggregates: Seq[JoinedAggregateAnalyzer]): LogicalPlan = {
    val extendProjects = buildExtendProjects(analyzedAggregates)
    val union = buildUnionOnExtendedProjects(extendProjects)
    // The output is {keys_0}, {keys_1}, ..., {aggs_0}, {agg_1}, ... , flag_0, flag_1, ...
    val aggregateUnion = buildAggregateOnUnion(union, analyzedAggregates)
    // Push a duplication of {kyes_0} to the head. This keys will be used as join keys later.
    val duplicateRightKeysProject =
      buildJoinRightKeysProject(aggregateUnion, analyzedAggregates)
    /*
     * If flag_0 is null, let {keys_0} be null too.
     * If flag_i is null, let {aggs_i} be null too.
     */
    val setNullsProject =
      buildMakeNotMatchedRowsNullProject(duplicateRightKeysProject, analyzedAggregates, Set())
    buildRenameProject(setNullsProject, analyzedAggregates)
  }

  def buildAggregateOnUnion(
      union: LogicalPlan,
      analyzedAggregates: Seq[JoinedAggregateAnalyzer]): LogicalPlan = {
    val unionOutput = union.output
    val keysNumber = analyzedAggregates.head.getGroupingKeys().length
    val aggregateExpressions = ArrayBuffer[NamedExpression]()

    val groupingKeys =
      unionOutput.slice(0, keysNumber).zip(analyzedAggregates.head.getGroupingKeys()).map {
        case (e, a) =>
          RuleExpressionHelper.makeNamedExpression(e, a.name)
      }
    aggregateExpressions ++= groupingKeys

    for (i <- 1 until analyzedAggregates.length) {
      val keys = analyzedAggregates(i).getGroupingKeys()
      val fieldIndex = i * keysNumber
      for (j <- 0 until keys.length) {
        val key = keys(j)
        val valueExpr = unionOutput(fieldIndex + j)
        val firstValue = RuleExpressionHelper.makeFirstAggregateExpression(valueExpr)
        aggregateExpressions += RuleExpressionHelper.makeNamedExpression(firstValue, key.name)
      }
    }

    var fieldIndex = keysNumber * analyzedAggregates.length
    for (i <- 0 until analyzedAggregates.length) {
      val partialAggregateExpressions = analyzedAggregates(i).getAggregateExpressions()
      val partialAggregateArguments = analyzedAggregates(i).getAggregateExpressionArguments()
      val aggregateFunctionAnalyzers = analyzedAggregates(i).getAggregateFunctionAnalyzers()
      for (j <- 0 until partialAggregateExpressions.length) {
        val aggregateExpression = partialAggregateExpressions(j)
        val arguments = partialAggregateArguments(j)
        val aggregateFunctionAnalyzer = aggregateFunctionAnalyzers(j)
        val newArguments = unionOutput.slice(fieldIndex, fieldIndex + arguments.length)
        val flagExpr = unionOutput(unionOutput.length - analyzedAggregates.length + i)
        val newAggregateExpression = aggregateFunctionAnalyzer
          .buildUnionAggregateExpression(newArguments, flagExpr)
        aggregateExpressions += RuleExpressionHelper.makeNamedExpression(
          newAggregateExpression,
          aggregateExpression.name)
        fieldIndex += arguments.length
      }
    }

    for (i <- fieldIndex until unionOutput.length) {
      val valueExpr = unionOutput(i)
      val firstValue = RuleExpressionHelper.makeFirstAggregateExpression(valueExpr)
      aggregateExpressions += RuleExpressionHelper.makeNamedExpression(firstValue, valueExpr.name)
    }

    Aggregate(groupingKeys, aggregateExpressions.toSeq, union)
  }

  /**
   * Some rows may come from the right tables which grouping keys are not in the prime keys set. We
   * should remove them.
   */
  def buildPrimeJoinKeysFilterOnAggregateUnion(
      plan: LogicalPlan,
      analyzedAggregates: Seq[JoinedAggregateAnalyzer]): LogicalPlan = {
    val flagExpressions = plan.output(plan.output.length - analyzedAggregates.length)
    val notNullExpr = IsNotNull(flagExpressions)
    Filter(notNullExpr, plan)
  }

  /**
   * When a row's grouping keys are not present in the related table, the related aggregate values
   * are replaced with nulls.
   */
  def buildMakeNotMatchedRowsNullProject(
      plan: LogicalPlan,
      analyzedAggregates: Seq[JoinedAggregateAnalyzer],
      ignoreAggregates: Set[Int]): LogicalPlan = {
    val input = plan.output

    val keysNumber = analyzedAggregates.head.getGroupingKeys().length
    val keysStartIndex = keysNumber
    val aggregateExpressionsStatIndex = keysStartIndex + analyzedAggregates.length * keysNumber
    val flagExpressionsStartIndex = input.length - analyzedAggregates.length

    val dupPrimeKeys = input.slice(0, keysStartIndex)
    val keys = input.slice(keysStartIndex, aggregateExpressionsStatIndex)
    val aggregateExpressions = input.slice(aggregateExpressionsStatIndex, flagExpressionsStartIndex)
    val flagExpressions = input.slice(flagExpressionsStartIndex, input.length)

    val newProjectList = ArrayBuffer[NamedExpression]()
    newProjectList ++= dupPrimeKeys
    val newFirstClauseKeys = keys.slice(0, keysNumber).map {
      case key =>
        val ifNull =
          If(IsNull(flagExpressions(0)), RuleExpressionHelper.makeNullLiteral(key.dataType), key)
        RuleExpressionHelper.makeNamedExpression(ifNull, key.name)
    }
    newProjectList ++= newFirstClauseKeys
    newProjectList ++= keys.slice(keysNumber, keys.length)

    var fieldIndex = 0
    val newAggregateExpressions = analyzedAggregates.zipWithIndex.map {
      case (analyzedAggregate, i) =>
        val localAggregateExpressions = analyzedAggregate.getAggregateExpressions()
        val aggregateFunctionAnalyzers = analyzedAggregate.getAggregateFunctionAnalyzers()
        localAggregateExpressions.zipWithIndex.map {
          case (e, j) =>
            val aggregateExpr = aggregateExpressions(fieldIndex)
            fieldIndex += 1

            if (ignoreAggregates.contains(i) || aggregateFunctionAnalyzers(j).ignoreNulls()) {
              aggregateExpr.asInstanceOf[NamedExpression]
            } else {
              val ifNullExpr = If(
                IsNull(flagExpressions(i)),
                RuleExpressionHelper.makeNullLiteral(aggregateExpr.dataType),
                aggregateExpr)
              RuleExpressionHelper.makeNamedExpression(ifNullExpr, aggregateExpr.name)
            }
        }
    }
    newProjectList ++= newAggregateExpressions.flatten
    newProjectList ++= flagExpressions
    Project(newProjectList.toSeq, plan)
  }

  /**
   * A final step, ensure the output attributes have the same name and exprId as the original join
   */
  def buildRenameProject(
      plan: LogicalPlan,
      analyzedAggregates: Seq[JoinedAggregateAnalyzer]): LogicalPlan = {
    val input = plan.output
    val projectList = ArrayBuffer[NamedExpression]()
    val keysNum = analyzedAggregates.head.getGroupingKeys.length
    projectList ++= input.slice(0, keysNum)
    var fieldIndex = keysNum
    for (i <- 0 until analyzedAggregates.length) {
      val keys = analyzedAggregates(i).getGroupingKeys()
      for (j <- 0 until keys.length) {
        val key = keys(j)
        projectList += Alias(input(fieldIndex), key.name)(
          key.exprId,
          key.qualifier,
          None,
          Seq.empty)
        fieldIndex += 1
      }
    }

    for (i <- 0 until analyzedAggregates.length) {
      val aggregateExpressions = analyzedAggregates(i).getAggregateExpressions()
      for (j <- 0 until aggregateExpressions.length) {
        val e = aggregateExpressions(j)
        val valueExpr = input(fieldIndex)
        projectList += Alias(valueExpr, e.name)(e.exprId, e.qualifier, None, Seq.empty)
        fieldIndex += 1
      }
    }

    // Keep the flag columns
    projectList ++= input.slice(input.length - analyzedAggregates.length, input.length)
    Project(projectList.toSeq, plan)
  }

  def buildJoinCondition(leftKeys: Seq[Attribute], rightKeys: Seq[Attribute]): Expression = {
    leftKeys
      .zip(rightKeys)
      .map {
        case (leftKey, rightKey) =>
          EqualTo(leftKey, rightKey)
      }
      .reduceLeft(And)
  }

  def buildJoinRightKeysProject(
      plan: LogicalPlan,
      analyzedAggregates: Seq[JoinedAggregateAnalyzer]): LogicalPlan = {
    val input = plan.output
    val keysNum = analyzedAggregates.head.getGroupingKeys.length
    // Make a duplication of the prime aggregate keys, and put them in the front
    val projectList = input.slice(0, keysNum).map {
      case key =>
        RuleExpressionHelper.makeNamedExpression(key, "_dup_prime_" + key.name)
    }
    Project(projectList ++ input, plan)
  }

  /**
   * Build a extended project list, which contains three parts.
   *   - The grouping keys of all tables.
   *   - All required columns as arguments for the aggregate functions in every table.
   *   - Flags for each table to indicate whether the row is in the table.
   */
  def buildExtendProjectList(
      analyzedAggregates: Seq[JoinedAggregateAnalyzer],
      index: Int): Seq[NamedExpression] = {
    val projectList = ArrayBuffer[NamedExpression]()
    projectList ++= analyzedAggregates(index).getGroupingExpressions().zipWithIndex.map {
      case (e, i) =>
        RuleExpressionHelper.makeNamedExpression(e, s"key_0_$i")
    }
    for (i <- 1 until analyzedAggregates.length) {
      val groupingKeys = analyzedAggregates(i).getGroupingExpressions()
      projectList ++= groupingKeys.zipWithIndex.map {
        case (e, j) =>
          if (i == index) {
            RuleExpressionHelper.makeNamedExpression(e, s"key_${i}_$j")
          } else {
            RuleExpressionHelper.makeNamedExpression(
              RuleExpressionHelper.makeNullLiteral(e.dataType),
              s"key_${i}_$j")
          }
      }
    }

    for (i <- 0 until analyzedAggregates.length) {
      val argsList = analyzedAggregates(i).getAggregateExpressionArguments()
      argsList.zipWithIndex.foreach {
        case (args, j) =>
          projectList ++= args.zipWithIndex.map {
            case (arg, k) =>
              if (i == index) {
                RuleExpressionHelper.makeNamedExpression(arg, s"arg_${i}_${j}_$k")
              } else {
                RuleExpressionHelper.makeNamedExpression(
                  RuleExpressionHelper.makeNullLiteral(arg.dataType),
                  s"arg_${i}_${j}_$k")
              }
          }
      }
    }

    for (i <- 0 until analyzedAggregates.length) {
      if (i == index) {
        projectList += RuleExpressionHelper.makeNamedExpression(
          RuleExpressionHelper.makeFlagLiteral(),
          s"flag_$i")
      } else {
        projectList += RuleExpressionHelper.makeNamedExpression(
          RuleExpressionHelper.makeNullLiteral(BooleanType),
          s"flag_$i")
      }
    }

    projectList.toSeq
  }

  def buildExtendProjects(analyzedAggregates: Seq[JoinedAggregateAnalyzer]): Seq[LogicalPlan] = {
    val projects = ArrayBuffer[LogicalPlan]()
    for (i <- 0 until analyzedAggregates.length) {
      val projectList = buildExtendProjectList(analyzedAggregates, i)
      projects += Project(projectList, analyzedAggregates(i).getAggregate().child)
    }
    projects.toSeq
  }

  def buildUnionOnExtendedProjects(plans: Seq[LogicalPlan]): LogicalPlan = {
    val union = Union(plans)
    logDebug(s"xxx build union: $union")
    union
  }

  def collectSameKeysJoinedAggregates(
      plan: LogicalPlan,
      analyzedAggregates: ArrayBuffer[JoinedAggregateAnalyzer]): Option[LogicalPlan] = {
    plan match {
      case join: Join if join.joinType == LeftOuter && join.condition.isDefined =>
        val optionAggregate = RulePlanHelper.extractDirectAggregate(join.right)
        if (optionAggregate.isEmpty) {
          return Some(plan)
        }
        val rightAggregateAnalyzer = JoinedAggregateAnalyzer.build(join, optionAggregate.get)
        if (rightAggregateAnalyzer.isEmpty) {
          logDebug(s"xxx Not a valid aggregate query")
          return Some(plan)
        }

        if (
          analyzedAggregates.isEmpty ||
          JoinedAggregateAnalyzer.haveSamePrimeJoinKeys(
            Seq(analyzedAggregates.head, rightAggregateAnalyzer.get))
        ) {
          // left plan is pushed in front
          analyzedAggregates.insert(0, rightAggregateAnalyzer.get)
          collectSameKeysJoinedAggregates(join.left, analyzedAggregates)
        } else {
          logDebug(
            s"xxx Not have same keys. join keys:" +
              s"${analyzedAggregates.head.getPrimeJoinKeys()} vs. " +
              s"${rightAggregateAnalyzer.get.getPrimeJoinKeys()}")
          Some(plan)
        }
      case _ if RulePlanHelper.extractDirectAggregate(plan).isDefined =>
        val aggregate = RulePlanHelper.extractDirectAggregate(plan).get
        val lastJoin = analyzedAggregates.head.join
        assert(lastJoin.left.equals(plan), "The node should be last join's left child")
        val leftAggregateAnalyzer = JoinedAggregateAnalyzer.build(lastJoin, aggregate)
        if (leftAggregateAnalyzer.isEmpty) {
          return Some(plan)
        }
        if (
          JoinedAggregateAnalyzer.haveSamePrimeJoinKeys(
            Seq(analyzedAggregates.head, leftAggregateAnalyzer.get))
        ) {
          analyzedAggregates.insert(0, leftAggregateAnalyzer.get)
          None
        } else {
          Some(plan)
        }
      case _ => Some(plan)
    }
  }

  def collectSameKeysJoinSubqueries(
      plan: LogicalPlan,
      groupedAggregates: ArrayBuffer[ArrayBuffer[JoinedAggregateAnalyzer]],
      nonAggregates: ArrayBuffer[LogicalPlan]): Option[LogicalPlan] = {
    plan match {
      case join: Join if join.joinType == LeftOuter && join.condition.isDefined =>
      case _ => Some(plan)
    }
    None
  }

}
