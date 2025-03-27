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

trait AggregateFunctionAnalyzer {
  def doValidate(): Boolean
  def getArgumentExpressions(): Option[Seq[Expression]]
}

case class DefaultAggregateFunctionAnalyzer() extends AggregateFunctionAnalyzer {
  override def doValidate(): Boolean = false
  override def getArgumentExpressions(): Option[Seq[Expression]] = None
}

case class SumAnalyzer(aggExpr: AggregateExpression) extends AggregateFunctionAnalyzer {
  val sum = aggExpr.aggregateFunction.asInstanceOf[Sum]
  override def doValidate(): Boolean = {
    !sum.child.isInstanceOf[Literal]
  }

  override def getArgumentExpressions(): Option[Seq[Expression]] = {
    Some(Seq(sum.child))
  }
}

case class CountAnalyzer(aggExpr: AggregateExpression) extends AggregateFunctionAnalyzer {
  val count = aggExpr.aggregateFunction.asInstanceOf[Count]
  override def doValidate(): Boolean = {
    count.children.length == 1 && !count.children.head.isInstanceOf[Literal]
  }

  override def getArgumentExpressions(): Option[Seq[Expression]] = {
    Some(count.children)
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
          case count: Count => CountAnalyzer(agg)
          case _ => DefaultAggregateFunctionAnalyzer()
        }
      case _ => DefaultAggregateFunctionAnalyzer()
    }
  }
}

case class JoinedAggregateAnalyzer(join: Join, subquery: LogicalPlan) extends Logging {
  def analysis(): Boolean = {
    if (!extractAggregateQuery(subquery)) {
      return false
    }

    if (!extractJoinKeys()) {
      return false
    }

    aggregateExpressions =
      aggregate.aggregateExpressions.filter(_.find(_.isInstanceOf[AggregateExpression]).isDefined)
    if (aggregateExpressions.exists(!AggregateFunctionAnalyzer(_).doValidate)) {
      return false
    }

    val arguments =
      aggregateExpressions.map(AggregateFunctionAnalyzer(_).getArgumentExpressions)
    if (arguments.exists(_.isEmpty)) {
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

  def getPrimeKeys(): Seq[AttributeReference] = primeKeys
  def getKeys(): Seq[AttributeReference] = keys
  def getAggregate(): Aggregate = aggregate
  def getAggregateExpressions(): Seq[NamedExpression] = aggregateExpressions
  def getAggregateExpressionArguments(): Seq[Seq[Expression]] = aggregateExpressionArguments

  private var primeKeys: Seq[AttributeReference] = Seq.empty
  private var keys: Seq[AttributeReference] = Seq.empty
  private var aggregate: Aggregate = null
  private var aggregateExpressions: Seq[NamedExpression] = Seq.empty
  private var aggregateExpressionArguments: Seq[Seq[Expression]] = Seq.empty

  private def extractJoinKeys(): Boolean = {
    val leftJoinKeys = ArrayBuffer[AttributeReference]()
    val subqueryKeys = ArrayBuffer[AttributeReference]()
    val leftOutputSet = join.left.outputSet
    val subqueryOutputSet = subquery.outputSet
    def visitJoinExpression(e: Expression): Unit = {
      e match {
        case and: And =>
          visitJoinExpression(and.left)
          visitJoinExpression(and.right)
        case equalTo @ EqualTo(left: AttributeReference, right: AttributeReference) =>
          if (leftOutputSet.contains(left)) {
            leftJoinKeys += left
          }
          if (subqueryOutputSet.contains(left)) {
            subqueryKeys += left
          }
          if (leftOutputSet.contains(right)) {
            leftJoinKeys += right
          }
          if (subqueryOutputSet.contains(right)) {
            subqueryKeys += right
          }
        case _ =>
          throw new GlutenException(s"Unsupported join condition $e")
      }
    }

    // They must be the same sets or total different sets
    if (
      leftJoinKeys.length != subqueryKeys.length ||
      !(leftJoinKeys.forall(key => subqueryKeys.exists(_.equals(key))) ||
        leftJoinKeys.forall(key => !subqueryKeys.exists(_.equals(key))))
    ) {
      return false
    }
    join.condition match {
      case Some(condition) =>
        try {
          visitJoinExpression(condition)
        } catch {
          case e: GlutenException =>
            return false
        }
        keys = subqueryKeys.toSeq
        primeKeys = leftJoinKeys.toSeq
        true
      case _ => false
    }
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

  def haveSamePrimeKeys(analzyers: Seq[JoinedAggregateAnalyzer]): Boolean = {
    val primeKeys = analzyers.map(_.getPrimeKeys()).map(AttributeSet(_))
    primeKeys
      .slice(1, primeKeys.length)
      .forall(keys => keys.equals(primeKeys.head))
  }
}

case class JoinAggregateToAggregateUnion(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {
  val JOIN_FILTER_FLAG_NAME = "_left_flag_"
  def isResolvedPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case insert: InsertIntoStatement => insert.query.resolved
      case _ => plan.resolved
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      spark.conf
        .get(CHBackendSettings.GLUTEN_JOIN_AGGREGATE_TO_AGGREGATE_UNION, "true")
        .toBoolean && isResolvedPlan(plan)
    ) {
      val newPlan = visitPlan(plan)
      logDebug(s"old plan\n$plan\nnew plan\n$newPlan")
      newPlan
    } else {
      plan
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case join: Join =>
        if (join.joinType == LeftOuter && join.condition.isDefined) {
          (join.left, join.right) match {
            case (left, right) if isDirectAggregate(left) && isDirectAggregate(right) =>
              val leftAggregate = extractDirectAggregate(left).get
              val rightAggregate = extractDirectAggregate(right).get
              logDebug(s"xxx case 1. left agg:\n$leftAggregate,\nright agg:\n$rightAggregate")
              rewriteOnlyTwoAggregatesJoin(leftAggregate, rightAggregate, join) match {
                case Some(newPlan) =>
                  newPlan.withNewChildren(newPlan.children.map(visitPlan))
                case _ =>
                  plan.withNewChildren(plan.children.map(visitPlan))
              }
            case (left, right) if isDirectJoin(left) && isDirectAggregate(right) =>
              logDebug(s"xxx case 2")
              val analyzedAggregates = ArrayBuffer[JoinedAggregateAnalyzer]()
              val remainedPlan = collectSameKeysJoinedAggregates(join, analyzedAggregates)
              logDebug(s"xxx join left\n$remainedPlan")
              logDebug(s"xxx analyzed aggregates number ${analyzedAggregates.length}")
              val unionedAggregates = unionAllJoinedAggregates(analyzedAggregates.toSeq)
              val finalPlan =
                if (analyzedAggregates.length == 0) {
                  join.copy(left = visitPlan(join.left), right = visitPlan(join.right))
                } else if (analyzedAggregates.length == 1) {
                  join.copy(left = visitPlan(join.left))
                } else if (remainedPlan.isDefined) {
                  val lastJoin = analyzedAggregates.last.join
                  lastJoin.copy(left = visitPlan(lastJoin.left), right = unionedAggregates)
                } else {
                  buildPrimeKeysFilterOnAggregateUnion(unionedAggregates, analyzedAggregates)
                }
              logDebug(s"xxx final rewritten plan\n$finalPlan")
              finalPlan
            case _ =>
              logDebug(s"xxx case 3.left\n${join.left}\nright\n${join.right}")
              plan.withNewChildren(plan.children.map(visitPlan))
          }
        } else {
          plan.withNewChildren(plan.children.map(visitPlan))
        }
      case _ => plan.withNewChildren(plan.children.map(visitPlan))
    }
  }

  /**
   * Rewrite the plan if it is a join between two aggregate tables. For example
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
  def rewriteOnlyTwoAggregatesJoin(
      leftAggregate: Aggregate,
      rightAggregate: Aggregate,
      join: Join): Option[LogicalPlan] = {
    val optionAnalyzers = Seq(leftAggregate, rightAggregate).map(
      agg => JoinedAggregateAnalyzer.build(join, agg.asInstanceOf[LogicalPlan]))
    if (optionAnalyzers.exists(_.isEmpty)) {
      return None
    }
    val analyzers = optionAnalyzers.map(_.get)
    if (!JoinedAggregateAnalyzer.haveSamePrimeKeys(analyzers)) {
      return None
    }

    val extendProjects = buildExtendProjects(analyzers)

    val union = buildUnionOnExtendedProjects(extendProjects)
    val aggregateUnion = buildAggregateOnUnion(union, analyzers)
    val filtAggregateUnion = buildPrimeKeysFilterOnAggregateUnion(aggregateUnion, analyzers)
    val setNullsProject = buildMakeNotMatchedRowsNullProject(filtAggregateUnion, analyzers, Set(0))
    val renameProject = buildRenameProject(setNullsProject, analyzers)
    logDebug(s"xxx rename project\n$renameProject")
    logDebug(s"xxx rename outputs ${renameProject.output}")
    Some(renameProject)
  }

  def buildAggregateExpressionWithNewChildren(
      ne: NamedExpression,
      inputs: Seq[Attribute]): NamedExpression = {
    val aggregateExpression =
      ne.asInstanceOf[Alias].child.asInstanceOf[AggregateExpression]
    val newAggregateFunction = aggregateExpression.aggregateFunction
      .withNewChildren(inputs)
      .asInstanceOf[AggregateFunction]
    val newAggregateExpression = aggregateExpression.copy(aggregateFunction = newAggregateFunction)
    makeNamedExpression(newAggregateExpression, ne.name)
  }

  def unionAllJoinedAggregates(analyzedAggregates: Seq[JoinedAggregateAnalyzer]): LogicalPlan = {
    val extendProjects = buildExtendProjects(analyzedAggregates)
    val union = buildUnionOnExtendedProjects(extendProjects)
    val aggregateUnion = buildAggregateOnUnion(union, analyzedAggregates)
    val setNullsProject =
      buildMakeNotMatchedRowsNullProject(aggregateUnion, analyzedAggregates, Set())
    buildRenameProject(setNullsProject, analyzedAggregates)
  }

  def buildAggregateOnUnion(
      union: LogicalPlan,
      analyzedAggregates: Seq[JoinedAggregateAnalyzer]): LogicalPlan = {
    val unionOutput = union.output
    val keysNumber = analyzedAggregates.head.getKeys().length
    val aggregateExpressions = ArrayBuffer[NamedExpression]()

    val groupingKeys = unionOutput.slice(0, keysNumber).zip(analyzedAggregates.head.getKeys()).map {
      case (e, a) =>
        makeNamedExpression(e, a.name)
    }
    aggregateExpressions ++= groupingKeys

    for (i <- 1 until analyzedAggregates.length) {
      val keys = analyzedAggregates(i).getKeys()
      val fieldIndex = i * keysNumber
      for (j <- 0 until keys.length) {
        val key = keys(j)
        val valueExpr = unionOutput(fieldIndex + j)
        val firstValue = makeFirstAggregateExpression(valueExpr)
        aggregateExpressions += makeNamedExpression(firstValue, key.name)
      }
    }

    var fieldIndex = keysNumber * analyzedAggregates.length
    for (i <- 0 until analyzedAggregates.length) {
      val partialAggregateExpressions = analyzedAggregates(i).getAggregateExpressions()
      val partialAggregateArguments = analyzedAggregates(i).getAggregateExpressionArguments()
      for (j <- 0 until partialAggregateExpressions.length) {
        val aggregateExpression = partialAggregateExpressions(j)
        val arguments = partialAggregateArguments(j)
        val newArguments = unionOutput.slice(fieldIndex, fieldIndex + arguments.length)
        aggregateExpressions += buildAggregateExpressionWithNewChildren(
          aggregateExpression,
          newArguments)
        fieldIndex += arguments.length
      }
    }

    for (i <- fieldIndex until unionOutput.length) {
      val valueExpr = unionOutput(i)
      val firstValue = makeFirstAggregateExpression(valueExpr)
      aggregateExpressions += makeNamedExpression(firstValue, valueExpr.name)
    }

    Aggregate(groupingKeys, aggregateExpressions.toSeq, union)
  }

  /**
   * If the grouping keys is in the right table but not in the left table, remove the row from the
   * result.
   */
  def buildPrimeKeysFilterOnAggregateUnion(
      plan: LogicalPlan,
      analyzedAggregates: Seq[JoinedAggregateAnalyzer]): LogicalPlan = {
    val flagExpressions = plan.output(plan.output.length - analyzedAggregates.length)
    val notNullExpr = IsNotNull(flagExpressions)
    Filter(notNullExpr, plan)
  }

  /** Make the aggregate result be null if the grouping keys is not in the most left table. */
  def buildMakeNotMatchedRowsNullProject(
      plan: LogicalPlan,
      analyzedAggregates: Seq[JoinedAggregateAnalyzer],
      ignoreAggregates: Set[Int]): LogicalPlan = {
    val input = plan.output
    val flagExpressions =
      input.slice(plan.output.length - analyzedAggregates.length, plan.output.length)
    val aggregateExprsStart = analyzedAggregates.length * analyzedAggregates.head.getKeys().length
    var fieldIndex = aggregateExprsStart
    val aggregatesIfNullExpressions = analyzedAggregates.zipWithIndex.map {
      case (analyzedAggregate, i) =>
        val flagExpr = flagExpressions(i)
        val aggregateExpressions = analyzedAggregate.getAggregateExpressions()
        aggregateExpressions.map {
          e =>
            val valueExpr = input(fieldIndex)
            fieldIndex += 1
            if (ignoreAggregates(i)) {
              valueExpr.asInstanceOf[NamedExpression]
            } else {
              val clearExpr = If(IsNull(flagExpr), makeNullLiteral(valueExpr.dataType), valueExpr)
              makeNamedExpression(clearExpr, valueExpr.name)
            }
        }
    }
    val ifNullExpressions = aggregatesIfNullExpressions.flatten
    val projectList = input.slice(0, aggregateExprsStart) ++ ifNullExpressions ++ flagExpressions
    Project(projectList, plan)
  }

  /**
   * Build a project to make the output attributes have the same name and exprId as the original
   * join output attributes.
   */
  def buildRenameProject(
      plan: LogicalPlan,
      analyzedAggregates: Seq[JoinedAggregateAnalyzer]): LogicalPlan = {
    val input = plan.output
    val joinKeys = analyzedAggregates.map(_.getKeys())
    val projectList = ArrayBuffer[NamedExpression]()
    val keysNum = analyzedAggregates.head.getKeys().length
    var fieldIndex = 0
    for (i <- 0 until analyzedAggregates.length) {
      val keys = analyzedAggregates(i).getKeys()
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

  def makeNamedExpression(e: Expression, name: String): NamedExpression = {
    Alias(e, name)()
  }

  def makeNullLiteral(dataType: DataType): Literal = {
    Literal.create(null, dataType)
  }

  def makeFlagLiteral(): Literal = {
    Literal.create(true, BooleanType)
  }

  def makeFirstAggregateExpression(e: Expression): AggregateExpression = {
    val aggregateFunction = First(e, true)
    AggregateExpression(aggregateFunction, Complete, false)
  }

  /**
   * Build a extended project list, which contains three parts.
   *   - The grouping keys of all tables.
   *   - All required columns for aggregate functions in every table.
   *   - Flags for each table to indicate whether the row is in the table.
   */
  def buildExtendProjectList(
      analyzedAggregates: Seq[JoinedAggregateAnalyzer],
      index: Int): Seq[NamedExpression] = {
    val projectList = ArrayBuffer[NamedExpression]()
    projectList ++= analyzedAggregates(index).getKeys().zipWithIndex.map {
      case (e, i) =>
        makeNamedExpression(e, s"key_0_$i")
    }
    for (i <- 1 until analyzedAggregates.length) {
      val groupingKeys = analyzedAggregates(i).getKeys()
      projectList ++= groupingKeys.zipWithIndex.map {
        case (e, j) =>
          if (i == index) {
            makeNamedExpression(e, s"key_${i}_$j")
          } else {
            makeNamedExpression(makeNullLiteral(e.dataType), s"key_${i}_$j")
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
                makeNamedExpression(arg, s"arg_${i}_${j}_$k")
              } else {
                makeNamedExpression(makeNullLiteral(arg.dataType), s"arg_${i}_${j}_$k")
              }
          }
      }
    }

    for (i <- 0 until analyzedAggregates.length) {
      if (i == index) {
        projectList += makeNamedExpression(makeFlagLiteral(), s"flag_$i")
      } else {
        projectList += makeNamedExpression(makeNullLiteral(BooleanType), s"flag_$i")
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
    union
  }

  def isDirectAggregate(plan: LogicalPlan): Boolean = {
    plan match {
      case _ @SubqueryAlias(_, aggregate: Aggregate) => true
      case _: Aggregate => true
      case _ => false
    }
  }

  def extractDirectAggregate(plan: LogicalPlan): Option[Aggregate] = {
    plan match {
      case _ @SubqueryAlias(_, aggregate: Aggregate) => Some(aggregate)
      case aggregate: Aggregate => Some(aggregate)
      case _ => None
    }
  }

  def isDirectJoin(plan: LogicalPlan): Boolean = {
    plan match {
      case _: Join => true
      case _ => false
    }
  }

  def extractDirectJoin(plan: LogicalPlan): Option[Join] = {
    plan match {
      case join: Join => Some(join)
      case _ => None
    }
  }

  def collectSameKeysJoinedAggregates(
      plan: LogicalPlan,
      analyzedAggregates: ArrayBuffer[JoinedAggregateAnalyzer]): Option[LogicalPlan] = {
    plan match {
      case join: Join if join.joinType == LeftOuter && join.condition.isDefined =>
        val optionAggregate = extractDirectAggregate(join.right)
        if (optionAggregate.isEmpty) {
          return Some(plan)
        }
        val rightAggregateAnalyzer = JoinedAggregateAnalyzer.build(join, optionAggregate.get)
        if (rightAggregateAnalyzer.isEmpty) {
          return Some(plan)
        }

        if (
          analyzedAggregates.isEmpty ||
          JoinedAggregateAnalyzer.haveSamePrimeKeys(
            Seq(analyzedAggregates.head, rightAggregateAnalyzer.get))
        ) {
          analyzedAggregates += rightAggregateAnalyzer.get
          collectSameKeysJoinedAggregates(join.left, analyzedAggregates)
        } else {
          Some(plan)
        }
      case _ if isDirectAggregate(plan) =>
        val aggregate = extractDirectAggregate(plan).get
        val lastJoin = analyzedAggregates.last.join
        assert(lastJoin.left.equals(plan), "The node should be last join's left child")
        val leftAggregateAnalyzer = JoinedAggregateAnalyzer.build(lastJoin, aggregate)
        if (leftAggregateAnalyzer.isEmpty) {
          return Some(plan)
        }
        if (
          JoinedAggregateAnalyzer.haveSamePrimeKeys(
            Seq(analyzedAggregates.head, leftAggregateAnalyzer.get))
        ) {
          analyzedAggregates += leftAggregateAnalyzer.get
          None
        } else {
          Some(plan)
        }
      case _ => Some(plan)
    }
  }

}
