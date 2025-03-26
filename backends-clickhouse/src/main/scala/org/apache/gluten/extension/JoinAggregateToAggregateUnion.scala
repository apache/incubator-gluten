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

trait AggregateExpressionValidator {
  def doValidate(): Boolean
  def getArgumentExpressions(): Option[Seq[Expression]]
}

case class DefaultValidator() extends AggregateExpressionValidator {
  override def doValidate(): Boolean = false
  override def getArgumentExpressions(): Option[Seq[Expression]] = None
}

case class SumValidator(aggExpr: AggregateExpression) extends AggregateExpressionValidator {
  val sum = aggExpr.aggregateFunction.asInstanceOf[Sum]
  override def doValidate(): Boolean = {
    !sum.child.isInstanceOf[Literal]
  }

  override def getArgumentExpressions(): Option[Seq[Expression]] = {
    Some(Seq(sum.child))
  }
}

case class CountValidator(aggExpr: AggregateExpression) extends AggregateExpressionValidator {
  val count = aggExpr.aggregateFunction.asInstanceOf[Count]
  override def doValidate(): Boolean = {
    count.children.length == 1 && !count.children.head.isInstanceOf[Literal]
  }

  override def getArgumentExpressions(): Option[Seq[Expression]] = {
    Some(count.children)
  }
}

object AggregateExpressionValidator {
  def apply(e: Expression): AggregateExpressionValidator = {
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
          case sum: Sum => SumValidator(agg)
          case count: Count => CountValidator(agg)
          case _ => DefaultValidator()
        }
      case _ => DefaultValidator()
    }
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

  def extractJoinKeys(
      join: Join): (Option[Seq[AttributeReference]], Option[Seq[AttributeReference]]) = {
    val leftKeys = ArrayBuffer[AttributeReference]()
    val rightKeys = ArrayBuffer[AttributeReference]()
    val leftOutputSet = join.left.outputSet
    val rightOutputSet = join.right.outputSet
    def visitJoinExpression(e: Expression): Unit = {
      e match {
        case and: And =>
          visitJoinExpression(and.left)
          visitJoinExpression(and.right)
        case equalTo @ EqualTo(left: AttributeReference, right: AttributeReference) =>
          if (leftOutputSet.contains(left) && rightOutputSet.contains(right)) {
            leftKeys += left
            rightKeys += right
          } else if (leftOutputSet.contains(right) && rightOutputSet.contains(left)) {
            leftKeys += right
            rightKeys += left
          } else {
            throw new GlutenException(s"Invalid join condition $equalTo")
          }
        case _ =>
          throw new GlutenException(s"Unsupported join condition $e")
      }
    }
    join.condition match {
      case Some(condition) =>
        try {
          visitJoinExpression(condition)
        } catch {
          case e: GlutenException =>
            logDebug(s"xxx invalid join condition $condition")
            return (None, None)
        }
        (Some(leftKeys), Some(rightKeys))
      case _ => (None, None)
    }
  }

  def extracAggregateExpressions(aggregate: Aggregate): Seq[NamedExpression] = {
    aggregate.aggregateExpressions.filter(_.find(_.isInstanceOf[AggregateExpression]).isDefined)
  }

  def validateAggregateExpressions(aggregateExpressions: Seq[NamedExpression]): Boolean = {
    aggregateExpressions.forall(AggregateExpressionValidator(_).doValidate)
  }

  def collectAggregateExpressionsArguments(
      expressions: Seq[NamedExpression]): Option[Seq[Expression]] = {
    val arguments = expressions.map(AggregateExpressionValidator(_).getArgumentExpressions)
    if (arguments.exists(_.isEmpty)) {
      None
    } else {
      Some(arguments.map(_.get).flatten)
    }
  }

  def areSameKeys(leftKeys: Seq[Expression], rightKeys: Seq[Expression]): Boolean = {
    var isValid = leftKeys.length == rightKeys.length
    isValid = isValid && leftKeys.forall(_.isInstanceOf[AttributeReference]) &&
      rightKeys.forall(_.isInstanceOf[AttributeReference])
    isValid && leftKeys.forall(e => rightKeys.exists(_.semanticEquals(e)))
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
    val (leftKeys, rightKeys) = extractJoinKeys(join)
    if (!leftKeys.isDefined || !rightKeys.isDefined) {
      return None
    }
    val aggregates = Seq(leftAggregate, rightAggregate)
    val joinKeys = Seq(leftKeys.get, rightKeys.get)
    val aggregateExpressions = aggregates.map(extracAggregateExpressions)
    if (aggregateExpressions.exists(!validateAggregateExpressions(_))) {
      return None
    }

    val keyPairs = joinKeys.zip(aggregates).map {
      case (keys, aggregate) =>
        (
          keys.map(_.asInstanceOf[Expression]),
          aggregate.groupingExpressions.map(_.asInstanceOf[Expression]))
    }
    if (keyPairs.exists(pair => !areSameKeys(pair._1, pair._2))) {
      return None
    }

    val optionAggregateInputs = aggregateExpressions.map(collectAggregateExpressionsArguments)
    if (optionAggregateInputs.exists(_.isEmpty)) {
      return None
    }
    val aggregateInputs = optionAggregateInputs.map(_.get)

    val extendProjects = buildExtendProjects(
      aggregates.map(_.groupingExpressions),
      aggregateInputs,
      aggregates.map(_.child)
    )

    val union = buildUnionOnExtendedProjects(extendProjects)
    val unionOutput = union.output
    val aggregateUnion = buildAggregateOnUnion(union, aggregates, joinKeys)
    val filtAggregateUnion = buildPrimeKeysFilterOnAggregateUnion(aggregateUnion, aggregates)
    val fieldStartOffset =
      aggregates.map(_.groupingExpressions.length).sum + aggregateExpressions.head.length
    val setNullsProject = buildMakeNotMatchedRowsNullProject(
      filtAggregateUnion,
      fieldStartOffset,
      aggregates.slice(1, aggregates.length))
    val renameProject =
      buildRenameProject(setNullsProject, aggregates, Seq(leftKeys.get, rightKeys.get))
    logDebug(s"xxx rename project\n$renameProject")
    logDebug(s"xxx rename outputs ${renameProject.output}")
    Some(renameProject)
  }

  def rebuildAggregateExpression(
      ne: NamedExpression,
      input: Seq[Attribute],
      inputOffset: Integer): (Integer, NamedExpression) = {
    val alias = ne.asInstanceOf[Alias]
    val aggregateExpression = alias.child.asInstanceOf[AggregateExpression]
    val validator = AggregateExpressionValidator(aggregateExpression)
    val arguments = validator.getArgumentExpressions().get
    val newArguments = input.slice(inputOffset, inputOffset + arguments.length)
    val newAggregateFunction = aggregateExpression.aggregateFunction
      .withNewChildren(newArguments)
      .asInstanceOf[AggregateFunction]
    val newAggregateExpression = aggregateExpression.copy(aggregateFunction = newAggregateFunction)
    val newAlias = makeNamedExpression(newAggregateExpression, alias.name)
    (inputOffset + arguments.length, newAlias)
  }

  def buildAggregateOnUnion(
      union: LogicalPlan,
      aggregates: Seq[Aggregate],
      joinKeys: Seq[Seq[AttributeReference]]): LogicalPlan = {
    val unionOutput = union.output
    val keysNumber = joinKeys.head.length
    val aggregateExpressions = ArrayBuffer[NamedExpression]()
    val groupingKeys = unionOutput.slice(0, keysNumber).zip(joinKeys.head).map {
      case (e, a) =>
        makeNamedExpression(e, a.name)
    }
    aggregateExpressions ++= groupingKeys

    var fieldIndex = keysNumber

    for (i <- 1 until joinKeys.length) {
      val keys = joinKeys(i)
      keys.foreach {
        key =>
          val valueExpr = unionOutput(fieldIndex)
          val firstValue = makeFirstAggregateExpression(valueExpr)
          aggregateExpressions += makeNamedExpression(firstValue, key.name)
          fieldIndex += 1
      }
    }

    aggregates.foreach {
      aggregate =>
        val partialAggregateExpressions = extracAggregateExpressions(aggregate)
        partialAggregateExpressions.foreach {
          e =>
            val (nextFieldIndex, newAggregateExpression) =
              rebuildAggregateExpression(e, unionOutput, fieldIndex)
            fieldIndex = nextFieldIndex
            aggregateExpressions += newAggregateExpression
        }
    }
    for (i <- fieldIndex until unionOutput.length) {
      val valueExpr = unionOutput(i)
      val firstValue = makeFirstAggregateExpression(valueExpr)
      aggregateExpressions += makeNamedExpression(firstValue, valueExpr.name)
    }

    Aggregate(groupingKeys, aggregateExpressions.toSeq, union)
  }

  def buildAggregateOnUnion(
      union: LogicalPlan,
      leftAggregate: Aggregate,
      leftKeys: Seq[AttributeReference],
      rightAggregate: Aggregate,
      rightKeys: Seq[AttributeReference]): LogicalPlan = {
    val unionOutput = union.output
    val groupingKeysNumber = leftKeys.length
    val aggregateExpressions = ArrayBuffer[NamedExpression]()
    val groupingKeys = unionOutput.slice(0, groupingKeysNumber).zip(leftKeys).map {
      case (e, a) =>
        makeNamedExpression(e, a.name)
    }
    aggregateExpressions ++= groupingKeys

    var fieldIndex = groupingKeysNumber
    aggregateExpressions ++= unionOutput
      .slice(groupingKeysNumber, groupingKeysNumber * 2)
      .map(makeFirstAggregateExpression)
      .zip(rightKeys)
      .map {
        case (e, a) =>
          fieldIndex += 1
          makeNamedExpression(e, a.name)
      }

    aggregateExpressions ++=
      extracAggregateExpressions(leftAggregate).map {
        e =>
          val (nextFieldIndex, newAggregateExpression) =
            rebuildAggregateExpression(e, unionOutput, fieldIndex)
          fieldIndex = nextFieldIndex
          newAggregateExpression
      }
    aggregateExpressions ++=
      extracAggregateExpressions(rightAggregate).map {
        e =>
          val (nextFieldIndex, newAggregateExpression) =
            rebuildAggregateExpression(e, unionOutput, fieldIndex)
          fieldIndex = nextFieldIndex
          newAggregateExpression
      }

    aggregateExpressions ++= unionOutput.slice(fieldIndex, unionOutput.length).map {
      attr =>
        val firstValue = makeFirstAggregateExpression(attr)
        makeNamedExpression(firstValue, attr.name)
    }

    Aggregate(groupingKeys, aggregateExpressions.toSeq, union)
  }

  def buildPrimeKeysFilterOnAggregateUnion(
      plan: LogicalPlan,
      aggregates: Seq[Aggregate]): LogicalPlan = {
    val flagExpressions = plan.output(plan.output.length - aggregates.length)
    val notNullExpr = IsNotNull(flagExpressions)
    Filter(notNullExpr, plan)
  }

  def buildMakeNotMatchedRowsNullProject(
      plan: LogicalPlan,
      fieldStartOffset: Integer,
      aggregates: Seq[Aggregate]): LogicalPlan = {
    val input = plan.output
    val flagExpressions = input.slice(plan.output.length - aggregates.length, plan.output.length)
    var fieldIndex = fieldStartOffset
    val aggregatesIfNullExpressions = aggregates.zipWithIndex.map {
      case (aggregate, i) =>
        val flagExpr = flagExpressions(i)
        val aggregateExpressions = extracAggregateExpressions(aggregate)
        aggregateExpressions.map {
          e =>
            val valueExpr = input(fieldIndex)
            fieldIndex += 1
            val clearExpr = If(IsNull(flagExpr), makeNullLiteral(valueExpr.dataType), valueExpr)
            makeNamedExpression(clearExpr, valueExpr.name)
        }
    }
    val ifNullExpressions = aggregatesIfNullExpressions.flatten
    val projectList = input.slice(0, fieldStartOffset) ++ ifNullExpressions
    Project(projectList, plan)
  }

  def buildRenameProject(
      plan: LogicalPlan,
      aggregates: Seq[Aggregate],
      joinKeys: Seq[Seq[AttributeReference]]): LogicalPlan = {
    val input = plan.output
    var fieldIndex = 0
    val projectList = ArrayBuffer[NamedExpression]()
    joinKeys.foreach {
      keys =>
        keys.foreach {
          key =>
            projectList += Alias(input(fieldIndex), key.name)(
              key.exprId,
              key.qualifier,
              None,
              Seq.empty)
            fieldIndex += 1
        }
    }
    aggregates.foreach {
      aggregate =>
        val aggregateExpressions = extracAggregateExpressions(aggregate)
        aggregateExpressions.foreach {
          e =>
            val valueExpr = input(fieldIndex)
            projectList += Alias(valueExpr, e.name)(e.exprId, e.qualifier, None, Seq.empty)
            fieldIndex += 1
        }
    }
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

  def buildExtendProjectList(
      groupingKeys: Seq[Seq[Expression]],
      aggregateInputs: Seq[Seq[Expression]],
      index: Int): Seq[NamedExpression] = {
    var fieldIndex = 0
    def nextFieldIndex(): Int = {
      val res = fieldIndex
      fieldIndex += 1
      res
    }

    val projectList = ArrayBuffer[NamedExpression]()
    val keys = groupingKeys(index)
    projectList ++= keys.map(makeNamedExpression(_, s"key$nextFieldIndex"))
    for (i <- 1 until groupingKeys.length) {
      if (i == index) {
        projectList ++= keys.map(makeNamedExpression(_, s"key$nextFieldIndex"))
      } else {
        projectList ++= keys
          .map(_.dataType)
          .map(makeNullLiteral)
          .map(makeNamedExpression(_, s"key$nextFieldIndex"))
      }
    }

    aggregateInputs.zipWithIndex.foreach {
      case (inputs, i) =>
        if (i == index) {
          projectList ++= inputs.map(makeNamedExpression(_, s"agg$nextFieldIndex"))
        } else {
          projectList ++= inputs
            .map(_.dataType)
            .map(makeNullLiteral)
            .map(makeNamedExpression(_, s"agg$nextFieldIndex"))
        }
    }

    for (i <- 0 until groupingKeys.length) {
      if (i == index) {
        projectList += makeNamedExpression(makeFlagLiteral(), s"flag$nextFieldIndex")
      } else {
        projectList += makeNamedExpression(makeNullLiteral(BooleanType), s"flag$nextFieldIndex")
      }
    }

    projectList.toSeq
  }

  def buildExtendProjects(
      groupingKeys: Seq[Seq[Expression]],
      aggregateInputs: Seq[Seq[Expression]],
      children: Seq[LogicalPlan]
  ): Seq[LogicalPlan] = {
    val projects = ArrayBuffer[LogicalPlan]()
    for (i <- 0 until children.length) {
      val projectList = buildExtendProjectList(groupingKeys, aggregateInputs, i)
      projects += Project(projectList, children(i))
    }
    projects.toSeq
  }

  def buildUnionOnExtendedProjects(plans: Seq[LogicalPlan]): LogicalPlan = {
    val union = Union(plans)
    logDebug(s"xxx union\n$union")
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
            case _ =>
              logDebug(s"xxx case 2.left\n{join.left}\nright\n{join.right}")
              plan.withNewChildren(plan.children.map(visitPlan))
          }
        } else {
          plan.withNewChildren(plan.children.map(visitPlan))
        }
      case _ => plan.withNewChildren(plan.children.map(visitPlan))
    }
  }
}
