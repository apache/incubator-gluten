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

  // Only two tables join, both are aggregate
  def rewriteOnlyTwoAggregatesJoin(
      leftAggregate: Aggregate,
      rightAggregate: Aggregate,
      join: Join): Option[LogicalPlan] = {
    val (leftKeys, rightKeys) = extractJoinKeys(join)
    if (!leftKeys.isDefined || !rightKeys.isDefined) {
      logDebug(s"xxx not valid join keys")
      return None
    }

    val leftAggregateExpressions = extracAggregateExpressions(leftAggregate)
    val rightAggregateExpressions = extracAggregateExpressions(rightAggregate)
    val firstAggExpr =
      leftAggregateExpressions.head.asInstanceOf[Alias].child.asInstanceOf[AggregateExpression]
    logDebug(s"xxx aggregate mode ${firstAggExpr.mode}")
    if (
      !validateAggregateExpressions(leftAggregateExpressions) ||
      !validateAggregateExpressions(rightAggregateExpressions)
    ) {
      logDebug(s"xxx not valid aggregate expressions")
      return None
    }

    if (
      !areSameKeys(
        leftKeys.get.map(_.asInstanceOf[Expression]),
        leftAggregate.groupingExpressions.map(_.asInstanceOf[Expression])) ||
      !areSameKeys(
        rightKeys.get.map(_.asInstanceOf[Expression]),
        rightAggregate.groupingExpressions.map(_.asInstanceOf[Expression]))
    ) {
      logDebug(s"xxx not same keys")
      return None
    }

    val leftAggregateInputs = collectAggregateExpressionsArguments(leftAggregateExpressions)
    val rightAggregateInputs = collectAggregateExpressionsArguments(rightAggregateExpressions)
    if (!leftAggregateInputs.isDefined || !rightAggregateInputs.isDefined) {
      logDebug(s"xxx not valid aggregate inputs")
      return None
    }
    logDebug(s"xxx left inputs\n$leftAggregateInputs\nright inputs\n$rightAggregateInputs")
    logDebug(s"xxx all check passed. $join")

    val aggregates = Seq(leftAggregate, rightAggregate)

    val leftProject = buildExtendedProject(
      leftAggregate.groupingExpressions,
      leftAggregate.groupingExpressions.map(_.dataType).map(makeNullLiteral),
      leftAggregateInputs.get,
      rightAggregateInputs.get.map(_.dataType).map(makeNullLiteral),
      Seq(makeFlagLiteral(), makeNullLiteral(BooleanType)),
      leftAggregate.child
    )
    val rightProject = buildExtendedProject(
      rightAggregate.groupingExpressions,
      rightAggregate.groupingExpressions,
      leftAggregateInputs.get.map(_.dataType).map(makeNullLiteral),
      rightAggregateInputs.get,
      Seq(makeNullLiteral(BooleanType), makeFlagLiteral()),
      rightAggregate.child
    )
    val union = buildUnionOnExtendedProjects(leftProject, rightProject)
    val unionOutput = union.output
    logDebug(s"xxx join outputs ${join.output}\nunion outputs ${union.output}")
    val aggregateUnion =
      buildAggregateOnUnion(union, leftAggregate, leftKeys.get, rightAggregate, rightKeys.get)
    logDebug(s"xxx aggregate union\n$aggregateUnion")
    logDebug(s"xxx aggregate outputs ${aggregateUnion.output}")
    val filtAggregateUnion = buildPrimeKeysFilterOnAggregateUnion(aggregateUnion, aggregates)
    logDebug(s"xxx filt aggregate union\n$filtAggregateUnion")
    logDebug(s"xxx filt outputs ${filtAggregateUnion.output}")
    val fieldStartOffset =
      leftAggregate.groupingExpressions.length * aggregates.length + leftAggregateExpressions.length
    val setNullsProject = buildMakeNotMatchedRowsNullProject(
      filtAggregateUnion,
      fieldStartOffset,
      aggregates.slice(1, aggregates.length))
    logDebug(s"xxx set nulls project\n$setNullsProject")
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
    // val newAlias =
    //  Alias(newAggregateExpression, alias.name)(alias.exprId, alias.qualifier, None, Seq.empty)
    val newAlias = makeNamedExpression(newAggregateExpression, alias.name)
    (inputOffset + arguments.length, newAlias)
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
        // Alias(e, a.name)(a.exprId, a.qualifier, Some(a.metadata), Seq.empty)
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
          // Alias(e, a.name)(a.exprId, a.qualifier, Some(a.metadata), Seq.empty)
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
      leftGroupingkeys: Seq[Expression],
      rightGroupingkeys: Seq[Expression],
      leftInputs: Seq[Expression],
      rightInputs: Seq[Expression],
      flags: Seq[Expression]): Seq[NamedExpression] = {
    var fieldIndex = 0
    val part1 = leftGroupingkeys.map {
      e =>
        fieldIndex += 1
        makeNamedExpression(e, s"field$fieldIndex")
    }
    val part2 = rightGroupingkeys.map {
      e =>
        fieldIndex += 1
        makeNamedExpression(e, s"field$fieldIndex")
    }
    val part3 = leftInputs.map {
      e =>
        fieldIndex += 1
        makeNamedExpression(e, s"field$fieldIndex")
    }
    val part4 = rightInputs.map {
      e =>
        fieldIndex += 1
        makeNamedExpression(e, s"field$fieldIndex")
    }
    val part5 = flags.map {
      e =>
        fieldIndex += 1
        makeNamedExpression(e, s"field$fieldIndex")
    }
    part1 ++ part2 ++ part3 ++ part4 ++ part5
  }

  def buildExtendedProject(
      leftGroupingkeys: Seq[Expression],
      rightGroupingkeys: Seq[Expression],
      leftInputs: Seq[Expression],
      rightInputs: Seq[Expression],
      flags: Seq[Expression],
      child: LogicalPlan): LogicalPlan = {
    val projectList =
      buildExtendProjectList(leftGroupingkeys, rightGroupingkeys, leftInputs, rightInputs, flags)
    Project(projectList, child)
  }


  def buildUnionOnExtendedProjects1(plans: Seq[LogicalPlan]): LogicalPlan = {
    val union = Union(plans)
    logDebug(s"xxx union\n$union")
    union
  }
  def buildUnionOnExtendedProjects(
      leftProject: LogicalPlan,
      rightProject: LogicalPlan): LogicalPlan = {
    val union = Union(Seq(leftProject, rightProject))
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
