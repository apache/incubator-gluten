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

import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*
 * Example:
 * Rewrite query
 *  SELECT a, b, sum(c) FROM t WHERE d = 1 GROUP BY a,b
 *  UNION ALL
 *  SELECT a, b, sum(c) FROM t WHERE d = 2 GROUP BY a,b
 * into
 *  SELECT a, b, sum(c) FROM (
 *   SELECT s.a as a, s.b as b, s.c as c, s.id as group_id FROM (
 *    SELECT explode(s) as s FROM (
 *      SELECT array(
 *        if(d = 1, named_struct('a', a, 'b', b, 'c', c, 'id', 0), null),
 *        if(d = 2, named_struct('a', a, 'b', b, 'c', c, 'id', 1), null)) as s
 *      FROM t WHERE d = 1 OR d = 2
 *    )
 *   ) WHERE s is not null
 *  ) GROUP BY a,b, group_id
 *
 * The first query need to scan `t` multiple times, when the output of scan is large, the query is
 * really slow. The rewritten query only scan `t` once, and the performance is much better.
 */

class CoalesceAggregationUnion(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  def removeAlias(e: Expression): Expression = {
    e match {
      case alias: Alias => alias.child
      case _ => e
    }
  }

  case class AnalyzedAggregteInfo(aggregate: Aggregate) {
    lazy val resultGroupingExpressions = aggregate.aggregateExpressions.filter {
      e =>
        removeAlias(e) match {
          case aggExpr: AggregateExpression => false
          case _ => true
        }
    }

    lazy val hasAggregateWithFilter = aggregate.aggregateExpressions.exists {
      e =>
        removeAlias(e) match {
          case aggExpr: AggregateExpression => aggExpr.filter.isDefined
          case _ => false
        }
    }

    lazy val resultPositionInGroupingKeys = {
      var i = 0
      resultGroupingExpressions.map {
        e =>
          e match {
            case literal @ Alias(_: Literal, _) =>
              var idx = aggregate.groupingExpressions.indexOf(e)
              if (idx == -1) {
                idx = aggregate.groupingExpressions.length + i
                i += 1
              }
              idx
            case _ => aggregate.groupingExpressions.indexOf(removeAlias(e))
          }
      }
    }
  }

  case class AnalyzedPlan(plan: LogicalPlan, analyzedAggregateInfo: Option[AnalyzedAggregteInfo])

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.resolved) {
      logError(s"xxx visit plan:\n$plan")
      val newPlan = visitPlan(plan)
      logError(s"xxx rewritten plan:\n$newPlan")
      newPlan
    } else {
      plan
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan match {
      case union: Union =>
        val planGroups = groupSameStructureAggregate(union)
        val newUnionClauses = planGroups.map {
          groupedPlans =>
            if (groupedPlans.length == 1) {
              groupedPlans.head.plan
            } else {
              val aggregates = groupedPlans.map(_.plan)
              val replaceAttributes = collectReplaceAttributes(aggregates)
              val filterConditions = buildAggregateCasesConditions(aggregates, replaceAttributes)
              val firstAggregateFilter =
                groupedPlans.head.plan.asInstanceOf[Aggregate].child.asInstanceOf[Filter]

              // Concat all filter conditions with `or` and apply it on the source node
              val unionFilter = Filter(
                buildUnionConditionForAggregateSource(filterConditions),
                firstAggregateFilter.child)
              logError(s"xxx union filter:\n$unionFilter")

              val wrappedAttributesProject =
                buildStructWrapperProject(
                  unionFilter,
                  groupedPlans,
                  filterConditions,
                  replaceAttributes)
              logError(s"xxx wrapped attributes project:\n$wrappedAttributesProject")

              val arrayProject = buildArrayProject(wrappedAttributesProject, filterConditions)
              logError(s"xxx array project:\n$arrayProject")

              val explode = buildArrayExplode(arrayProject)
              logError(s"xxx explode:\n$explode")

              val notNullFilter = Filter(IsNotNull(explode.output.head), explode)
              logError(s"xxx not null filter:\n$notNullFilter")

              val destructStructProject = buildDestructStructProject(notNullFilter)
              logError(s"xxx destruct struct project:\n$destructStructProject")

              val singleAggregate = buildAggregateWithGroupId(destructStructProject, groupedPlans)
              logError(s"xxx single agg:\n$singleAggregate")

              singleAggregate
            }
        }
        if (newUnionClauses.length == 1) {
          newUnionClauses.head
        } else {
          union.withNewChildren(newUnionClauses)
        }
      case _ => plan.withNewChildren(plan.children.map(visitPlan))
    }
    newPlan.copyTagsFrom(plan)
    newPlan
  }

  def isSupportedAggregate(info: AnalyzedAggregteInfo): Boolean = {
    if (info.hasAggregateWithFilter) {
      false
    } else {
      true
    }
  }

  def areSameStructureAggregate(l: AnalyzedAggregteInfo, r: AnalyzedAggregteInfo): Boolean = {
    val lAggregate = l.aggregate
    val rAggregate = r.aggregate

    // Check aggregate result expressions. need same schema.
    if (lAggregate.aggregateExpressions.length != rAggregate.aggregateExpressions.length) {
      return false
    }

    lAggregate.aggregateExpressions.zip(rAggregate.aggregateExpressions).foreach {
      case (lExpr, rExpr) =>
        if (!lExpr.dataType.equals(rExpr.dataType)) {
          return false
        }
        (removeAlias(lExpr), removeAlias(rExpr)) match {
          case (lAggExpr: AggregateExpression, rAggExpr: AggregateExpression) =>
            if (lAggExpr.aggregateFunction.getClass != rAggExpr.aggregateFunction.getClass) {
              return false
            }
            if (
              lAggExpr.aggregateFunction.children.length !=
                rAggExpr.aggregateFunction.children.length
            ) {
              return false
            }
          case _ =>
        }
    }

    // Check grouping expressions, need same schema.
    if (lAggregate.groupingExpressions.length != rAggregate.groupingExpressions.length) {
      return false
    }

    def hasAggregateExpression(e: Expression): Boolean = {
      if (e.children.isEmpty && !e.isInstanceOf[AggregateExpression]) {
        return false
      }
      e match {
        case _: AggregateExpression => true
        case _ => e.children.exists(hasAggregateExpression(_))
      }
    }

    // 1. data types must be same.
    // 2. The aggregate expressions must have same structure and at the same position.
    lAggregate.groupingExpressions.zip(rAggregate.groupingExpressions).foreach {
      case (lExpr, rExpr) =>
        if (!lExpr.dataType.equals(rExpr.dataType)) {
          return false
        }
        val lHasAgg = hasAggregateExpression(lExpr)
        val rHasAgg = hasAggregateExpression(rExpr)
        if (lHasAgg != rHasAgg) {
          return false
        } else if (lHasAgg && !areEqualExpressions(lExpr, rExpr)) {
          return false
        }
    }

    if (
      l.resultPositionInGroupingKeys.length !=
        r.resultPositionInGroupingKeys.length
    ) {
      return false
    }
    l.resultPositionInGroupingKeys
      .zip(r.resultPositionInGroupingKeys)
      .foreach {
        case (lPos, rPos) =>
          if (lPos != rPos) {
            return false
          }
      }

    // Must come from same source.
    if (
      areSameAggregateSource(
        lAggregate.child.asInstanceOf[Filter].child,
        rAggregate.child.asInstanceOf[Filter].child)
    ) {
      return true
    }

    true
  }

  // If returns -1, not found same structure aggregate.
  def findSameStructureAggregate(
      planGroups: ArrayBuffer[ArrayBuffer[AnalyzedPlan]],
      analyzedAggregateInfo: AnalyzedAggregteInfo): Int = {
    planGroups.zipWithIndex.foreach {
      case (groupedPlans, i) =>
        if (
          groupedPlans.head.analyzedAggregateInfo.isDefined &&
          areSameStructureAggregate(
            groupedPlans.head.analyzedAggregateInfo.get,
            analyzedAggregateInfo)
        ) {
          return i
        }
    }
    -1
  }

  def groupSameStructureAggregate(union: Union): ArrayBuffer[ArrayBuffer[AnalyzedPlan]] = {
    val groupResults = ArrayBuffer[ArrayBuffer[AnalyzedPlan]]()
    union.children.foreach {
      case agg @ Aggregate(_, _, filter: Filter) =>
        val analyzedInfo = AnalyzedAggregteInfo(agg)
        if (isSupportedAggregate(analyzedInfo)) {
          if (groupResults.isEmpty) {
            groupResults += ArrayBuffer(AnalyzedPlan(agg, Some(analyzedInfo)))
          } else {
            val idx = findSameStructureAggregate(groupResults, analyzedInfo)
            if (idx != -1) {
              groupResults(idx) += AnalyzedPlan(agg, Some(analyzedInfo))
            } else {
              groupResults += ArrayBuffer(AnalyzedPlan(agg, Some(analyzedInfo)))
            }
          }
        } else {
          groupResults += ArrayBuffer(AnalyzedPlan(agg, None))
        }
      case other =>
        groupResults += ArrayBuffer(AnalyzedPlan(other, None))
    }
    groupResults
  }

  def areEqualExpressions(l: Expression, r: Expression): Boolean = {
    (l, r) match {
      case (lAttr: Attribute, rAttr: Attribute) =>
        lAttr.qualifiedName == rAttr.qualifiedName
      case (lLiteral: Literal, rLiteral: Literal) =>
        lLiteral.value.equals(rLiteral.value)
      case _ =>
        if (l.children.length != r.children.length || l.getClass != r.getClass) {
          false
        } else {
          l.children.zip(r.children).forall {
            case (lChild, rChild) => areEqualExpressions(lChild, rChild)
          }
        }
    }
  }

  def areSameAggregateSource(lPlan: LogicalPlan, rPlan: LogicalPlan): Boolean = {
    if (lPlan.children.length != rPlan.children.length || lPlan.getClass != rPlan.getClass) {
      false
    } else {
      lPlan.children.zip(rPlan.children).forall {
        case (lRel: LogicalRelation, rRel: LogicalRelation) =>
          val lTable = lRel.catalogTable.map(_.identifier.unquotedString).getOrElse("")
          val rTable = rRel.catalogTable.map(_.identifier.unquotedString).getOrElse("")
          lTable.equals(rTable) && lTable.nonEmpty
        case (lSubQuery: SubqueryAlias, rSubQuery: SubqueryAlias) =>
          areSameAggregateSource(lSubQuery.child, rSubQuery.child)
        case (lChild, rChild) => false
      }
    }
  }

  def collectReplaceAttributes(groupedPlans: ArrayBuffer[LogicalPlan]): Map[String, Attribute] = {
    def findFirstRelation(plan: LogicalPlan): LogicalRelation = {
      if (plan.isInstanceOf[LogicalRelation]) {
        return plan.asInstanceOf[LogicalRelation]
      } else if (plan.children.isEmpty) {
        return null
      } else {
        plan.children.foreach {
          child =>
            val rel = findFirstRelation(child)
            if (rel != null) {
              return rel
            }
        }
        return null
      }
    }
    val replaceMap = new mutable.HashMap[String, Attribute]()
    val firstFilter = groupedPlans.head.asInstanceOf[Aggregate].child.asInstanceOf[Filter]
    val qualifierPrefix =
      firstFilter.output.find(e => e.qualifier.nonEmpty).head.qualifier.mkString(".")
    val firstRelation = findFirstRelation(firstFilter.child)
    if (firstRelation == null) {
      throw new GlutenNotSupportException(s"Not found relation in plan: $firstFilter")
    }
    firstRelation.output.foreach {
      attr =>
        val qualifiedName = s"$qualifierPrefix.${attr.name}"
        replaceMap.put(qualifiedName, attr)
    }
    replaceMap.toMap
  }

  def replaceAttributes(expression: Expression, replaceMap: Map[String, Attribute]): Expression = {
    expression.transform {
      case attr: Attribute =>
        replaceMap.get(attr.qualifiedName) match {
          case Some(replaceAttr) => replaceAttr
          case None => attr
        }
    }
  }

  def buildAggregateCasesConditions(
      groupedPlans: ArrayBuffer[LogicalPlan],
      replaceMap: Map[String, Attribute]): ArrayBuffer[Expression] = {
    groupedPlans.map {
      plan =>
        val filter = plan.asInstanceOf[Aggregate].child.asInstanceOf[Filter]
        replaceAttributes(filter.condition, replaceMap)
    }
  }

  def buildUnionConditionForAggregateSource(conditions: ArrayBuffer[Expression]): Expression = {
    conditions.reduce(Or);
  }

  def wrapAggregatesAttributesInStructs(
      groupedPlans: ArrayBuffer[AnalyzedPlan],
      replaceMap: Map[String, Attribute]): Seq[NamedExpression] = {
    val structAttributes = ArrayBuffer[NamedExpression]()
    val casePrefix = "case_"
    val structPrefix = "field_"
    groupedPlans.zipWithIndex.foreach {
      case (aggregateCase, case_index) =>
        val aggregate = aggregateCase.plan.asInstanceOf[Aggregate]
        val analyzedInfo = aggregateCase.analyzedAggregateInfo.get
        val structFields = ArrayBuffer[Expression]()
        var fieldIndex: Int = 0
        aggregate.groupingExpressions.foreach {
          e =>
            structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
            structFields += replaceAttributes(e, replaceMap)
            fieldIndex += 1
        }
        for (i <- 0 until analyzedInfo.resultPositionInGroupingKeys.length) {
          val position = analyzedInfo.resultPositionInGroupingKeys(i)
          if (position >= fieldIndex) {
            val expr = analyzedInfo.resultGroupingExpressions(i)
            structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
            structFields += replaceAttributes(analyzedInfo.resultGroupingExpressions(i), replaceMap)
            fieldIndex += 1
          }
        }

        aggregate.aggregateExpressions
          .filter(
            e =>
              removeAlias(e) match {
                case aggExpr: AggregateExpression => true
                case _ => false
              })
          .foreach {
            e =>
              val aggFunction = removeAlias(e).asInstanceOf[AggregateExpression].aggregateFunction
              aggFunction.children.foreach {
                child =>
                  structFields += Literal(
                    UTF8String.fromString(s"$structPrefix$fieldIndex"),
                    StringType)
                  structFields += replaceAttributes(child, replaceMap)
                  fieldIndex += 1
              }
          }
        structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
        structFields += Literal(case_index, IntegerType)
        structAttributes += makeAlias(CreateNamedStruct(structFields), s"$casePrefix$case_index")
    }
    structAttributes
  }

  def buildStructWrapperProject(
      child: LogicalPlan,
      groupedPlans: ArrayBuffer[AnalyzedPlan],
      conditions: ArrayBuffer[Expression],
      replaceMap: Map[String, Attribute]): LogicalPlan = {
    val wrappedAttributes = wrapAggregatesAttributesInStructs(groupedPlans, replaceMap)
    val ifAttributes = wrappedAttributes.zip(conditions).map {
      case (attr, condition) =>
        makeAlias(If(condition, attr, Literal(null, attr.dataType)), attr.name)
          .asInstanceOf[NamedExpression]
    }
    Project(ifAttributes, child)
  }

  def buildArrayProject(child: LogicalPlan, conditions: ArrayBuffer[Expression]): LogicalPlan = {
    assert(
      child.output.length == conditions.length,
      s"Expected same length of output and conditions")
    val array = makeAlias(CreateArray(child.output), "array")
    Project(Seq(array), child)
  }

  def buildArrayExplode(child: LogicalPlan): LogicalPlan = {
    assert(child.output.length == 1, s"Expected single output from $child")
    val array = child.output.head.asInstanceOf[Expression]
    assert(array.dataType.isInstanceOf[ArrayType], s"Expected ArrayType from $array")
    val explodeExpr = Explode(array)
    val exploadOutput =
      AttributeReference("generate_output", array.dataType.asInstanceOf[ArrayType].elementType)()
    Generate(
      explodeExpr,
      unrequiredChildIndex = Seq(0),
      outer = false,
      qualifier = None,
      generatorOutput = Seq(exploadOutput),
      child)
  }

  def buildGroupConditions(
      groupedPlans: ArrayBuffer[LogicalPlan],
      replaceMap: Map[String, Attribute]): (ArrayBuffer[Expression], Expression) = {
    val conditions = groupedPlans.map {
      plan =>
        val filter = plan.asInstanceOf[Aggregate].child.asInstanceOf[Filter]
        replaceAttributes(filter.condition, replaceMap)
    }
    val unionCond = conditions.reduce(Or)
    (conditions, unionCond)
  }

  def makeAlias(e: Expression, name: String): NamedExpression = {
    Alias(e, name)(
      NamedExpression.newExprId,
      e match {
        case ne: NamedExpression => ne.qualifier
        case _ => Seq.empty
      },
      None,
      Seq.empty)
  }

  def buildDestructStructProject(child: LogicalPlan): LogicalPlan = {
    assert(child.output.length == 1, s"Expected single output from $child")
    val structedData = child.output.head
    assert(
      structedData.dataType.isInstanceOf[StructType],
      s"Expected StructType from $structedData")
    val structType = structedData.dataType.asInstanceOf[StructType]
    val attributes = ArrayBuffer[NamedExpression]()
    var index = 0
    structType.fields.foreach {
      field =>
        attributes += Alias(GetStructField(structedData, index), field.name)()
        index += 1
    }
    Project(attributes, child)
  }

  def buildAggregateWithGroupId(
      child: LogicalPlan,
      groupedPlans: ArrayBuffer[AnalyzedPlan]): LogicalPlan = {
    val attributes = child.output
    val aggregateTemplate = groupedPlans.head.plan.asInstanceOf[Aggregate]
    val analyzedAggregateInfo = groupedPlans.head.analyzedAggregateInfo.get

    val totalGroupingExpressionsCount =
      math.max(
        aggregateTemplate.groupingExpressions.length,
        analyzedAggregateInfo.resultPositionInGroupingKeys.max + 1)

    val groupingExpressions = attributes
      .slice(0, totalGroupingExpressionsCount)
      .map(_.asInstanceOf[Expression]) :+ attributes.last

    val normalExpressionPosition = analyzedAggregateInfo.resultPositionInGroupingKeys
    var normalExpressionCount = 0
    var aggregateExpressionIndex = totalGroupingExpressionsCount
    val aggregateExpressions = ArrayBuffer[NamedExpression]()
    aggregateTemplate.aggregateExpressions.foreach {
      e =>
        removeAlias(e) match {
          case aggExpr: AggregateExpression =>
            val aggFunc = aggExpr.aggregateFunction
            val newAggFuncArgs = aggFunc.children.zipWithIndex.map {
              case (arg, i) =>
                attributes(aggregateExpressionIndex + i)
            }
            aggregateExpressionIndex += aggFunc.children.length
            val newAggFunc =
              aggFunc.withNewChildren(newAggFuncArgs).asInstanceOf[AggregateFunction]
            val newAggExpr = AggregateExpression(
              newAggFunc,
              aggExpr.mode,
              aggExpr.isDistinct,
              aggExpr.filter,
              aggExpr.resultId)
            aggregateExpressions += makeAlias(newAggExpr, e.name)

          case other =>
            val position = normalExpressionPosition(normalExpressionCount)
            val attr = attributes(position)
            normalExpressionCount += 1
            aggregateExpressions += makeAlias(attr, e.name)
              .asInstanceOf[NamedExpression]
        }
    }
    Aggregate(groupingExpressions, aggregateExpressions, child)
  }
}
