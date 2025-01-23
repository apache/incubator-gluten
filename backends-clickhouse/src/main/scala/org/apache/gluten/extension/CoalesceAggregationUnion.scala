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

    lazy val constResultGroupingExpressions = resultGroupingExpressions.filter {
      e =>
        removeAlias(e) match {
          case literal: Literal => true
          case _ => false
        }
    }

    lazy val nonConstResultGroupingExpressions = resultGroupingExpressions.filter {
      e =>
        removeAlias(e) match {
          case literal: Literal => false
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

  case class GroupPlanResult(plan: LogicalPlan, analyzedAggregateInfo: Option[AnalyzedAggregteInfo])

  override def apply(plan: LogicalPlan): LogicalPlan = {
    logError(s"xxx plan is resolved: ${plan.resolved}")
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
        val groups = groupSameStructureAggregate(union)
        groups.zipWithIndex.foreach {
          case (g, i) =>
            g.foreach(
              e =>
                logError(
                  s"xxx group $i plan: ${e.plan}\n" +
                    s"positions:" +
                    s"${e.analyzedAggregateInfo.get.resultPositionInGroupingKeys}"))
        }
        val rewrittenGroups = groups.map {
          group =>
            if (group.length == 1) {
              group.head.plan
            } else {
              val aggregates = group.map(_.plan)
              val replaceAttributes = collectReplaceAttributes(aggregates)
              val filterConditions = buildAggregateCasesConditions(aggregates, replaceAttributes)
              val firstAggregateFilter =
                group.head.plan.asInstanceOf[Aggregate].child.asInstanceOf[Filter]

              // Concat all filter conditions with `or` and apply it on the source node
              val unionFilter = Filter(
                buildUnionConditionForAggregateSource(filterConditions),
                firstAggregateFilter.child)
              logError(s"xxx union filter:\n$unionFilter")

              val wrappedAttributes = wrapAggregatesAttributesInStructs(group, replaceAttributes)
              logError(s"xxx wrapped attributes:\n$wrappedAttributes")
              val wrappedAttributesProject = Project(wrappedAttributes, unionFilter)
              logError(s"xxx wrapped attributes project:\n$wrappedAttributesProject")

              val arrayProject = buildArrayProject(wrappedAttributesProject)
              logError(s"xxx array project:\n$arrayProject")

              val explode = buildArrayExplode(arrayProject)
              logError(s"xxx explode:\n$explode")

              val notNullFilter = Filter(IsNotNull(explode.output.head), explode)
              logError(s"xxx not null filter:\n$notNullFilter")

              val destructStructProject = buildDestructStructProject(notNullFilter)
              logError(s"xxx destruct struct project:\n$destructStructProject")

              val resultAgg = buildAggregateWithGroupId(destructStructProject, group)
              logError(s"xxx result agg:\n$resultAgg")

              group.head.plan
            }
        }
        if (rewrittenGroups.length == 1) {
          rewrittenGroups.head
        } else {
          union.withNewChildren(rewrittenGroups)
        }
      case generate: Generate =>
        logError(
          s"xxx generate:\nunrequiredChildIndex: ${generate.unrequiredChildIndex}" +
            s"\nuter:${generate.outer} \neneratorOutput: ${generate.generatorOutput}")
        generate
      case project: Project =>
        project.projectList.foreach(
          e => logError(s"xxx project expression: ${removeAlias(e).getClass}, $e"))
        project.withNewChildren(project.children.map(visitPlan))
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
    lAggregate.groupingExpressions.zip(rAggregate.groupingExpressions).foreach {
      case (lExpr, rExpr) =>
        if (!lExpr.dataType.equals(rExpr.dataType)) {
          return false
        }
    }

    // All result expressions which come from grouping keys must refer to the same position in
    // grouping keys
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
      areSameAggregationSource(
        lAggregate.child.asInstanceOf[Filter].child,
        rAggregate.child.asInstanceOf[Filter].child)
    ) {
      return true
    }

    true
  }

  // If returns -1, not found same structure aggregate.
  def findSameStructureAggregate(
      groups: ArrayBuffer[ArrayBuffer[GroupPlanResult]],
      analyzedAggregateInfo: AnalyzedAggregteInfo): Int = {
    groups.zipWithIndex.foreach {
      case (group, i) =>
        if (
          group.head.analyzedAggregateInfo.isDefined &&
          areSameStructureAggregate(group.head.analyzedAggregateInfo.get, analyzedAggregateInfo)
        ) {
          return i
        }
    }
    -1
  }

  def groupSameStructureAggregate(union: Union): ArrayBuffer[ArrayBuffer[GroupPlanResult]] = {
    val groupResults = ArrayBuffer[ArrayBuffer[GroupPlanResult]]()
    union.children.foreach {
      case agg @ Aggregate(_, _, filter: Filter) =>
        val analyzedInfo = AnalyzedAggregteInfo(agg)
        if (isSupportedAggregate(analyzedInfo)) {
          if (groupResults.isEmpty) {
            groupResults += ArrayBuffer(GroupPlanResult(agg, Some(analyzedInfo)))
          } else {
            val idx = findSameStructureAggregate(groupResults, analyzedInfo)
            if (idx != -1) {
              groupResults(idx) += GroupPlanResult(agg, Some(analyzedInfo))
            } else {
              groupResults += ArrayBuffer(GroupPlanResult(agg, Some(analyzedInfo)))
            }
          }
        } else {
          groupResults += ArrayBuffer(GroupPlanResult(agg, None))
        }
      case other =>
        groupResults += ArrayBuffer(GroupPlanResult(other, None))
    }
    groupResults
  }

  def groupSameStructureAggregations(union: Union): ArrayBuffer[ArrayBuffer[LogicalPlan]] = {
    val groupResults = ArrayBuffer[ArrayBuffer[LogicalPlan]]()
    union.children.foreach {
      case agg @ Aggregate(_, _, filter: Filter) =>
        val analyzedInfo = AnalyzedAggregteInfo(agg)
        if (isSupportedAggregate(analyzedInfo)) {
          if (groupResults.isEmpty) {
            groupResults += ArrayBuffer(agg)
          } else {}
        }
        if (
          groupResults.isEmpty && agg.aggregateExpressions.exists(
            e =>
              removeAlias(e) match {
                case aggExpr: AggregateExpression => !aggExpr.filter.isDefined
                case _ => true
              })
        ) {
          groupResults += ArrayBuffer(agg)
        } else {
          groupResults.find {
            group =>
              group.head match {
                case toMatchAgg @ Aggregate(_, _, toMatchFilter: Filter) =>
                  areSameSchemaAggregations(toMatchAgg, agg) &&
                  areSameAggregationSource(toMatchFilter.child, filter.child)
                case _ => false
              }
          } match {
            case Some(group) =>
              group += agg.asInstanceOf[LogicalPlan]
            case None =>
              val newGroup = ArrayBuffer(agg.asInstanceOf[LogicalPlan])
              groupResults += newGroup
          }
        }
      case other =>
        // Other plans will be remained as union clauses.
        val singlePlan = ArrayBuffer(other)
        groupResults += singlePlan
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

  def areSameSchemaAggregations(agg1: Aggregate, agg2: Aggregate): Boolean = {
    if (
      agg1.aggregateExpressions.length != agg2.aggregateExpressions.length ||
      agg1.groupingExpressions.length != agg2.groupingExpressions.length
    ) {
      false
    } else {
      agg1.aggregateExpressions.zip(agg2.aggregateExpressions).forall {
        case (_ @Alias(_: Literal, lName), _ @Alias(_: Literal, rName)) =>
          lName.equals(rName)
        case (l, r) => areEqualExpressions(l, r)
      } &&
      agg1.groupingExpressions.zip(agg2.groupingExpressions).forall {
        case (l, r) => areEqualExpressions(l, r)
      }
    }
  }

  def areSameAggregationSource(lPlan: LogicalPlan, rPlan: LogicalPlan): Boolean = {
    if (lPlan.children.length != rPlan.children.length || lPlan.getClass != rPlan.getClass) {
      false
    } else {
      lPlan.children.zip(rPlan.children).forall {
        case (lRel: LogicalRelation, rRel: LogicalRelation) =>
          val lTable = lRel.catalogTable.map(_.identifier.unquotedString).getOrElse("")
          val rTable = rRel.catalogTable.map(_.identifier.unquotedString).getOrElse("")
          lRel.output.foreach(attr => logError(s"xxx l attr: $attr, ${attr.qualifiedName}"))
          rRel.output.foreach(attr => logError(s"xxx r attr: $attr, ${attr.qualifiedName}"))
          logError(s"xxx table: $lTable, $rTable")
          lTable.equals(rTable) && lTable.nonEmpty
        case (lSubQuery: SubqueryAlias, rSubQuery: SubqueryAlias) =>
          areSameAggregationSource(lSubQuery.child, rSubQuery.child)
        case (lChild, rChild) => false
      }
    }
  }

  def collectReplaceAttributes(group: ArrayBuffer[LogicalPlan]): Map[String, Attribute] = {
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
    val firstFilter = group.head.asInstanceOf[Aggregate].child.asInstanceOf[Filter]
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
      group: ArrayBuffer[LogicalPlan],
      replaceMap: Map[String, Attribute]): ArrayBuffer[Expression] = {
    group.map {
      plan =>
        val filter = plan.asInstanceOf[Aggregate].child.asInstanceOf[Filter]
        replaceAttributes(filter.condition, replaceMap)
    }
  }

  def buildUnionConditionForAggregateSource(conditions: ArrayBuffer[Expression]): Expression = {
    conditions.reduce(Or);
  }

  def wrapAggregatesAttributesInStructs(
      aggregateGroup: ArrayBuffer[GroupPlanResult],
      replaceMap: Map[String, Attribute]): Seq[NamedExpression] = {
    val structAttributes = ArrayBuffer[NamedExpression]()
    val casePrefix = "case_"
    val structPrefix = "field_"
    aggregateGroup.zipWithIndex.foreach {
      case (aggregateCase, case_index) =>
        val aggregate = aggregateCase.plan.asInstanceOf[Aggregate]
        val analyzedInfo = aggregateCase.analyzedAggregateInfo.get
        val structFields = ArrayBuffer[Expression]()
        var fieldIndex: Int = 0
        aggregate.groupingExpressions.foreach {
          e =>
            structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
            structFields += e
            fieldIndex += 1
        }
        logError(s"xxx 1) struct fields: $structFields")
        for (i <- 0 until analyzedInfo.resultPositionInGroupingKeys.length) {
          val position = analyzedInfo.resultPositionInGroupingKeys(i)
          logError(s"xxx position: $position, structFields: ${structFields.length}, f:$fieldIndex")
          if (position >= fieldIndex) {
            val expr = analyzedInfo.resultGroupingExpressions(i)
            structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
            structFields += analyzedInfo.resultGroupingExpressions(i)
            fieldIndex += 1
          }
        }
        logError(s"xxx 2) struct fields: $structFields")

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
                  structFields += child
                  fieldIndex += 1
              }
          }
        logError(s"xxx 3) struct fields: $structFields")
        structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
        structFields += Literal(case_index, IntegerType)
        structAttributes += makeAlias(CreateNamedStruct(structFields), s"$casePrefix$case_index")
    }
    structAttributes
  }

  def buildArrayProject(child: LogicalPlan): Project = {
    val outputs = child.output.map(_.asInstanceOf[Expression])
    val array = makeAlias(CreateArray(outputs), "array")
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
      group: ArrayBuffer[LogicalPlan],
      replaceMap: Map[String, Attribute]): (ArrayBuffer[Expression], Expression) = {
    val conditions = group.map {
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

  def concatAllNotAggregateExpressionsInAggregate(
      group: ArrayBuffer[LogicalPlan],
      replaceMap: Map[String, Attribute]): Seq[NamedExpression] = {

    val projectionExpressions = ArrayBuffer[NamedExpression]()
    val columnNamePrefix = "agg_expr_"
    val structFieldNamePrefix = "field_"
    var groupIndex: Int = 0
    group.foreach {
      plan =>
        var fieldIndex: Int = 0
        val agg = plan.asInstanceOf[Aggregate]
        val structFields = ArrayBuffer[Expression]()
        agg.groupingExpressions.map(e => replaceAttributes(e, replaceMap)).foreach {
          e =>
            structFields += Literal(
              UTF8String.fromString(s"$structFieldNamePrefix$fieldIndex"),
              StringType)
            structFields += e
            fieldIndex += 1
        }
        agg.aggregateExpressions.map(e => replaceAttributes(e, replaceMap)).foreach {
          e =>
            removeAlias(e) match {
              case aggExpr: AggregateExpression =>
                val aggFunc = aggExpr.aggregateFunction
                aggFunc.children.foreach {
                  child =>
                    structFields += Literal(
                      UTF8String.fromString(s"$structFieldNamePrefix$fieldIndex"),
                      StringType)
                    structFields += child
                    fieldIndex += 1
                }
              case notAggExpr =>
                structFields += Literal(
                  UTF8String.fromString(s"$structFieldNamePrefix$fieldIndex"),
                  StringType)
                structFields += e
                fieldIndex += 1
            }
        }
        projectionExpressions += makeAlias(
          CreateNamedStruct(structFields),
          s"$columnNamePrefix$groupIndex")
        groupIndex += 1
    }
    projectionExpressions.foreach(e => logError(s"xxx 11cprojection expression: ${e.resolved}, $e"))
    projectionExpressions
  }

  def buildBranchesArray(
      group: ArrayBuffer[LogicalPlan],
      groupConditions: ArrayBuffer[Expression],
      replaceMap: Map[String, Attribute]): Expression = {
    val groupStructs = concatAllNotAggregateExpressionsInAggregate(group, replaceMap)
    val arrrayFields = ArrayBuffer[Expression]()
    for (i <- 0 until groupStructs.length) {
      val structData = CreateNamedStruct(
        Seq[Expression](
          Literal(UTF8String.fromString("f1"), StringType),
          groupStructs(i),
          Literal(UTF8String.fromString("f2"), StringType),
          Literal(i, IntegerType)
        ))
      val field = If(groupConditions(i), structData, Literal(null, structData.dataType))
      logError(s"xxx if field: ${field.resolved} $field")
      logError(s"xxx if field type: ${field.dataType}")
      arrrayFields += field
    }
    val res = CreateArray(arrrayFields.toSeq)
    logError(s"xxx array fields: ${res.resolved} $res")
    res
  }

  def explodeBranches(
      group: ArrayBuffer[LogicalPlan],
      groupConditions: ArrayBuffer[Expression],
      replaceMap: Map[String, Attribute]): NamedExpression = {
    val explode = Explode(buildBranchesArray(group, groupConditions, replaceMap))
    logError(s"xxx explode. resolved: ${explode.resolved}, $explode\n${explode.dataType}")
    val alias = Alias(explode, "explode_branches_result")()
    logError(
      s"xxx explode alias. resolved: ${alias.resolved},${alias.childrenResolved}," +
        s"${alias.checkInputDataTypes().isSuccess} \n$alias")
    alias
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
      aggregateGroup: ArrayBuffer[GroupPlanResult]): LogicalPlan = {
    val attributes = child.output
    val aggregateTemplate = aggregateGroup.head.plan.asInstanceOf[Aggregate]
    val analyzedAggregateInfo = aggregateGroup.head.analyzedAggregateInfo.get

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
                logError(s"xxx agg expr: $arg, $i, $aggregateExpressionIndex")
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

  def buildAggregateWithGroupId1(
      structedData: Expression,
      child: LogicalPlan,
      templateAgg: Aggregate): LogicalPlan = {
    logError(s"xxx struct data: $structedData")
    logError(s"xxx struct type: ${structedData.dataType}")
    val structType = structedData.dataType.asInstanceOf[StructType]
    val flattenAtrributes = ArrayBuffer[NamedExpression]()
    var fieldIndex: Int = 0
    structType.fields(0).dataType.asInstanceOf[StructType].fields.foreach {
      field =>
        flattenAtrributes += Alias(
          GetStructField(GetStructField(structedData, 0), fieldIndex),
          field.name)()
        fieldIndex += 1
    }
    flattenAtrributes += Alias(
      GetStructField(structedData, 1),
      CoalesceAggregationUnion.UNION_TAG_FIELD)()
    logError(s"xxx flatten attributes: ${flattenAtrributes.length}\n$flattenAtrributes")

    val flattenProject = Project(flattenAtrributes, child)
    val groupingExpessions = ArrayBuffer[Expression]()
    for (i <- 0 until templateAgg.groupingExpressions.length) {
      groupingExpessions += flattenProject.output(i)
    }
    groupingExpessions += flattenProject.output.last

    var aggregateExpressionIndex = templateAgg.groupingExpressions.length
    val aggregateExpressions = ArrayBuffer[NamedExpression]()
    for (i <- 0 until templateAgg.aggregateExpressions.length) {
      logError(
        s"xxx field index: $aggregateExpressionIndex, i: $i, " +
          s"len:${templateAgg.aggregateExpressions.length}")
      removeAlias(templateAgg.aggregateExpressions(i)) match {
        case aggExpr: AggregateExpression =>
          val aggregateFunctionArgs = aggExpr.aggregateFunction.children.zipWithIndex.map {
            case (e, index) => flattenProject.output(aggregateExpressionIndex + index)
          }
          aggregateExpressionIndex += aggExpr.aggregateFunction.children.length
          val aggregateFunction = aggExpr.aggregateFunction
            .withNewChildren(aggregateFunctionArgs)
            .asInstanceOf[AggregateFunction]
          val newAggregateExpression = AggregateExpression(
            aggregateFunction,
            aggExpr.mode,
            aggExpr.isDistinct,
            aggExpr.filter,
            aggExpr.resultId)
          aggregateExpressions += Alias(
            newAggregateExpression,
            templateAgg.aggregateExpressions(i).asInstanceOf[Alias].name)()
        case notAggExpr =>
          aggregateExpressions += flattenProject.output(aggregateExpressionIndex)
          aggregateExpressionIndex += 1
      }
    }
    Aggregate(groupingExpessions, aggregateExpressions, flattenProject)
  }
}

object CoalesceAggregationUnion {
  val UNION_TAG_FIELD: String = "union_tag_"
}
