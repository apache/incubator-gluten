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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/** Abstract class representing a plan analyzer. */
abstract class AbstractPlanAnalyzer() {

  /** Extract the common source subplan from the plan. */
  def getExtractedSourcePlan(): Option[LogicalPlan] = None

  /**
   * Construct a plan with filter. The filter condition may be rewritten, different from the
   * original filter condition.
   */
  def getConstructedFilterPlan(): Option[LogicalPlan] = None

  /**
   * Construct a plan with aggregate . The aggregate expressions may be rewritten, different from
   * the original aggregate.
   */
  def getConstructedAggregatePlan(): Option[LogicalPlan] = None

  /** Construct a plan with project. */
  def getConstructedProjectPlan: Option[LogicalPlan] = None

  /** If the rule cannot be applied, return false, otherwise return true. */
  def doValidate(): Boolean = false
}

/**
 * Case class representing an analyzed logical plan.
 *
 * @param plan
 *   The original plan which is analyzed.
 * @param planAnalyzer
 *   An optional plan analyzer that provides additional analysis capabilities. When the rule cannot
 *   apply to the plan, the planAnalyzer is None.
 */
case class AnalyzedPlan(plan: LogicalPlan, planAnalyzer: Option[AbstractPlanAnalyzer])

object CoalesceUnionUtil extends Logging {

  def isResolvedPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case isnert: InsertIntoStatement => isnert.query.resolved
      case _ => plan.resolved
    }
  }

  // unfold nested unions
  def collectAllUnionClauses(union: Union): ArrayBuffer[LogicalPlan] = {
    val unionClauses = ArrayBuffer[LogicalPlan]()
    union.children.foreach {
      case u: Union =>
        unionClauses ++= collectAllUnionClauses(u)
      case other =>
        unionClauses += other
    }
    unionClauses
  }

  def isRelation(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[MultiInstanceRelation]
  }

  def validateSource(plan: LogicalPlan): Boolean = {
    plan match {
      case relation if isRelation(relation) => true
      case _: Project | _: Filter | _: SubqueryAlias =>
        plan.children.forall(validateSource)
      case _ => false
    }
  }

  def buildAttributesMap(
      attributes: Seq[Attribute],
      expressions: Seq[NamedExpression]): Map[ExprId, NamedExpression] = {
    assert(attributes.length == expressions.length)
    val map = new mutable.HashMap[ExprId, NamedExpression]()
    attributes.zip(expressions).foreach {
      case (attr, expr) =>
        map.put(attr.exprId, expr)
    }
    map.toMap
  }

  def replaceAttributes(e: Expression, replaceMap: Map[ExprId, NamedExpression]): Expression = {
    e match {
      case attr: Attribute =>
        replaceMap.getOrElse(attr.exprId, attr)
      case attr: OuterReference =>
        replaceMap.get(attr.exprId) match {
          case Some(replaceAttr) =>
            OuterReference.apply(replaceAttr)
          case _ => attr
        }
      case subquery: ScalarSubquery =>
        val plan = replaceSubqueryAttributes(subquery.plan, replaceMap)
        val outerAttrs = subquery.outerAttrs.map(replaceAttributes(_, replaceMap))
        subquery.copy(plan, outerAttrs)
      case _ =>
        e.withNewChildren(e.children.map(replaceAttributes(_, replaceMap)))
    }
  }

  def replaceSubqueryAttributes(
      plan: LogicalPlan,
      replaceMap: Map[ExprId, NamedExpression]): LogicalPlan = {
    plan match {
      case filter: Filter =>
        filter.copy(
          replaceAttributes(filter.condition, replaceMap),
          replaceSubqueryAttributes(filter.child, replaceMap))
      case _ =>
        plan.withNewChildren(plan.children.map(replaceSubqueryAttributes(_, replaceMap)))
    }
  }

  // Plans and expressions are the same.
  def areStrictMatchedRelation(leftRelation: LogicalPlan, rightRelation: LogicalPlan): Boolean = {
    (leftRelation, rightRelation) match {
      case (leftLogicalRelation: LogicalRelation, rightLogicalRelation: LogicalRelation) =>
        val leftTable =
          leftLogicalRelation.catalogTable.map(_.identifier.unquotedString).getOrElse("")
        val rightTable =
          rightLogicalRelation.catalogTable.map(_.identifier.unquotedString).getOrElse("")
        leftLogicalRelation.output.length == rightLogicalRelation.output.length &&
        leftLogicalRelation.output.zip(rightLogicalRelation.output).forall {
          case (leftAttr, rightAttr) =>
            leftAttr.dataType.equals(rightAttr.dataType) && leftAttr.name.equals(rightAttr.name)
        } &&
        leftTable.equals(rightTable) && leftTable.nonEmpty
      case (leftCTE: CTERelationRef, rightCTE: CTERelationRef) =>
        leftCTE.cteId == rightCTE.cteId
      case (leftHiveTable: HiveTableRelation, rightHiveTable: HiveTableRelation) =>
        leftHiveTable.tableMeta.identifier.unquotedString
          .equals(rightHiveTable.tableMeta.identifier.unquotedString)
      case (leftSubquery: SubqueryAlias, rightSubquery: SubqueryAlias) =>
        areStrictMatchedRelation(leftSubquery.child, rightSubquery.child)
      case (leftProject: Project, rightProject: Project) =>
        leftProject.projectList.length == rightProject.projectList.length &&
        leftProject.projectList.zip(rightProject.projectList).forall {
          case (leftExpr, rightExpr) =>
            areMatchedExpression(leftExpr, rightExpr)
        } &&
        areStrictMatchedRelation(leftProject.child, rightProject.child)
      case (leftFilter: Filter, rightFilter: Filter) =>
        areMatchedExpression(leftFilter.condition, rightFilter.condition) &&
        areStrictMatchedRelation(leftFilter.child, rightFilter.child)
      case (_, _) => false
    }
  }

  // Two projects have the same output schema. Don't need the projectLists are the same.
  def areOutputMatchedProject(leftPlan: LogicalPlan, rightPlan: LogicalPlan): Boolean = {
    (leftPlan, rightPlan) match {
      case (leftProject: Project, rightProject: Project) =>
        val leftOutput = leftProject.output
        val rightOutput = rightProject.output
        leftOutput.length == rightOutput.length &&
        leftOutput.zip(rightOutput).forall {
          case (leftAttr, rightAttr) =>
            leftAttr.dataType.equals(rightAttr.dataType) && leftAttr.name.equals(rightAttr.name)
        }
      case (_, _) =>
        false
    }
  }

  def areMatchedExpression(leftExpression: Expression, rightExpression: Expression): Boolean = {
    (leftExpression, rightExpression) match {
      case (leftLiteral: Literal, rightLiteral: Literal) =>
        leftLiteral.dataType.equals(rightLiteral.dataType) &&
        leftLiteral.value == rightLiteral.value
      case (leftAttr: Attribute, rightAttr: Attribute) =>
        leftAttr.dataType.equals(rightAttr.dataType) && leftAttr.name.equals(rightAttr.name)
      case (leftAgg: AggregateExpression, rightAgg: AggregateExpression) =>
        leftAgg.isDistinct == rightAgg.isDistinct &&
        areMatchedExpression(leftAgg.aggregateFunction, rightAgg.aggregateFunction)
      case (_, _) =>
        leftExpression.getClass == rightExpression.getClass &&
        leftExpression.children.length == rightExpression.children.length &&
        leftExpression.children.zip(rightExpression.children).forall {
          case (leftChild, rightChild) => areMatchedExpression(leftChild, rightChild)
        }
    }
  }

  // Normalize all the filter conditions to make them fit with the first plan's source
  def normalizedClausesFilterCondition(analyzedPlans: Seq[AnalyzedPlan]): Seq[Expression] = {
    val valueAttributes = analyzedPlans.head.planAnalyzer.get.getExtractedSourcePlan.get.output
    analyzedPlans.map {
      analyzedPlan =>
        val keyAttributes = analyzedPlan.planAnalyzer.get.getExtractedSourcePlan.get.output
        val replaceMap = buildAttributesMap(keyAttributes, valueAttributes)
        val filter = analyzedPlan.planAnalyzer.get.getConstructedFilterPlan.get.asInstanceOf[Filter]
        CoalesceUnionUtil.replaceAttributes(filter.condition, replaceMap)
    }
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

  def removeAlias(e: Expression): Expression = {
    e match {
      case alias: Alias => alias.child
      case _ => e
    }
  }

  def isAggregateExpression(e: Expression): Boolean = {
    e match {
      case cast: Cast => isAggregateExpression(cast.child)
      case alias: Alias => isAggregateExpression(alias.child)
      case agg: AggregateExpression => true
      case _ => false
    }
  }

  def hasAggregateExpression(e: Expression): Boolean = {
    if (e.children.isEmpty && !e.isInstanceOf[AggregateExpression]) {
      false
    } else {
      e match {
        case _: AggregateExpression => true
        case _ => e.children.exists(hasAggregateExpression(_))
      }
    }
  }

  def addArrayStep(plan: LogicalPlan): LogicalPlan = {
    val array =
      CoalesceUnionUtil.makeAlias(CreateArray(plan.output.map(_.asInstanceOf[Expression])), "array")
    Project(Seq(array), plan)
  }

  def addExplodeStep(plan: LogicalPlan): LogicalPlan = {
    val arrayOutput = plan.output.head.asInstanceOf[Expression]
    val explodeExpression = Explode(arrayOutput)
    val explodeOutput = AttributeReference(
      "generate_output",
      arrayOutput.dataType.asInstanceOf[ArrayType].elementType)()
    val generate = Generate(
      explodeExpression,
      unrequiredChildIndex = Seq(0),
      outer = false,
      qualifier = None,
      generatorOutput = Seq(explodeOutput),
      plan)
    Filter(IsNotNull(generate.output.head), generate)
  }

  def addUnfoldStructStep(plan: LogicalPlan): LogicalPlan = {
    assert(plan.output.length == 1)
    val structExpression = plan.output.head
    assert(structExpression.dataType.isInstanceOf[StructType])
    val structType = structExpression.dataType.asInstanceOf[StructType]
    val attributes = ArrayBuffer[NamedExpression]()
    var fieldIndex = 0
    structType.fields.foreach {
      field =>
        attributes += Alias(GetStructField(structExpression, fieldIndex), field.name)()
        fieldIndex += 1
    }
    Project(attributes.toSeq, plan)
  }

  def unionClauses(originalUnion: Union, clauses: Seq[LogicalPlan]): LogicalPlan = {
    val coalescePlan = if (clauses.length == 1) {
      clauses.head
    } else {
      var firstUnionChild = clauses.head
      for (i <- 1 until clauses.length - 1) {
        firstUnionChild = Union(firstUnionChild, clauses(i))
      }
      Union(firstUnionChild, clauses.last)
    }
    val outputPairs = coalescePlan.output.zip(originalUnion.output)
    if (outputPairs.forall(pair => pair._1.semanticEquals(pair._2))) {
      coalescePlan
    } else {
      val reprojectOutputs = outputPairs.map {
        case (newAttr, oldAttr) =>
          if (newAttr.exprId == oldAttr.exprId) {
            newAttr
          } else {
            Alias(newAttr, oldAttr.name)(oldAttr.exprId, oldAttr.qualifier, None, Seq.empty)
          }
      }
      Project(reprojectOutputs, coalescePlan)
    }
  }

  def hasListQueryInside(expression: Expression): Boolean = {
    expression match {
      case list: ListQuery => true
      case _ => expression.children.exists(hasListQueryInside)
    }
  }
}

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
 * The first query need to scan `t` multiply, when the output of scan is large, the query is
 * really slow. The rewritten query only scan `t` once, and the performance is much better.
 */

case class CoalesceAggregationUnion(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  case class PlanAnalyzer(originalAggregate: Aggregate) extends AbstractPlanAnalyzer {

    protected def extractFilter(): Option[Filter] = {
      val filter = originalAggregate.child match {
        case filter: Filter => Some(filter)
        case project @ Project(_, filter: Filter) => Some(filter)
        case subquery: SubqueryAlias =>
          subquery.child match {
            case filter: Filter => Some(filter)
            case project @ Project(_, filter: Filter) => Some(filter)
            case relation if CoalesceUnionUtil.isRelation(relation) =>
              Some(Filter(Literal(true, BooleanType), subquery))
            case nestedRelation: SubqueryAlias
                if (CoalesceUnionUtil.isRelation(nestedRelation.child)) =>
              Some(Filter(Literal(true, BooleanType), nestedRelation))
            case _ => None
          }
        case _ => None
      }
      filter match {
        case Some(f) if CoalesceUnionUtil.hasListQueryInside(f.condition) => None
        case _ => filter
      }
    }

    // Try to make the plan simple, contain only three steps, source, filter, aggregate.
    lazy val extractedSourcePlan = {
      extractFilter match {
        case Some(filter) =>
          filter.child match {
            case project: Project if CoalesceUnionUtil.validateSource(project.child) =>
              Some(project.child)
            case other if CoalesceUnionUtil.validateSource(other) => Some(other)
            case _ => None
          }
        case None => None
      }
    }
    override def getExtractedSourcePlan(): Option[LogicalPlan] = extractedSourcePlan

    lazy val constructedFilterPlan = {
      extractedSourcePlan match {
        case Some(sourcePlan) =>
          val filter = extractFilter().get
          val innerProject = filter.child match {
            case project: Project => Some(project)
            case _ => None
          }

          val newFilter = innerProject match {
            case Some(project) =>
              val replaceMap =
                CoalesceUnionUtil.buildAttributesMap(project.output, project.projectList)
              val newCondition = CoalesceUnionUtil.replaceAttributes(filter.condition, replaceMap)
              Filter(newCondition, sourcePlan)
            case None => filter.withNewChildren(Seq(sourcePlan))
          }
          Some(newFilter)
        case None => None
      }
    }
    override def getConstructedFilterPlan: Option[LogicalPlan] = constructedFilterPlan

    lazy val constructedAggregatePlan = {
      if (!constructedFilterPlan.isDefined) {
        None
      } else {
        val innerProject = originalAggregate.child match {
          case project: Project => Some(project)
          case subquery @ SubqueryAlias(_, project: Project) =>
            Some(project)
          case _ => None
        }

        val newAggregate = innerProject match {
          case Some(project) =>
            val replaceMap =
              CoalesceUnionUtil.buildAttributesMap(project.output, project.projectList)
            val newGroupExpressions = originalAggregate.groupingExpressions.map {
              e => CoalesceUnionUtil.replaceAttributes(e, replaceMap)
            }
            val newAggregateExpressions = originalAggregate.aggregateExpressions.map {
              e => CoalesceUnionUtil.replaceAttributes(e, replaceMap).asInstanceOf[NamedExpression]
            }
            Aggregate(newGroupExpressions, newAggregateExpressions, constructedFilterPlan.get)
          case None => originalAggregate.withNewChildren(Seq(constructedFilterPlan.get))
        }
        Some(newAggregate)
      }
    }
    override def getConstructedAggregatePlan: Option[LogicalPlan] = constructedAggregatePlan

    def hasAggregateExpressionsWithFilter(e: Expression): Boolean = {
      if (e.children.isEmpty && !e.isInstanceOf[AggregateExpression]) {
        return false
      }
      e match {
        case aggExpr: AggregateExpression =>
          aggExpr.filter.isDefined
        case _ => e.children.exists(hasAggregateExpressionsWithFilter(_))
      }
    }
    lazy val hasAggregateWithFilter = originalAggregate.aggregateExpressions.exists {
      e => hasAggregateExpressionsWithFilter(e)
    }

    // The output results which are not aggregate expressions.
    lazy val resultRequiredGroupingExpressions = constructedAggregatePlan match {
      case Some(agg) =>
        agg
          .asInstanceOf[Aggregate]
          .aggregateExpressions
          .filter(e => !CoalesceUnionUtil.hasAggregateExpression(e))
      case None => Seq.empty
    }

    // For non-aggregate expressions in the output, they shoud be matched with one of the grouping
    // keys. There are some exceptions
    // 1. The expression is a literal. The grouping keys do not contain the literal.
    // 2. The expression is an expression withs gruping keys. For example,
    //    `select k1 + k2, count(1) from t group by k1, k2`.
    // This is used to judge whether two aggregate plans are matched.
    lazy val aggregateResultMatchedGroupingKeysPositions = {
      var extraPosition = 0
      val aggregate = constructedAggregatePlan.get.asInstanceOf[Aggregate]
      resultRequiredGroupingExpressions.map {
        case literal @ Alias(_: Literal, _) =>
          aggregate.groupingExpressions.indexOf(literal) match {
            case -1 =>
              extraPosition += 1
              aggregate.groupingExpressions.length + extraPosition - 1
            case position => position
          }
        case normalExpression =>
          aggregate.groupingExpressions.indexOf(
            CoalesceUnionUtil.removeAlias(normalExpression)) match {
            case -1 => aggregate.groupingExpressions.indexOf(normalExpression)
            case position => position
          }
      }
    }

    override def doValidate(): Boolean = {
      !hasAggregateWithFilter &&
      constructedAggregatePlan.isDefined &&
      aggregateResultMatchedGroupingKeysPositions.forall(_ >= 0) &&
      originalAggregate.aggregateExpressions.forall {
        e =>
          val innerExpr = CoalesceUnionUtil.removeAlias(e)
          // `agg_fun1(x) + agg_fun2(y)` is supported, but `agg_fun1(x) + y` is not supported.
          if (CoalesceUnionUtil.hasAggregateExpression(innerExpr)) {
            innerExpr.isInstanceOf[AggregateExpression] ||
            innerExpr.children.forall(e => CoalesceUnionUtil.isAggregateExpression(e))
          } else {
            true
          }
      } &&
      extractedSourcePlan.isDefined
    }
  }

  /*
   * Case class representing an analyzed plan.
   *
   * @param plan The logical plan that to be analyzed.
   * @param planAnalyzer Optional information about the aggregate analysis.
   */
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      spark.conf
        .get(CHBackendSettings.GLUTEN_ENABLE_COALESCE_AGGREGATION_UNION, "false")
        .toBoolean && CoalesceUnionUtil.isResolvedPlan(plan)
    ) {
      Try {
        visitPlan(plan)
      } match {
        case Success(newPlan) => newPlan
        case Failure(e) =>
          logError(s"$e")
          plan
      }
    } else {
      plan
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case union @ Union(_, false, false) =>
        val groupedUnionClauses = groupUnionClauses(union)
        val newUnionClauses = groupedUnionClauses.map {
          clauses => coalesceMatchedUnionClauses(clauses)
        }
        CoalesceUnionUtil.unionClauses(union, newUnionClauses)
      case _ => plan.withNewChildren(plan.children.map(visitPlan))
    }
  }

  def groupUnionClauses(union: Union): Seq[Seq[AnalyzedPlan]] = {
    val unionClauses = CoalesceUnionUtil.collectAllUnionClauses(union)
    val groups = ArrayBuffer[ArrayBuffer[AnalyzedPlan]]()
    unionClauses.foreach {
      clause =>
        val innerClause = clause match {
          case project @ Project(projectList, aggregate: Aggregate) =>
            if (projectList.forall(_.isInstanceOf[Alias])) {
              Some(aggregate)
            } else {
              None
            }
          case aggregate: Aggregate =>
            Some(aggregate)
          case _ => None
        }
        innerClause match {
          case Some(aggregate) =>
            val planAnalyzer = PlanAnalyzer(aggregate)
            if (planAnalyzer.doValidate()) {
              val analyzedPlan = AnalyzedPlan(clause, Some(planAnalyzer))
              val matchedGroup = findMatchedGroup(analyzedPlan, groups)
              if (matchedGroup != -1) {
                groups(matchedGroup) += analyzedPlan
              } else {
                groups += ArrayBuffer(analyzedPlan)
              }
            } else {
              val newClause = visitPlan(clause)
              groups += ArrayBuffer(AnalyzedPlan(newClause, None))
            }
          case None =>
            val newClause = visitPlan(clause)
            groups += ArrayBuffer(AnalyzedPlan(newClause, None))
        }
    }
    groups.map(_.toSeq).toSeq
  }

  def findMatchedGroup(
      analyedPlan: AnalyzedPlan,
      groups: ArrayBuffer[ArrayBuffer[AnalyzedPlan]]): Int = {
    groups.zipWithIndex.find {
      case (group, groupIndex) =>
        val checkedAnalyzedPlan = group.head
        if (checkedAnalyzedPlan.planAnalyzer.isDefined && analyedPlan.planAnalyzer.isDefined) {
          val leftPlanAnalyzer = checkedAnalyzedPlan.planAnalyzer.get.asInstanceOf[PlanAnalyzer]
          val rightPlanAnalyzer = analyedPlan.planAnalyzer.get.asInstanceOf[PlanAnalyzer]
          val leftAggregate =
            leftPlanAnalyzer.getConstructedAggregatePlan.get.asInstanceOf[Aggregate]
          val rightAggregate =
            rightPlanAnalyzer.getConstructedAggregatePlan.get.asInstanceOf[Aggregate]

          var isMatched = CoalesceUnionUtil
            .areStrictMatchedRelation(
              leftPlanAnalyzer.extractedSourcePlan.get,
              rightPlanAnalyzer.extractedSourcePlan.get)

          isMatched = isMatched &&
            leftAggregate.groupingExpressions.length == rightAggregate.groupingExpressions.length &&
            leftAggregate.groupingExpressions.zip(rightAggregate.groupingExpressions).forall {
              case (leftExpr, rightExpr) => leftExpr.dataType.equals(rightExpr.dataType)
            }

          isMatched = isMatched &&
            leftPlanAnalyzer.aggregateResultMatchedGroupingKeysPositions.length ==
            rightPlanAnalyzer.aggregateResultMatchedGroupingKeysPositions.length &&
            leftPlanAnalyzer.aggregateResultMatchedGroupingKeysPositions
              .zip(rightPlanAnalyzer.aggregateResultMatchedGroupingKeysPositions)
              .forall { case (leftPos, rightPos) => leftPos == rightPos }

          isMatched = isMatched && leftAggregate.aggregateExpressions.length ==
            rightAggregate.aggregateExpressions.length &&
            leftAggregate.aggregateExpressions.zip(rightAggregate.aggregateExpressions).forall {
              case (leftExpr, rightExpr) =>
                if (leftExpr.dataType.equals(rightExpr.dataType)) {
                  (
                    CoalesceUnionUtil.hasAggregateExpression(leftExpr),
                    CoalesceUnionUtil.hasAggregateExpression(rightExpr)) match {
                    case (true, true) =>
                      CoalesceUnionUtil.areMatchedExpression(leftExpr, rightExpr)
                    case (false, true) => false
                    case (true, false) => false
                    case (false, false) => true
                  }
                } else {
                  false
                }
            }
          isMatched
        } else {
          false
        }
    } match {
      case Some((_, i)) => i
      case None => -1
    }
  }

  def coalesceMatchedUnionClauses(clauses: Seq[AnalyzedPlan]): LogicalPlan = {
    if (clauses.length == 1) {
      clauses.head.plan
    } else {
      val normalizedConditions = CoalesceUnionUtil.normalizedClausesFilterCondition(clauses)
      val newSource = clauses.head.planAnalyzer.get.getExtractedSourcePlan.get
      val newFilter = Filter(normalizedConditions.reduce(Or), newSource)
      val foldStructStep = addFoldStructStep(newFilter, normalizedConditions, clauses)
      val arrayStep = CoalesceUnionUtil.addArrayStep(foldStructStep)
      val explodeStep = CoalesceUnionUtil.addExplodeStep(arrayStep)
      val unfoldStructStep = CoalesceUnionUtil.addUnfoldStructStep(explodeStep)
      addAggregateStep(unfoldStructStep, clauses.head.planAnalyzer.get.asInstanceOf[PlanAnalyzer])
    }
  }

  def addFoldStructStep(
      plan: LogicalPlan,
      conditions: Seq[Expression],
      analyzedPlans: Seq[AnalyzedPlan]): LogicalPlan = {
    val replaceAttributes = analyzedPlans.head.planAnalyzer.get.getExtractedSourcePlan.get.output

    val structAttributes = analyzedPlans.zipWithIndex.map {
      case (analyzedPlan, clauseIndex) =>
        val attributeReplaceMap = CoalesceUnionUtil.buildAttributesMap(
          analyzedPlan.planAnalyzer.get.getExtractedSourcePlan.get.output,
          replaceAttributes)
        val structFields = collectClauseStructFields(analyzedPlan, clauseIndex, attributeReplaceMap)
        CoalesceUnionUtil
          .makeAlias(CreateNamedStruct(structFields), s"clause_$clauseIndex")
          .asInstanceOf[NamedExpression]
    }

    val projectList = structAttributes.zip(conditions).map {
      case (attribute, condition) =>
        CoalesceUnionUtil
          .makeAlias(If(condition, attribute, Literal(null, attribute.dataType)), attribute.name)
          .asInstanceOf[NamedExpression]
    }
    Project(projectList, plan)
  }

  def collectClauseStructFields(
      analyzedPlan: AnalyzedPlan,
      clauseIndex: Int,
      attributeReplaceMap: Map[ExprId, NamedExpression]): Seq[Expression] = {

    val planAnalyzer = analyzedPlan.planAnalyzer.get.asInstanceOf[PlanAnalyzer]
    val aggregate = planAnalyzer.constructedAggregatePlan.get.asInstanceOf[Aggregate]
    val structFields = ArrayBuffer[Expression]()

    aggregate.groupingExpressions.foreach {
      e =>
        val fieldCounter = structFields.length / 2
        structFields += Literal(UTF8String.fromString(s"f$fieldCounter"), StringType)
        structFields += CoalesceUnionUtil.replaceAttributes(e, attributeReplaceMap)
    }

    planAnalyzer.aggregateResultMatchedGroupingKeysPositions.zipWithIndex.foreach {
      case (position, index) =>
        val fieldCounter = structFields.length / 2
        if (position >= fieldCounter) {
          val expression = planAnalyzer.resultRequiredGroupingExpressions(index)
          structFields += Literal(UTF8String.fromString(s"f$fieldCounter"), StringType)
          structFields += CoalesceUnionUtil.replaceAttributes(expression, attributeReplaceMap)
        }
    }

    aggregate.aggregateExpressions
      .filter(e => CoalesceUnionUtil.hasAggregateExpression(e))
      .foreach {
        e =>
          def visitAggregateExpression(expression: Expression): Unit = {
            expression match {
              case aggregateExpression: AggregateExpression =>
                val fieldCounter = structFields.length / 2
                val aggregateFunction = aggregateExpression.aggregateFunction
                aggregateFunction.children.foreach {
                  argument =>
                    val fieldCounter = structFields.length / 2
                    structFields += Literal(UTF8String.fromString(s"f$fieldCounter"), StringType)
                    structFields += CoalesceUnionUtil.replaceAttributes(
                      argument,
                      attributeReplaceMap)
                }
              case combindedAggregateExpression
                  if CoalesceUnionUtil.hasAggregateExpression(combindedAggregateExpression) =>
                combindedAggregateExpression.children.foreach(visitAggregateExpression)
              case other =>
                val fieldCounter = structFields.length / 2
                structFields += Literal(UTF8String.fromString(s"f$fieldCounter"), StringType)
                structFields += CoalesceUnionUtil.replaceAttributes(other, attributeReplaceMap)
            }
          }
          visitAggregateExpression(e)
      }

    // Add the clause index to the struct.
    val fieldCounter = structFields.length / 2
    structFields += Literal(UTF8String.fromString(s"f$fieldCounter"), StringType)
    structFields += Literal(UTF8String.fromString(s"$clauseIndex"), StringType)

    structFields.toSeq
  }

  def addAggregateStep(plan: LogicalPlan, templatePlanAnalyzer: PlanAnalyzer): LogicalPlan = {
    val inputAttributes = plan.output
    val templateAggregate =
      templatePlanAnalyzer.constructedAggregatePlan.get.asInstanceOf[Aggregate]

    val totalGroupingExpressionsCount = math.max(
      templateAggregate.groupingExpressions.length,
      templatePlanAnalyzer.aggregateResultMatchedGroupingKeysPositions.max + 1)

    // inputAttributes.last is the clause index.
    val groupingExpressions = inputAttributes
      .slice(0, totalGroupingExpressionsCount)
      .map(_.asInstanceOf[Expression]) :+ inputAttributes.last

    var aggregateExpressionCount = totalGroupingExpressionsCount
    var nonAggregateExpressionCount = 0
    val aggregateExpressions = ArrayBuffer[NamedExpression]()
    templateAggregate.aggregateExpressions.foreach {
      e =>
        CoalesceUnionUtil.removeAlias(e) match {
          case aggregateExpression
              if CoalesceUnionUtil.hasAggregateExpression(aggregateExpression) =>
            val (newAggregateExpression, count) = buildNewAggregateExpression(
              aggregateExpression,
              inputAttributes,
              aggregateExpressionCount)

            aggregateExpressionCount += count
            aggregateExpressions += CoalesceUnionUtil
              .makeAlias(newAggregateExpression, e.name)
              .asInstanceOf[NamedExpression]
          case nonAggregateExpression =>
            val position = templatePlanAnalyzer.aggregateResultMatchedGroupingKeysPositions(
              nonAggregateExpressionCount)
            val attribute = inputAttributes(position)
            aggregateExpressions += CoalesceUnionUtil
              .makeAlias(attribute, e.name)
              .asInstanceOf[NamedExpression]
            nonAggregateExpressionCount += 1

        }
    }
    Aggregate(groupingExpressions.toSeq, aggregateExpressions.toSeq, plan)
  }

  def buildNewAggregateExpression(
      oldExpression: Expression,
      inputAttributes: Seq[Attribute],
      attributesOffset: Int): (Expression, Int) = {
    oldExpression match {
      case aggregateExpression: AggregateExpression =>
        val aggregateFunction = aggregateExpression.aggregateFunction
        val newArguments = aggregateFunction.children.zipWithIndex.map {
          case (argument, i) => inputAttributes(i + attributesOffset)
        }
        val newAggregateFunction =
          aggregateFunction.withNewChildren(newArguments).asInstanceOf[AggregateFunction]
        val newAggregateExpression = AggregateExpression(
          newAggregateFunction,
          aggregateExpression.mode,
          aggregateExpression.isDistinct,
          aggregateExpression.filter,
          aggregateExpression.resultId)
        (newAggregateExpression, 1)
      case combindedAggregateExpression
          if CoalesceUnionUtil.hasAggregateExpression(combindedAggregateExpression) =>
        var count = 0
        val newChildren = ArrayBuffer[Expression]()
        combindedAggregateExpression.children.foreach {
          case child =>
            val (newChild, n) =
              buildNewAggregateExpression(child, inputAttributes, attributesOffset + count)
            count += n
            newChildren += newChild
        }
        val newExpression = combindedAggregateExpression.withNewChildren(newChildren.toSeq)
        (newExpression, count)
      case _ => (inputAttributes(attributesOffset), 1)
    }
  }
}

/**
 * Rewrite following query select a,b, 1 as c from t where d = 1 union all select a,b, 2 as c from t
 * where d = 2 into select s.f0 as a, s.f1 as b, s.f2 as c from ( select explode(s) as s from (
 * select array(if(d=1, named_struct('f0', a, 'f1', b, 'f2', 1), null), if(d=2, named_struct('f0',
 * a, 'f1', b, 'f2', 2), null)) as s from t where d = 1 or d = 2 ) ) where s is not null
 */
case class CoalesceProjectionUnion(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  case class PlanAnalyzer(originalPlan: LogicalPlan) extends AbstractPlanAnalyzer {
    def extractFilter(): Option[Filter] = {
      val filter = originalPlan match {
        case project @ Project(_, filter: Filter) => Some(filter)
        case _ => None
      }
      filter match {
        case Some(f) if CoalesceUnionUtil.hasListQueryInside(f.condition) => None
        case _ => filter
      }
    }

    lazy val extractedSourcePlan = {
      extractFilter match {
        case Some(filter) =>
          filter.child match {
            case project: Project =>
              if (CoalesceUnionUtil.validateSource(project.child)) Some(project.child)
              else None
              Some(project.child)
            case subquery @ SubqueryAlias(_, project: Project) =>
              if (CoalesceUnionUtil.validateSource(project.child)) Some(project.child)
              else None
            case _ => Some(filter.child)
          }
        case None => None
      }
    }

    lazy val constructedFilterPlan = {
      extractedSourcePlan match {
        case Some(source) =>
          val filter = extractFilter().get
          filter.child match {
            case project: Project =>
              val replaceMap =
                CoalesceUnionUtil.buildAttributesMap(
                  project.output,
                  project.projectList.map(_.asInstanceOf[NamedExpression]))
              val newCondition = CoalesceUnionUtil.replaceAttributes(filter.condition, replaceMap)
              Some(Filter(newCondition, source))
            case subquery @ SubqueryAlias(_, project: Project) =>
              val replaceMap =
                CoalesceUnionUtil.buildAttributesMap(
                  project.output,
                  project.projectList.map(_.asInstanceOf[NamedExpression]))
              val newCondition = CoalesceUnionUtil.replaceAttributes(filter.condition, replaceMap)
              Some(Filter(newCondition, source))
            case _ => Some(filter)
          }
        case None => None
      }
    }

    lazy val constructedProjectPlan = {
      constructedFilterPlan match {
        case Some(filter) =>
          val originalFilter = extractFilter().get
          originalFilter.child match {
            case project: Project =>
              val replaceMap =
                CoalesceUnionUtil.buildAttributesMap(project.output, project.projectList)
              val originalProject = originalPlan.asInstanceOf[Project]
              val newProjectList =
                originalProject.projectList
                  .map(e => CoalesceUnionUtil.replaceAttributes(e, replaceMap))
                  .map(_.asInstanceOf[NamedExpression])
              val newProject = Project(newProjectList, filter)
              Some(newProject)
            case subquery @ SubqueryAlias(_, project: Project) =>
              val replaceMap =
                CoalesceUnionUtil.buildAttributesMap(project.output, project.projectList)
              val originalProject = originalPlan.asInstanceOf[Project]
              val newProjectList =
                originalProject.projectList
                  .map(e => CoalesceUnionUtil.replaceAttributes(e, replaceMap))
                  .map(_.asInstanceOf[NamedExpression])
              val newProject = Project(newProjectList, filter)
              Some(newProject)
            case _ => Some(originalPlan)
          }
        case None => None
      }
    }

    override def doValidate(): Boolean = {
      constructedProjectPlan.isDefined
    }

    override def getExtractedSourcePlan: Option[LogicalPlan] = extractedSourcePlan

    override def getConstructedFilterPlan: Option[LogicalPlan] = constructedFilterPlan

    override def getConstructedProjectPlan: Option[LogicalPlan] = constructedProjectPlan
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      spark.conf
        .get(CHBackendSettings.GLUTEN_ENABLE_COALESCE_PROJECT_UNION, "false")
        .toBoolean && CoalesceUnionUtil.isResolvedPlan(plan)
    ) {
      Try {
        visitPlan(plan)
      } match {
        case Success(newPlan) => newPlan
        case Failure(e) =>
          logError(s"$e")
          plan
      }
    } else {
      plan
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case union @ Union(_, false, false) =>
        val groupedUnionClauses = groupUnionClauses(union)
        val newUnionClauses =
          groupedUnionClauses.map(clauses => coalesceMatchedUnionClauses(clauses))
        val newPlan = CoalesceUnionUtil.unionClauses(union, newUnionClauses)
        newPlan
      case other =>
        other.withNewChildren(other.children.map(visitPlan))
    }
  }

  def groupUnionClauses(union: Union): Seq[Seq[AnalyzedPlan]] = {
    val unionClauses = CoalesceUnionUtil.collectAllUnionClauses(union)
    val groups = ArrayBuffer[ArrayBuffer[AnalyzedPlan]]()
    unionClauses.foreach {
      clause =>
        val planAnalyzer = PlanAnalyzer(clause)
        if (planAnalyzer.doValidate()) {
          val matchedGroup = findMatchedGroup(AnalyzedPlan(clause, Some(planAnalyzer)), groups)
          if (matchedGroup != -1) {
            groups(matchedGroup) += AnalyzedPlan(clause, Some(planAnalyzer))
          } else {
            groups += ArrayBuffer(AnalyzedPlan(clause, Some(planAnalyzer)))
          }
        } else {
          val newClause = visitPlan(clause)
          groups += ArrayBuffer(AnalyzedPlan(newClause, None))
        }
    }
    groups.map(_.toSeq).toSeq
  }

  def findMatchedGroup(
      analyzedPlan: AnalyzedPlan,
      groupedClauses: ArrayBuffer[ArrayBuffer[AnalyzedPlan]]): Int = {
    groupedClauses.zipWithIndex.find {
      groupWithIndex =>
        if (groupWithIndex._1.head.planAnalyzer.isDefined) {
          val checkedPlanAnalyzer = groupWithIndex._1.head.planAnalyzer.get
          val checkPlanAnalyzer = analyzedPlan.planAnalyzer.get
          CoalesceUnionUtil.areOutputMatchedProject(
            checkedPlanAnalyzer.getConstructedProjectPlan.get,
            checkPlanAnalyzer.getConstructedProjectPlan.get) &&
          CoalesceUnionUtil.areStrictMatchedRelation(
            checkedPlanAnalyzer.getExtractedSourcePlan.get,
            checkPlanAnalyzer.getExtractedSourcePlan.get)
        } else {
          false
        }

    } match {
      case Some((_, i)) => i
      case None => -1
    }
  }

  def coalesceMatchedUnionClauses(clauses: Seq[AnalyzedPlan]): LogicalPlan = {
    if (clauses.length == 1) {
      clauses.head.plan
    } else {
      val normalizedConditions = CoalesceUnionUtil.normalizedClausesFilterCondition(clauses)
      val newSource = clauses.head.planAnalyzer.get.getExtractedSourcePlan.get
      val newFilter = Filter(normalizedConditions.reduce(Or), newSource)
      val foldStructStep = addFoldStructStep(newFilter, normalizedConditions, clauses)
      val arrayStep = CoalesceUnionUtil.addArrayStep(foldStructStep)
      val explodeStep = CoalesceUnionUtil.addExplodeStep(arrayStep)
      CoalesceUnionUtil.addUnfoldStructStep(explodeStep)
    }
  }

  def buildClausesStructs(analyzedPlans: Seq[AnalyzedPlan]): Seq[NamedExpression] = {
    val valueAttributes = analyzedPlans.head.planAnalyzer.get.getExtractedSourcePlan.get.output
    analyzedPlans.zipWithIndex.map {
      case (analyzedPlan, clauseIndex) =>
        val planAnalyzer = analyzedPlan.planAnalyzer.get
        val keyAttributes = planAnalyzer.getExtractedSourcePlan.get.output
        val replaceMap = CoalesceUnionUtil.buildAttributesMap(keyAttributes, valueAttributes)
        val projectPlan = planAnalyzer.getConstructedProjectPlan.get.asInstanceOf[Project]
        val newProjectList = projectPlan.projectList.map {
          e => CoalesceUnionUtil.replaceAttributes(e, replaceMap)
        }

        val structFields = ArrayBuffer[Expression]()
        newProjectList.zipWithIndex.foreach {
          case (e, fieldIndex) =>
            structFields += Literal(UTF8String.fromString(s"f$fieldIndex"), StringType)
            structFields += e
        }
        CoalesceUnionUtil.makeAlias(CreateNamedStruct(structFields.toSeq), s"clause_$clauseIndex")
    }
  }

  def addFoldStructStep(
      plan: LogicalPlan,
      conditions: Seq[Expression],
      analyzedPlans: Seq[AnalyzedPlan]): LogicalPlan = {
    val structAttributes = buildClausesStructs(analyzedPlans)
    assert(structAttributes.length == conditions.length)
    val structAttributesWithCondition = structAttributes.zip(conditions).map {
      case (struct, condition) =>
        CoalesceUnionUtil
          .makeAlias(If(condition, struct, Literal(null, struct.dataType)), struct.name)
          .asInstanceOf[NamedExpression]
    }
    Project(structAttributesWithCondition, plan)
  }
}
