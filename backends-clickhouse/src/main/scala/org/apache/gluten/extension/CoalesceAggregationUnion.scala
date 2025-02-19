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
import org.apache.gluten.exception.GlutenNotSupportException

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

abstract class AbstractPlanAnalyzer() {
  def getExtractedSourcePlan: Option[LogicalPlan] = None
  def getConstructedFilterPlan: Option[LogicalPlan] = None
  def getConstructedAggregatePlan: Option[LogicalPlan] = None
  def getConstructedProjectPlan: Option[LogicalPlan] = None
  def doValidate: Boolean = false
}

case class AnalyzedPlanV1(plan: LogicalPlan, planAnalyzer: Option[AbstractPlanAnalyzer])

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
      expressions: Seq[Expression]): Map[ExprId, Expression] = {
    assert(attributes.length == expressions.length)
    val map = new mutable.HashMap[ExprId, Expression]()
    attributes.zip(expressions).foreach {
      case (attr, expr) =>
        map.put(attr.exprId, expr)
    }
    map.toMap
  }

  def replaceAttributes(e: Expression, replaceMap: Map[ExprId, Expression]): Expression = {
    e match {
      case attr: Attribute =>
        replaceMap.get(attr.exprId) match {
          case Some(replaceAttr) => replaceAttr
          case None =>
            throw new GlutenNotSupportException(s"Not found attribute: $attr ${attr.qualifiedName}")
        }
      case _ =>
        e.withNewChildren(e.children.map(replaceAttributes(_, replaceMap)))
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
        leftLiteral.value.equals(rightLiteral.value)
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
  def normalizedClausesFilterCondition(
      analyzedPlans: ArrayBuffer[AnalyzedPlanV1]): Seq[Expression] = {
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

  def addArrayStep(plan: LogicalPlan): LogicalPlan = {
    val array = CoalesceUnionUtil.makeAlias(CreateArray(plan.output), "array")
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

class CoalesceAggregationUnion(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  def removeAlias(e: Expression): Expression = {
    e match {
      case alias: Alias => alias.child
      case _ => e
    }
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

  def isAggregateExpression(e: Expression): Boolean = {
    e match {
      case cast: Cast => isAggregateExpression(cast.child)
      case alias: Alias => isAggregateExpression(alias.child)
      case agg: AggregateExpression => true
      case _ => false
    }
  }

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

  case class PlanAnalyzer(originalAggregate: Aggregate) {
    protected def extractFilter(): Option[Filter] = {
      originalAggregate.child match {
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
    }

    // Try to make the plan simple, contain only three steps, source, filter, aggregate.
    lazy val extractedSourcePlan = {
      val filter = extractFilter()
      if (!filter.isDefined) {
        None
      } else {
        filter.get.child match {
          case project: Project if CoalesceUnionUtil.validateSource(project.child) =>
            Some(project.child)
          case other if CoalesceUnionUtil.validateSource(other) => Some(other)
          case _ => None
        }
      }
    }

    lazy val constructedFilterPlan = {
      val filter = extractFilter()
      if (!filter.isDefined || !extractedSourcePlan.isDefined) {
        None
      } else {
        val project = filter.get.child match {
          case project: Project => Some(project)
          case other =>
            None
        }
        val newFilter = project match {
          case Some(project) =>
            val replaceMap = CoalesceUnionUtil.buildAttributesMap(
              project.output,
              project.child.output.map(_.asInstanceOf[Expression]))
            val newCondition = CoalesceUnionUtil.replaceAttributes(filter.get.condition, replaceMap)
            Filter(newCondition, extractedSourcePlan.get)
          case None => filter.get.withNewChildren(Seq(extractedSourcePlan.get))
        }
        Some(newFilter)
      }
    }

    lazy val constructedAggregatePlan = {
      if (!constructedFilterPlan.isDefined) {
        None
      } else {
        val project = originalAggregate.child match {
          case p: Project => Some(p)
          case subquery: SubqueryAlias =>
            subquery.child match {
              case p: Project => Some(p)
              case _ => None
            }
          case _ => None
        }

        val newAggregate = project match {
          case Some(innerProject) =>
            val replaceMap = CoalesceUnionUtil.buildAttributesMap(
              innerProject.output,
              innerProject.projectList.map(_.asInstanceOf[Expression]))
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

    lazy val hasAggregateWithFilter = originalAggregate.aggregateExpressions.exists {
      e => hasAggregateExpressionsWithFilter(e)
    }

    // The output results which are not aggregate expressions.
    lazy val resultGroupingExpressions = constructedAggregatePlan match {
      case Some(agg) =>
        agg.asInstanceOf[Aggregate].aggregateExpressions.filter(e => !hasAggregateExpression(e))
      case None => Seq.empty
    }

    lazy val positionInGroupingKeys = {
      var i = 0
      // In most cases, the expressions which are not aggregate result could be matched with one of
      // groupingk keys. There are some exceptions
      // 1. The expression is a literal. The grouping keys do not contain the literal.
      // 2. The expression is an expression withs gruping keys. For example,
      //    `select k1 + k2, count(1) from t group by k1, k2`.
      resultGroupingExpressions.map {
        e =>
          val aggregate = constructedAggregatePlan.get.asInstanceOf[Aggregate]
          e match {
            case literal @ Alias(_: Literal, _) =>
              var idx = aggregate.groupingExpressions.indexOf(e)
              if (idx == -1) {
                idx = aggregate.groupingExpressions.length + i
                i += 1
              }
              idx
            case _ =>
              var idx = aggregate.groupingExpressions.indexOf(removeAlias(e))
              idx = if (idx == -1) {
                aggregate.groupingExpressions.indexOf(e)
              } else {
                idx
              }
              idx
          }
      }
    }
  }

  /*
   * Case class representing an analyzed plan.
   *
   * @param plan The logical plan that to be analyzed.
   * @param planAnalyzer Optional information about the aggregate analysis.
   */
  case class AnalyzedPlan(plan: LogicalPlan, planAnalyzer: Option[PlanAnalyzer])

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      spark.conf
        .get(CHBackendSettings.GLUTEN_ENABLE_COALESCE_UNION, "true")
        .toBoolean && CoalesceUnionUtil.isResolvedPlan(plan)
    ) {
      Try {
        visitPlan(plan)
      } match {
        case Success(newPlan) => newPlan
        case Failure(e) => plan
      }
    } else {
      plan
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case union: Union =>
        val planGroups = groupStructureMatchedAggregate(union)
        if (planGroups.forall(group => group.length == 1)) {
          plan.withNewChildren(plan.children.map(visitPlan))
        } else {
          val newUnionClauses = planGroups.map {
            groupedPlans =>
              if (groupedPlans.length == 1) {
                groupedPlans.head.plan
              } else {
                val firstPlanAnalyzer = groupedPlans.head.planAnalyzer.get
                val aggregates = groupedPlans.map(_.planAnalyzer.get.constructedAggregatePlan.get)
                val filterConditions = buildAggregateCasesConditions(groupedPlans)
                val firstAggregateFilter =
                  firstPlanAnalyzer.constructedFilterPlan.get.asInstanceOf[Filter]

                // Add a filter step with condition `cond1 or cond2 or ...`, `cond_i` comes from
                // each union clause. Apply this filter on the source plan.
                val unionFilter = Filter(
                  buildUnionConditionForAggregateSource(filterConditions),
                  firstPlanAnalyzer.extractedSourcePlan.get)

                // Wrap all the attributes into a single structure attribute.
                val wrappedAttributesProject =
                  buildProjectFoldIntoStruct(unionFilter, groupedPlans, filterConditions)

                // Build an array which element are response to each union clause.
                val arrayProject =
                  buildProjectBranchArray(wrappedAttributesProject, filterConditions)

                // Explode the array
                val explode = buildExplodeBranchArray(arrayProject)

                // Null value means that the union clause does not have the corresponding data.
                val notNullFilter = Filter(IsNotNull(explode.output.head), explode)

                // Destruct the struct attribute.
                val destructStructProject = buildProjectUnfoldStruct(notNullFilter)

                buildAggregateWithGroupId(destructStructProject, groupedPlans)
              }
          }
          val coalesePlan = if (newUnionClauses.length == 1) {
            newUnionClauses.head
          } else {
            var firstUnionChild = newUnionClauses.head
            for (i <- 1 until newUnionClauses.length - 1) {
              firstUnionChild = Union(firstUnionChild, newUnionClauses(i))
            }
            Union(firstUnionChild, newUnionClauses.last)
          }

          // We need to keep the output atrributes same as the original plan.
          val outputAttrPairs = coalesePlan.output.zip(union.output)
          if (outputAttrPairs.forall(pair => pair._1.semanticEquals(pair._2))) {
            coalesePlan
          } else {
            val reprejectOutputs = outputAttrPairs.map {
              case (newAttr, oldAttr) =>
                if (newAttr.exprId == oldAttr.exprId) {
                  newAttr
                } else {
                  Alias(newAttr, oldAttr.name)(oldAttr.exprId, oldAttr.qualifier, None, Seq.empty)
                }
            }
            Project(reprejectOutputs, coalesePlan)
          }
        }
      case _ => plan.withNewChildren(plan.children.map(visitPlan))
    }
  }

  def isSupportedAggregate(info: PlanAnalyzer): Boolean = {

    !info.hasAggregateWithFilter &&
    info.constructedAggregatePlan.isDefined &&
    info.positionInGroupingKeys.forall(_ >= 0) &&
    info.originalAggregate.aggregateExpressions.forall {
      e =>
        val innerExpr = removeAlias(e)
        // `agg_fun1(x) + agg_fun2(y)` is supported, but `agg_fun1(x) + y` is not supported.
        if (hasAggregateExpression(innerExpr)) {
          innerExpr.isInstanceOf[AggregateExpression] ||
          innerExpr.children.forall(e => isAggregateExpression(e))
        } else {
          true
        }
    } &&
    info.extractedSourcePlan.isDefined
  }

  /**
   * Checks if two PlanAnalyzer instances have the same structure.
   *
   * This method compares the aggregate expressions, grouping expressions, and the source plans of
   * the two PlanAnalyzer instances to determine if they have the same structure.
   *
   * @param l
   *   The first PlanAnalyzer instance.
   * @param r
   *   The second PlanAnalyzer instance.
   * @return
   *   True if the two instances have the same structure, false otherwise.
   */
  def areStructureMatchedAggregate(l: PlanAnalyzer, r: PlanAnalyzer): Boolean = {
    val lAggregate = l.constructedAggregatePlan.get.asInstanceOf[Aggregate]
    val rAggregate = r.constructedAggregatePlan.get.asInstanceOf[Aggregate]
    lAggregate.aggregateExpressions.length == rAggregate.aggregateExpressions.length &&
    lAggregate.aggregateExpressions.zip(rAggregate.aggregateExpressions).forall {
      case (lExpr, rExpr) =>
        if (!lExpr.dataType.equals(rExpr.dataType)) {
          false
        } else {
          (hasAggregateExpression(lExpr), hasAggregateExpression(rExpr)) match {
            case (true, true) => areStructureMatchedExpressions(lExpr, rExpr)
            case (false, true) => false
            case (true, false) => false
            case (false, false) => true
          }
        }
    } &&
    lAggregate.groupingExpressions.length == rAggregate.groupingExpressions.length &&
    l.positionInGroupingKeys.length == r.positionInGroupingKeys.length &&
    l.positionInGroupingKeys.zip(r.positionInGroupingKeys).forall {
      case (lPos, rPos) => lPos == rPos
    } &&
    areSameAggregateSource(l.extractedSourcePlan.get, r.extractedSourcePlan.get)
  }

  /*
   * Finds the index of the first group in `planGroups` that has the same structure as the given
   * `planAnalyzer`.
   *
   * This method iterates over the `planGroups` and checks if the first `AnalyzedPlan` in each group
   * has an `planAnalyzer` that matches the structure of the provided `planAnalyzer`. If a match is
   * found, the index of the group is returned. If no match is found, -1 is returned.
   *
   * @param planGroups
   *   An ArrayBuffer of ArrayBuffers, where each inner ArrayBuffer contains `AnalyzedPlan`
   *   instances.
   * @param planAnalyzer
   *   The `PlanAnalyzer` to match against the groups in `planGroups`.
   * @return
   *   The index of the first group with a matching structure, or -1 if no match is found.
   */
  def findStructureMatchedAggregate(
      planGroups: ArrayBuffer[ArrayBuffer[AnalyzedPlan]],
      planAnalyzer: PlanAnalyzer): Int = {
    planGroups.zipWithIndex.find(
      planWithIndex =>
        planWithIndex._1.head.planAnalyzer.isDefined &&
          areStructureMatchedAggregate(
            planWithIndex._1.head.planAnalyzer.get,
            planAnalyzer)) match {
      case Some((_, i)) => i
      case None => -1
    }

  }

  def groupStructureMatchedAggregate(union: Union): ArrayBuffer[ArrayBuffer[AnalyzedPlan]] = {

    def tryPutToGroup(
        groupResults: ArrayBuffer[ArrayBuffer[AnalyzedPlan]],
        agg: Aggregate): Unit = {
      val planAnalyzer = PlanAnalyzer(agg)
      if (isSupportedAggregate(planAnalyzer)) {
        if (groupResults.isEmpty) {
          groupResults += ArrayBuffer(
            AnalyzedPlan(planAnalyzer.originalAggregate, Some(planAnalyzer)))
        } else {
          val idx = findStructureMatchedAggregate(groupResults, planAnalyzer)
          if (idx != -1) {
            groupResults(idx) += AnalyzedPlan(
              planAnalyzer.constructedAggregatePlan.get,
              Some(planAnalyzer))
          } else {
            groupResults += ArrayBuffer(
              AnalyzedPlan(planAnalyzer.constructedAggregatePlan.get, Some(planAnalyzer)))
          }
        }
      } else {
        val rewrittenPlan = visitPlan(agg)
        groupResults += ArrayBuffer(AnalyzedPlan(rewrittenPlan, None))
      }
    }

    val groupResults = ArrayBuffer[ArrayBuffer[AnalyzedPlan]]()
    CoalesceUnionUtil.collectAllUnionClauses(union).foreach {
      case project @ Project(projectList, agg: Aggregate) =>
        if (projectList.forall(e => e.isInstanceOf[Alias])) {
          tryPutToGroup(groupResults, agg)
        } else {
          val rewrittenPlan = visitPlan(project)
          groupResults += ArrayBuffer(AnalyzedPlan(rewrittenPlan, None))
        }
      case agg: Aggregate =>
        tryPutToGroup(groupResults, agg)
      case other =>
        val rewrittenPlan = visitPlan(other)
        groupResults += ArrayBuffer(AnalyzedPlan(rewrittenPlan, None))
    }
    groupResults
  }

  def areStructureMatchedExpressions(l: Expression, r: Expression): Boolean = {
    if (l.dataType.equals(r.dataType)) {
      (l, r) match {
        case (lAttr: Attribute, rAttr: Attribute) =>
          // The the qualifier may be overwritten by a subquery alias, and make this check fail.
          lAttr.qualifiedName.equals(rAttr.qualifiedName)
        case (lLiteral: Literal, rLiteral: Literal) =>
          lLiteral.value == rLiteral.value
        case (lagg: AggregateExpression, ragg: AggregateExpression) =>
          lagg.isDistinct == ragg.isDistinct &&
          areStructureMatchedExpressions(lagg.aggregateFunction, ragg.aggregateFunction)
        case _ =>
          l.children.length == r.children.length &&
          l.getClass == r.getClass &&
          l.children.zip(r.children).forall {
            case (lChild, rChild) => areStructureMatchedExpressions(lChild, rChild)
          }
      }
    } else {
      false
    }
  }

  def areSameAggregateSource(lPlan: LogicalPlan, rPlan: LogicalPlan): Boolean = {
    if (lPlan.children.length != rPlan.children.length || lPlan.getClass != rPlan.getClass) {
      false
    } else {
      lPlan.children.zip(rPlan.children).forall {
        case (lRelation, rRelation)
            if (CoalesceUnionUtil.isRelation(lRelation) && CoalesceUnionUtil.isRelation(
              rRelation)) =>
          CoalesceUnionUtil.areStrictMatchedRelation(lRelation, rRelation)
        case (lSubQuery: SubqueryAlias, rSubQuery: SubqueryAlias) =>
          areSameAggregateSource(lSubQuery.child, rSubQuery.child)
        case (lproject: Project, rproject: Project) =>
          lproject.projectList.length == rproject.projectList.length &&
          lproject.projectList.zip(rproject.projectList).forall {
            case (lExpr, rExpr) => areStructureMatchedExpressions(lExpr, rExpr)
          } &&
          areSameAggregateSource(lproject.child, rproject.child)
        case (lFilter: Filter, rFilter: Filter) =>
          areStructureMatchedExpressions(lFilter.condition, rFilter.condition) &&
          areSameAggregateSource(lFilter.child, rFilter.child)
        case (lChild, rChild) => false
      }
    }
  }

  def buildAggregateCasesConditions(
      groupedPlans: ArrayBuffer[AnalyzedPlan]): ArrayBuffer[Expression] = {
    val firstPlanSourceOutputAttrs =
      groupedPlans.head.planAnalyzer.get.extractedSourcePlan.get.output
    groupedPlans.map {
      plan =>
        val attrsMap =
          CoalesceUnionUtil.buildAttributesMap(
            plan.planAnalyzer.get.extractedSourcePlan.get.output,
            firstPlanSourceOutputAttrs)
        val filter = plan.planAnalyzer.get.constructedFilterPlan.get.asInstanceOf[Filter]
        CoalesceUnionUtil.replaceAttributes(filter.condition, attrsMap)
    }
  }

  def buildUnionConditionForAggregateSource(conditions: ArrayBuffer[Expression]): Expression = {
    conditions.reduce(Or);
  }

  def wrapAggregatesAttributesInStructs(
      groupedPlans: ArrayBuffer[AnalyzedPlan]): Seq[NamedExpression] = {
    val structAttributes = ArrayBuffer[NamedExpression]()
    val casePrefix = "case_"
    val structPrefix = "field_"
    val firstSourceAttrs = groupedPlans.head.planAnalyzer.get.extractedSourcePlan.get.output
    groupedPlans.zipWithIndex.foreach {
      case (aggregateCase, case_index) =>
        val planAnalyzer = aggregateCase.planAnalyzer.get
        val aggregate = planAnalyzer.constructedAggregatePlan.get.asInstanceOf[Aggregate]
        val structFields = ArrayBuffer[Expression]()
        var fieldIndex: Int = 0
        val attrReplaceMap = CoalesceUnionUtil.buildAttributesMap(
          aggregateCase.planAnalyzer.get.extractedSourcePlan.get.output,
          firstSourceAttrs)
        aggregate.groupingExpressions.foreach {
          e =>
            structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
            structFields += CoalesceUnionUtil.replaceAttributes(e, attrReplaceMap)
            fieldIndex += 1
        }
        for (i <- 0 until planAnalyzer.positionInGroupingKeys.length) {
          val position = planAnalyzer.positionInGroupingKeys(i)
          if (position >= fieldIndex) {
            val expr = planAnalyzer.resultGroupingExpressions(i)
            structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
            structFields += CoalesceUnionUtil.replaceAttributes(
              planAnalyzer.resultGroupingExpressions(i),
              attrReplaceMap)
            fieldIndex += 1
          }
        }

        aggregate.aggregateExpressions
          .filter(e => hasAggregateExpression(e))
          .foreach {
            e =>
              def collectExpressionsInAggregateExpression(aggExpr: Expression): Unit = {
                aggExpr match {
                  case aggExpr: AggregateExpression =>
                    val aggFunction =
                      removeAlias(aggExpr).asInstanceOf[AggregateExpression].aggregateFunction
                    aggFunction.children.foreach {
                      child =>
                        structFields += Literal(
                          UTF8String.fromString(s"$structPrefix$fieldIndex"),
                          StringType)
                        structFields += CoalesceUnionUtil.replaceAttributes(child, attrReplaceMap)
                        fieldIndex += 1
                    }
                  case combineAgg if hasAggregateExpression(combineAgg) =>
                    combineAgg.children.foreach {
                      combindAggchild => collectExpressionsInAggregateExpression(combindAggchild)
                    }
                  case other =>
                    structFields += Literal(
                      UTF8String.fromString(s"$structPrefix$fieldIndex"),
                      StringType)
                    structFields += CoalesceUnionUtil.replaceAttributes(other, attrReplaceMap)
                    fieldIndex += 1
                }
              }
              collectExpressionsInAggregateExpression(e)
          }
        structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
        structFields += Literal(case_index, IntegerType)
        structAttributes += CoalesceUnionUtil.makeAlias(
          CreateNamedStruct(structFields.toSeq),
          s"$casePrefix$case_index")
    }
    structAttributes.toSeq
  }

  def buildProjectFoldIntoStruct(
      child: LogicalPlan,
      groupedPlans: ArrayBuffer[AnalyzedPlan],
      conditions: ArrayBuffer[Expression]): LogicalPlan = {
    val wrappedAttributes = wrapAggregatesAttributesInStructs(groupedPlans)
    val ifAttributes = wrappedAttributes.zip(conditions).map {
      case (attr, condition) =>
        CoalesceUnionUtil
          .makeAlias(If(condition, attr, Literal(null, attr.dataType)), attr.name)
          .asInstanceOf[NamedExpression]
    }
    Project(ifAttributes, child)
  }

  def buildProjectBranchArray(
      child: LogicalPlan,
      conditions: ArrayBuffer[Expression]): LogicalPlan = {
    assert(
      child.output.length == conditions.length,
      s"Expected same length of output and conditions")
    val array = CoalesceUnionUtil.makeAlias(CreateArray(child.output), "array")
    Project(Seq(array), child)
  }

  def buildExplodeBranchArray(child: LogicalPlan): LogicalPlan = {
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

  def buildProjectUnfoldStruct(child: LogicalPlan): LogicalPlan = {
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
    Project(attributes.toSeq, child)
  }

  def buildAggregateWithGroupId(
      child: LogicalPlan,
      groupedPlans: ArrayBuffer[AnalyzedPlan]): LogicalPlan = {
    val attributes = child.output
    val firstPlanAnalyzer = groupedPlans.head.planAnalyzer.get
    val aggregateTemplate =
      firstPlanAnalyzer.constructedAggregatePlan.get.asInstanceOf[Aggregate]
    val planAnalyzer = groupedPlans.head.planAnalyzer.get

    val totalGroupingExpressionsCount =
      math.max(
        aggregateTemplate.groupingExpressions.length,
        planAnalyzer.positionInGroupingKeys.max + 1)

    val groupingExpressions = attributes
      .slice(0, totalGroupingExpressionsCount)
      .map(_.asInstanceOf[Expression]) :+ attributes.last

    val normalExpressionPosition = planAnalyzer.positionInGroupingKeys
    var normalExpressionCount = 0
    var aggregateExpressionIndex = totalGroupingExpressionsCount
    val aggregateExpressions = ArrayBuffer[NamedExpression]()
    aggregateTemplate.aggregateExpressions.foreach {
      e =>
        removeAlias(e) match {
          case aggExpr if hasAggregateExpression(aggExpr) =>
            val (newAggExpr, count) =
              constructAggregateExpression(aggExpr, attributes, aggregateExpressionIndex)
            aggregateExpressions += CoalesceUnionUtil
              .makeAlias(newAggExpr, e.name)
              .asInstanceOf[NamedExpression]
            aggregateExpressionIndex += count
          case other =>
            val position = normalExpressionPosition(normalExpressionCount)
            val attr = attributes(position)
            normalExpressionCount += 1
            aggregateExpressions += CoalesceUnionUtil
              .makeAlias(attr, e.name)
              .asInstanceOf[NamedExpression]
        }
    }
    Aggregate(groupingExpressions.toSeq, aggregateExpressions.toSeq, child)
  }

  def constructAggregateExpression(
      aggExpr: Expression,
      attributes: Seq[Attribute],
      index: Int): (Expression, Int) = {
    aggExpr match {
      case singleAggExpr: AggregateExpression =>
        val aggFunc = singleAggExpr.aggregateFunction
        val newAggFuncArgs = aggFunc.children.zipWithIndex.map {
          case (arg, i) =>
            attributes(index + i)
        }
        val newAggFunc =
          aggFunc.withNewChildren(newAggFuncArgs).asInstanceOf[AggregateFunction]
        val res = AggregateExpression(
          newAggFunc,
          singleAggExpr.mode,
          singleAggExpr.isDistinct,
          singleAggExpr.filter,
          singleAggExpr.resultId)
        (res, 1)
      case combineAggExpr if hasAggregateExpression(combineAggExpr) =>
        val childrenExpressions = ArrayBuffer[Expression]()
        var totalCount = 0
        combineAggExpr.children.foreach {
          child =>
            val (expr, count) = constructAggregateExpression(child, attributes, totalCount + index)
            childrenExpressions += expr
            totalCount += count
        }
        (combineAggExpr.withNewChildren(childrenExpressions.toSeq), totalCount)
      case _ => (attributes(index), 1)
    }
  }
}

class CoalesceProjectionUnion(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  case class PlanAnalyzer(originalPlan: LogicalPlan) extends AbstractPlanAnalyzer {
    def extractFilter(): Option[Filter] = {
      originalPlan match {
        case project @ Project(_, filter: Filter) => Some(filter)
        case _ => None
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
                  project.projectList.map(_.asInstanceOf[Expression]))
              val newCondition = CoalesceUnionUtil.replaceAttributes(filter.condition, replaceMap)
              Some(Filter(newCondition, source))
            case subquery @ SubqueryAlias(_, project: Project) =>
              val replaceMap =
                CoalesceUnionUtil.buildAttributesMap(
                  project.output,
                  project.projectList.map(_.asInstanceOf[Expression]))
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
              None
              val replaceMap =
                CoalesceUnionUtil.buildAttributesMap(
                  project.output,
                  project.projectList.map(_.asInstanceOf[Expression]))
              val originalProject = originalPlan.asInstanceOf[Project]
              val newProjectList =
                originalProject.projectList
                  .map(e => CoalesceUnionUtil.replaceAttributes(e, replaceMap))
                  .map(_.asInstanceOf[NamedExpression])
              val newProject = Project(newProjectList, filter)
              Some(newProject)
            case subquery @ SubqueryAlias(_, project: Project) =>
              val replaceMap =
                CoalesceUnionUtil.buildAttributesMap(
                  project.output,
                  project.projectList.map(_.asInstanceOf[Expression]))
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

  case class AnalyzedPlan(plan: LogicalPlan, planAnalyzer: Option[PlanAnalyzer])

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      spark.conf
        .get(CHBackendSettings.GLUTEN_ENABLE_COALESCE_UNION, "true")
        .toBoolean && plan.resolved
    ) {
      Try {
        visitPlan(plan)
      } match {
        case Success(newPlan) => newPlan
        case Failure(e) =>
          logError(s"xxx exception: $e")
          plan
      }
    } else {
      plan
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case union: Union =>
        val groupedUnionClauses = groupUnionClauses(union)
        val newUnionClauses =
          groupedUnionClauses.map(clauses => coalesceMatchedUnionClauses(clauses))
        val newPlan = CoalesceUnionUtil.unionClauses(union, newUnionClauses)
        newPlan
      case other =>
        other.withNewChildren(other.children.map(visitPlan))
    }
  }

  def groupUnionClauses(union: Union): ArrayBuffer[ArrayBuffer[AnalyzedPlanV1]] = {
    val unionClauses = CoalesceUnionUtil.collectAllUnionClauses(union)
    val groups = ArrayBuffer[ArrayBuffer[AnalyzedPlanV1]]()
    unionClauses.foreach {
      clause =>
        val planAnalyzer = PlanAnalyzer(clause)
        if (planAnalyzer.doValidate()) {
          val matchedGroup = findMatchedGroup(AnalyzedPlanV1(clause, Some(planAnalyzer)), groups)
          if (matchedGroup != -1) {
            groups(matchedGroup) += AnalyzedPlanV1(clause, Some(planAnalyzer))
          } else {
            groups += ArrayBuffer(AnalyzedPlanV1(clause, Some(planAnalyzer)))
          }
        } else {
          val newClause = visitPlan(clause)
          groups += ArrayBuffer(AnalyzedPlanV1(newClause, None))
        }
    }
    groups
  }

  def findMatchedGroup(
      analyzedPlan: AnalyzedPlanV1,
      groupedClauses: ArrayBuffer[ArrayBuffer[AnalyzedPlanV1]]): Int = {
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

  def coalesceMatchedUnionClauses(clauses: ArrayBuffer[AnalyzedPlanV1]): LogicalPlan = {
    val normalizedConditions = CoalesceUnionUtil.normalizedClausesFilterCondition(clauses)
    val newSource = clauses.head.planAnalyzer.get.getExtractedSourcePlan.get
    val newFilter = Filter(normalizedConditions.reduce(Or), newSource)
    val foldStructStep = addFoldStructStep(newFilter, normalizedConditions, clauses)
    val arrayStep = CoalesceUnionUtil.addArrayStep(foldStructStep)
    val explodeStep = CoalesceUnionUtil.addExplodeStep(arrayStep)
    CoalesceUnionUtil.addUnfoldStructStep(explodeStep)
  }

  def buildClausesStructs(analyzedPlans: ArrayBuffer[AnalyzedPlanV1]): Seq[NamedExpression] = {
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
      analyzedPlans: ArrayBuffer[AnalyzedPlanV1]): LogicalPlan = {
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
