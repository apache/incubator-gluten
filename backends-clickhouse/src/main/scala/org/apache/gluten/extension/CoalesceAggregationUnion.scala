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

  case class AggregateAnalzyInfo(originalAggregate: Aggregate) {

    protected def buildAttributesToExpressionsMap(
        attributes: Seq[Attribute],
        expressions: Seq[Expression]): Map[ExprId, Expression] = {
      val map = new mutable.HashMap[ExprId, Expression]()
      attributes.zip(expressions).foreach {
        case (attr, expr) =>
          map.put(attr.exprId, expr)
      }
      map.toMap
    }

    protected def replaceAttributes(
        expression: Expression,
        replaceMap: Map[ExprId, Expression]): Expression = {
      expression.transform {
        case attr: Attribute =>
          replaceMap.getOrElse(attr.exprId, attr.asInstanceOf[Expression])
      }
    }

    protected def getFilter(): Option[Filter] = {
      originalAggregate.child match {
        case filter: Filter => Some(filter)
        case project @ Project(_, filter: Filter) => Some(filter)
        case subquery: SubqueryAlias =>
          logError(s"xxx subquery child. ${subquery.child.getClass}")
          subquery.child match {
            case filter: Filter => Some(filter)
            case project @ Project(_, filter: Filter) => Some(filter)
            case relation: LogicalRelation => Some(Filter(Literal(true, BooleanType), subquery))
            case nestedRelation: SubqueryAlias =>
              logError(
                s"xxx nestedRelation child. ${nestedRelation.child.getClass}" +
                  s"\n$nestedRelation")
              if (nestedRelation.child.isInstanceOf[LogicalRelation]) {
                Some(Filter(Literal(true, BooleanType), nestedRelation))
              } else {
                None
              }
            case _ => None
          }
        case _ => None
      }
    }

    // Try to make the plan simple, contain only three steps, source, filter, aggregate.
    lazy val sourcePlan = {
      val filter = getFilter()
      if (!filter.isDefined) {
        None
      } else {
        filter.get.child match {
          case project: Project => Some(project.child)
          case other => Some(other)
        }
      }
    }

    lazy val filterPlan = {
      val filter = getFilter()
      if (!filter.isDefined || !sourcePlan.isDefined) {
        None
      } else {
        val project = filter.get.child match {
          case project: Project => Some(project)
          case other =>
            None
        }
        val replacedFilter = project match {
          case Some(project) =>
            val replaceMap = buildAttributesToExpressionsMap(project.output, project.child.output)
            val replacedCondition = replaceAttributes(filter.get.condition, replaceMap)
            Filter(replacedCondition, sourcePlan.get)
          case None => filter.get.withNewChildren(Seq(sourcePlan.get))
        }
        Some(replacedFilter)
      }
    }

    lazy val aggregatePlan = {
      if (!filterPlan.isDefined) {
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

        val replacedAggregate = project match {
          case Some(innerProject) =>
            val replaceMap =
              buildAttributesToExpressionsMap(innerProject.output, innerProject.projectList)
            val groupExpressions = originalAggregate.groupingExpressions.map {
              e => replaceAttributes(e, replaceMap)
            }
            val aggregateExpressions = originalAggregate.aggregateExpressions.map {
              e => replaceAttributes(e, replaceMap).asInstanceOf[NamedExpression]
            }
            Aggregate(groupExpressions, aggregateExpressions, filterPlan.get)
          case None => originalAggregate.withNewChildren(Seq(filterPlan.get))
        }
        Some(replacedAggregate)
      }
    }

    lazy val hasAggregateWithFilter = originalAggregate.aggregateExpressions.exists {
      e => hasAggregateExpressionsWithFilter(e)
    }

    // The output results which are not aggregate expressions.
    lazy val resultGroupingExpressions = aggregatePlan match {
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
          val aggregate = aggregatePlan.get.asInstanceOf[Aggregate]
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
   * @param analyzedInfo Optional information about the aggregate analysis.
   */
  case class AnalyzedPlan(plan: LogicalPlan, analyzedInfo: Option[AggregateAnalzyInfo])

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.resolved) {
      logError(s"xxx visit plan:\n$plan")
      val newPlan = visitPlan(plan)
      logError(s"xxx output attributes:\n${newPlan.output}\n${plan.output}")
      logError(s"xxx rewrite plan:\n$newPlan")
      newPlan
    } else {
      logError(s"xxx plan not resolved:\n$plan")
      plan
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan match {
      case union: Union =>
        val planGroups = groupStructureMatchedAggregate(union)
        val newUnionClauses = planGroups.map {
          groupedPlans =>
            if (groupedPlans.length == 1) {
              groupedPlans.head.plan
            } else {
              val firstAggregateAnalzyInfo = groupedPlans.head.analyzedInfo.get
              val aggregates = groupedPlans.map(_.analyzedInfo.get.aggregatePlan.get)
              val replaceAttributes = collectReplaceAttributes(aggregates)
              val filterConditions = buildAggregateCasesConditions(aggregates, replaceAttributes)
              logError(s"xxx filterConditions. ${filterConditions.length},\n$filterConditions")
              val firstAggregateFilter =
                firstAggregateAnalzyInfo.filterPlan.get.asInstanceOf[Filter]

              // Add a filter step with condition `cond1 or cond2 or ...`, `cond_i` comes from each
              // union clause. Apply this filter on the source plan.
              val unionFilter = Filter(
                buildUnionConditionForAggregateSource(filterConditions),
                firstAggregateAnalzyInfo.sourcePlan.get)

              // Wrap all the attributes into a single structure attribute.
              val wrappedAttributesProject = buildStructWrapperProject(
                unionFilter,
                groupedPlans,
                filterConditions,
                replaceAttributes)

              // Build an array which element are response to each union clause.
              val arrayProject = buildArrayProject(wrappedAttributesProject, filterConditions)

              // Explode the array
              val explode = buildArrayExplode(arrayProject)

              // Null value means that the union clause does not have the corresponding data.
              val notNullFilter = Filter(IsNotNull(explode.output.head), explode)

              // Destruct the struct attribute.
              val destructStructProject = buildDestructStructProject(notNullFilter)

              buildAggregateWithGroupId(destructStructProject, groupedPlans)
            }
        }
        logError(s"xxx newUnionClauses. ${newUnionClauses.length},\n$newUnionClauses")
        val coalesePlan = if (newUnionClauses.length == 1) {
          newUnionClauses.head
        } else {
          var firstUnionChild = newUnionClauses.head
          for (i <- 1 until newUnionClauses.length - 1) {
            firstUnionChild = Union(firstUnionChild, newUnionClauses(i))
          }
          Union(firstUnionChild, newUnionClauses.last)
        }
        logError(s"xxx coalesePlan:$coalesePlan")

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
      case _ => plan.withNewChildren(plan.children.map(visitPlan))
    }
    // newPlan.copyTagsFrom(plan)
    newPlan
  }

  def isSupportedAggregate(info: AggregateAnalzyInfo): Boolean = {
    if (info.hasAggregateWithFilter) {
      return false
    }
    if (!info.aggregatePlan.isDefined) {
      return false
    }

    if (info.positionInGroupingKeys.exists(_ < 0)) {
      return false
    }

    // `agg_fun1(x) + agg_fun2(y)` is supported, but `agg_fun1(x) + y` is not supported.
    if (
      info.originalAggregate.aggregateExpressions.exists {
        e =>
          val innerExpr = removeAlias(e)
          if (hasAggregateExpression(innerExpr)) {
            !innerExpr.isInstanceOf[AggregateExpression] &&
            !innerExpr.children.forall(e => isAggregateExpression(e))
          } else {
            false
          }
      }
    ) {
      return false
    }

    if (!info.aggregatePlan.isDefined) {
      return false
    }
    true
  }

  /**
   * Checks if two AggregateAnalzyInfo instances have the same structure.
   *
   * This method compares the aggregate expressions, grouping expressions, and the source plans of
   * the two AggregateAnalzyInfo instances to determine if they have the same structure.
   *
   * @param l
   *   The first AggregateAnalzyInfo instance.
   * @param r
   *   The second AggregateAnalzyInfo instance.
   * @return
   *   True if the two instances have the same structure, false otherwise.
   */
  def areStructureMatchedAggregate(l: AggregateAnalzyInfo, r: AggregateAnalzyInfo): Boolean = {
    val lAggregate = l.aggregatePlan.get.asInstanceOf[Aggregate]
    val rAggregate = r.aggregatePlan.get.asInstanceOf[Aggregate]

    // Check aggregate result expressions. need same schema.
    if (lAggregate.aggregateExpressions.length != rAggregate.aggregateExpressions.length) {
      logError(s"xxx not equal 1")

      return false
    }
    val allAggregateExpressionAreSame =
      lAggregate.aggregateExpressions.zip(rAggregate.aggregateExpressions).forall {
        case (lExpr, rExpr) =>
          if (!lExpr.dataType.equals(rExpr.dataType)) {
            false
          } else {
            (hasAggregateExpression(lExpr), hasAggregateExpression(rExpr)) match {
              case (true, true) =>
                areStructureMatchedExpressions(lExpr, rExpr)
              case (false, true) => false
              case (true, false) => false
              case (false, false) => true
            }
          }
      }
    if (!allAggregateExpressionAreSame) {
      return false
    }

    // Check grouping expressions, need same schema.
    if (lAggregate.groupingExpressions.length != rAggregate.groupingExpressions.length) {
      logError(s"xxx not equal 3")
      return false
    }
    if (
      l.positionInGroupingKeys.length !=
        r.positionInGroupingKeys.length
    ) {
      logError(s"xxx not equal 4")
      return false
    }
    val allSameGroupingKeysRef = l.positionInGroupingKeys
      .zip(r.positionInGroupingKeys)
      .forall { case (lPos, rPos) => lPos == rPos }
    if (!allSameGroupingKeysRef) {
      logError(s"xxx not equal 5")
      return false
    }

    // Must come from same source.
    if (!areSameAggregateSource(l.sourcePlan.get, r.sourcePlan.get)) {
      logError(s"xxx not same source. ${l.sourcePlan.get}\n${r.sourcePlan.get}")
      return false
    }

    true
  }

  /*
   * Finds the index of the first group in `planGroups` that has the same structure as the given
   * `analyzedInfo`.
   *
   * This method iterates over the `planGroups` and checks if the first `AnalyzedPlan` in each group
   * has an `analyzedInfo` that matches the structure of the provided `analyzedInfo`. If a match is
   * found, the index of the group is returned. If no match is found, -1 is returned.
   *
   * @param planGroups
   *   An ArrayBuffer of ArrayBuffers, where each inner ArrayBuffer contains `AnalyzedPlan`
   *   instances.
   * @param analyzedInfo
   *   The `AggregateAnalzyInfo` to match against the groups in `planGroups`.
   * @return
   *   The index of the first group with a matching structure, or -1 if no match is found.
   */
  def findStructureMatchedAggregate(
      planGroups: ArrayBuffer[ArrayBuffer[AnalyzedPlan]],
      analyzedInfo: AggregateAnalzyInfo): Int = {
    planGroups.zipWithIndex.find(
      planWithIndex =>
        planWithIndex._1.head.analyzedInfo.isDefined &&
          areStructureMatchedAggregate(
            planWithIndex._1.head.analyzedInfo.get,
            analyzedInfo)) match {
      case Some((_, i)) => i
      case None => -1
    }

  }

  // Union only has two children. It's children may also be Union.
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

  def groupStructureMatchedAggregate(union: Union): ArrayBuffer[ArrayBuffer[AnalyzedPlan]] = {
    val groupResults = ArrayBuffer[ArrayBuffer[AnalyzedPlan]]()
    collectAllUnionClauses(union).foreach {
      case agg: Aggregate =>
        val analyzedInfo = AggregateAnalzyInfo(agg)
        if (isSupportedAggregate(analyzedInfo)) {
          if (groupResults.isEmpty) {
            groupResults += ArrayBuffer(AnalyzedPlan(agg, Some(analyzedInfo)))
          } else {
            val idx = findStructureMatchedAggregate(groupResults, analyzedInfo)
            if (idx != -1) {
              groupResults(idx) += AnalyzedPlan(agg, Some(analyzedInfo))
            } else {
              groupResults += ArrayBuffer(AnalyzedPlan(agg, Some(analyzedInfo)))
            }
          }
        } else {
          logError(s"xxx not supported. $agg")
          groupResults += ArrayBuffer(AnalyzedPlan(agg, None))
        }
      case other =>
        groupResults += ArrayBuffer(AnalyzedPlan(other, None))
    }
    groupResults
  }

  def areStructureMatchedExpressions(l: Expression, r: Expression): Boolean = {
    (l, r) match {
      case (lAttr: Attribute, rAttr: Attribute) =>
        logError(s"xxx attr equal: ${lAttr.qualifiedName}, ${rAttr.qualifiedName}")
        lAttr.qualifiedName == rAttr.qualifiedName
      case (lLiteral: Literal, rLiteral: Literal) =>
        lLiteral.value.equals(rLiteral.value)
      case _ =>
        if (l.children.length != r.children.length || l.getClass != r.getClass) {
          false
        } else {
          l.children.zip(r.children).forall {
            case (lChild, rChild) => areStructureMatchedExpressions(lChild, rChild)
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
          logError(s"xxx table equal: $lTable, $rTable")
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
        val analyzedInfo = aggregateCase.analyzedInfo.get
        val aggregate = analyzedInfo.aggregatePlan.get.asInstanceOf[Aggregate]
        val structFields = ArrayBuffer[Expression]()
        var fieldIndex: Int = 0
        aggregate.groupingExpressions.foreach {
          e =>
            structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
            structFields += replaceAttributes(e, replaceMap)
            fieldIndex += 1
        }
        for (i <- 0 until analyzedInfo.positionInGroupingKeys.length) {
          val position = analyzedInfo.positionInGroupingKeys(i)
          if (position >= fieldIndex) {
            val expr = analyzedInfo.resultGroupingExpressions(i)
            structFields += Literal(UTF8String.fromString(s"$structPrefix$fieldIndex"), StringType)
            structFields += replaceAttributes(analyzedInfo.resultGroupingExpressions(i), replaceMap)
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
                        structFields += replaceAttributes(child, replaceMap)
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
                    structFields += replaceAttributes(other, replaceMap)
                    fieldIndex += 1
                }
              }
              collectExpressionsInAggregateExpression(e)
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
    val firstAggregateAnalzyInfo = groupedPlans.head.analyzedInfo.get
    val aggregateTemplate = firstAggregateAnalzyInfo.aggregatePlan.get.asInstanceOf[Aggregate]
    val analyzedInfo = groupedPlans.head.analyzedInfo.get

    val totalGroupingExpressionsCount =
      math.max(
        aggregateTemplate.groupingExpressions.length,
        analyzedInfo.positionInGroupingKeys.max + 1)

    val groupingExpressions = attributes
      .slice(0, totalGroupingExpressionsCount)
      .map(_.asInstanceOf[Expression]) :+ attributes.last

    val normalExpressionPosition = analyzedInfo.positionInGroupingKeys
    var normalExpressionCount = 0
    var aggregateExpressionIndex = totalGroupingExpressionsCount
    val aggregateExpressions = ArrayBuffer[NamedExpression]()
    aggregateTemplate.aggregateExpressions.foreach {
      e =>
        removeAlias(e) match {
          case aggExpr if hasAggregateExpression(aggExpr) =>
            aggregateExpressions += makeAlias(
              constructAggregateExpression(aggExpr, attributes, aggregateExpressionIndex),
              e.name)
              .asInstanceOf[NamedExpression]
            aggregateExpressionIndex += aggExpr.children.length
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

  def constructAggregateExpression(
      aggExpr: Expression,
      attributes: Seq[Attribute],
      index: Int): Expression = {
    aggExpr match {
      case singleAggExpr: AggregateExpression =>
        val aggFunc = singleAggExpr.aggregateFunction
        val newAggFuncArgs = aggFunc.children.zipWithIndex.map {
          case (arg, i) =>
            attributes(index + i)
        }
        val newAggFunc =
          aggFunc.withNewChildren(newAggFuncArgs).asInstanceOf[AggregateFunction]
        AggregateExpression(
          newAggFunc,
          singleAggExpr.mode,
          singleAggExpr.isDistinct,
          singleAggExpr.filter,
          singleAggExpr.resultId)
      case combineAggExpr if hasAggregateExpression(combineAggExpr) =>
        combineAggExpr.withNewChildren(
          combineAggExpr.children.map(constructAggregateExpression(_, attributes, index)))
      case _ =>
        val normalExpr = attributes(index)
        normalExpr
    }
  }
}
