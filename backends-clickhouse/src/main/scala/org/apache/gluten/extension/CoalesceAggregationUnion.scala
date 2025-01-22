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
  override def apply(plan: LogicalPlan): LogicalPlan = {
    logError(s"xxx plan is resolved: ${plan.resolved}")
    if (plan.resolved) {
      logError(s"xxx visit plan:\n$plan")
      visitPlan(plan)
    } else {
      plan
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan match {
      case union: Union =>
        logError(s"xxx is union node, children: ${union.children}")
        val groups = groupUnionAggregations(union)
        val rewrittenGroups = groups.map {
          group =>
            if (group.length == 1) {
              group.head
            } else {
              val replaceMap = collectReplaceAttributes(group)
              val filter = group.head.asInstanceOf[Aggregate].child.asInstanceOf[Filter]
              logError(s"xxx replace map: $replaceMap")
              val (groupConds, unionCond) = buildGroupConditions(group, replaceMap)
              logError(s"xxx replace condition: $groupConds, $unionCond")
              val explodeResult = explodeBranches(group, groupConds, replaceMap)
              val unionFilter = Filter(unionCond, filter.child)
              val project = Project(Seq(explodeResult), unionFilter)
              logError(s"xxx new project\n$project")
              val nullFilter = Filter(IsNotNull(explodeResult), project)
              logError(s"xxx new null filter\n$nullFilter")
              group.head
            }
        }
        if (rewrittenGroups.length == 1) {
          rewrittenGroups.head
        } else {
          union.withNewChildren(rewrittenGroups)
        }
      case project: Project =>
        project.projectList.foreach(
          e => logError(s"xxx project expression: ${removeAlias(e).getClass}, $e"))
        project.withNewChildren(project.children.map(visitPlan))
      case _ => plan.withNewChildren(plan.children.map(visitPlan))
    }
    newPlan.copyTagsFrom(plan)
    newPlan
  }

  def removeAlias(e: Expression): Expression = {
    e match {
      case alias: Alias => alias.child
      case _ => e
    }
  }

  def groupUnionAggregations(union: Union): ArrayBuffer[ArrayBuffer[LogicalPlan]] = {
    val groupResults = ArrayBuffer[ArrayBuffer[LogicalPlan]]()
    union.children.foreach {
      case agg @ Aggregate(_, _, filter: Filter) =>
        agg.groupingExpressions.foreach(
          e => logError(s"xxx grouping expression: ${e.getClass}, $e"))
        agg.aggregateExpressions.foreach(
          e => logError(s"xxx aggregate expression: ${e.getClass}, $e"))
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

  def buildStructType(attributes: Seq[Attribute]): StructType = {
    val fields = attributes.map {
      attr => StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
    }
    StructType(fields :+ StructField(CoalesceAggregationUnion.UNION_TAG_FIELD, IntegerType, false))
  }

  def buildStructType1(
      group: ArrayBuffer[LogicalPlan],
      replaceMap: Map[String, Attribute]): StructType = {
    var attributesSet: AttributeSet = null
    val attributes = group.foreach {
      plan =>
        val agg = plan.asInstanceOf[Aggregate]
        val filter = agg.child.asInstanceOf[Filter]
        agg.groupingExpressions
          .map(e => replaceAttributes(e, replaceMap))
          .foreach(
            e =>
              if (attributesSet == null) {
                attributesSet = e.references
              } else {
                attributesSet ++= e.references
              })
        agg.aggregateExpressions
          .map(e => replaceAttributes(e, replaceMap))
          .foreach(e => attributesSet ++= e.references)
        filter.output
          .map(e => replaceAttributes(e, replaceMap))
          .foreach(e => attributesSet ++= e.references)
    }
    logError(s"xxx all needed attributes: $attributesSet")
    val fields = ArrayBuffer[StructField]()
    attributesSet.foreach {
      attr => fields += StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
    }
    StructType(fields :+ StructField(CoalesceAggregationUnion.UNION_TAG_FIELD, IntegerType, false))
  }

  def collectGroupRequiredAttributes(
      group: ArrayBuffer[LogicalPlan],
      replaceMap: Map[String, Attribute]): AttributeSet = {
    var attributesSet: AttributeSet = null
    group.foreach {
      plan =>
        val agg = plan.asInstanceOf[Aggregate]
        val filter = agg.child.asInstanceOf[Filter]
        agg.groupingExpressions
          .map(e => replaceAttributes(e, replaceMap))
          .foreach(
            e =>
              if (attributesSet == null) {
                attributesSet = e.references
              } else {
                attributesSet ++= e.references
              })
        agg.aggregateExpressions
          .map(e => replaceAttributes(e, replaceMap))
          .foreach(e => attributesSet ++= e.references)
        filter.output
          .map(e => replaceAttributes(e, replaceMap))
          .foreach(e => attributesSet ++= e.references)
    }
    attributesSet
  }

  def concatAllNotAggregateExpressionsInAggregate(
      group: ArrayBuffer[LogicalPlan],
      replaceMap: Map[String, Attribute]): Seq[NamedExpression] = {

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
      val field = If(groupConditions(i), structData, Literal(null, groupStructs(i).dataType))
      arrrayFields += field
    }
    CreateArray(arrrayFields.toSeq)
  }

  def explodeBranches(
      group: ArrayBuffer[LogicalPlan],
      groupConditions: ArrayBuffer[Expression],
      replaceMap: Map[String, Attribute]): NamedExpression = {
    Alias(
      Explode(buildBranchesArray(group, groupConditions, replaceMap)),
      "explode_branches_result")()
  }
}

object CoalesceAggregationUnion {
  val UNION_TAG_FIELD: String = "union_tag_"
}
