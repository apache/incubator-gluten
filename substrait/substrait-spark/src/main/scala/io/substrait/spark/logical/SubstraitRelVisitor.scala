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
package io.substrait.spark.logical

import io.substrait.spark.ExpressionConverter
import io.substrait.spark.expression.LiteralConverter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.substrait.TypeConverter.toNamedStruct

import io.substrait.`type`.Type
import io.substrait.expression.{Expression => SExpression, ExpressionCreator}
import io.substrait.relation
import io.substrait.relation.Rel

import java.util.Collections

import scala.annotation.tailrec
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable.ArrayBuffer
class SubstraitRelVisitor extends LogicalPlanVisitor[relation.Rel] with Logging {

  private val TRUE = ExpressionCreator.bool(false, true)

  private def t(p: LogicalPlan): relation.Rel =
    throw new UnsupportedOperationException(s"Unable to convert the expression ${p.nodeName}")

  override def default(p: LogicalPlan): relation.Rel = p match {
    case p: LeafNode => convertReadOperator(p)
    case other => t(other)
  }

  @tailrec
  private def checkNoExtraExpr(x: Expression): Boolean = x match {
    case Alias(child, _) => checkNoExtraExpr(child)
    case MultiAlias(child, _) => checkNoExtraExpr(child)
    case _: AggregateExpression => true
    case _ => false
  }

  private def fromGroupSet(
      e: Seq[Expression],
      output: Seq[Attribute]): relation.Aggregate.Grouping = {

    relation.Aggregate.Grouping.builder
      .addAllExpressions(e.map(toExpression(output)).asJava)
      .build()
  }

  private def fromAggCall(
      expression: AggregateExpression,
      output: Seq[Attribute]): relation.Aggregate.Measure = {
    val substraitExps = expression.aggregateFunction.children.map(toExpression(output))
    val invocation = ExpressionConverter.aggregateConverter.apply(expression, substraitExps)
    relation.Aggregate.Measure.builder.function(invocation).build()
  }

  /**
   * The current substrait [[relation.Aggregate]] can't specify output, but spark [[Aggregate]]
   * allow. So we can't support #1 <code>select max(b) from table group by a</code>, and #2
   * <code>select a, max(b) + 1 from table group by a</code>
   *
   * We need create [[Project]] on top of [[Aggregate]] to correctly support it.
   *
   * TODO: support [[Rollup]] and [[GroupingSets]]
   */
  override def visitAggregate(agg: Aggregate): relation.Rel = {
    val input = visit(agg.child)
    val actualResultExprs = agg.aggregateExpressions
    val actualGroupExprs = agg.groupingExpressions
    val (aggInResult, projectInResult) = actualResultExprs.partition(_.find {
      case _: AggregateExpression => true
      case _ => false
    }.isDefined)
    val aggregates = aggInResult.flatMap(_.collect { case agg: AggregateExpression => agg })

    if (
      aggInResult.forall(checkNoExtraExpr) &&
      projectInResult.size == actualGroupExprs.size &&
      aggregates.size == aggInResult.size
    ) {

      val groupings = Collections.singletonList(fromGroupSet(projectInResult, agg.child.output))
      val aggCalls = aggregates.map(fromAggCall(_, agg.child.output)).asJava

      relation.Aggregate.builder
        .input(input)
        .addAllGroupings(groupings)
        .addAllMeasures(aggCalls)
        .build
    } else {
      t(agg)
    }
  }

  override def visitDistinct(p: Distinct): relation.Rel = t(p)

  override def visitExcept(p: Except): relation.Rel = t(p)

  override def visitExpand(p: Expand): relation.Rel = t(p)

  override def visitFilter(p: Filter): relation.Rel = {
    val condition = toExpression(p.child.output)(p.condition)
    relation.Filter.builder().condition(condition).input(visit(p.child)).build()
  }

  override def visitGenerate(p: Generate): relation.Rel = t(p)

  override def visitGlobalLimit(p: GlobalLimit): relation.Rel = t(p)

  override def visitIntersect(p: Intersect): relation.Rel = t(p)

  private def toSubstraitJoin(joinType: JoinType): relation.Join.JoinType = joinType match {
    case Inner | Cross => relation.Join.JoinType.INNER
    case LeftOuter => relation.Join.JoinType.LEFT
    case RightOuter => relation.Join.JoinType.RIGHT
    case FullOuter => relation.Join.JoinType.OUTER
    case LeftSemi => relation.Join.JoinType.SEMI
    case LeftAnti => relation.Join.JoinType.ANTI
    case other => throw new UnsupportedOperationException(s"Unsupported join type $other")
  }

  override def visitJoin(p: Join): relation.Rel = {
    val left = visit(p.left)
    val right = visit(p.right)
    val condition = p.condition.map(toExpression(p.left.output ++ p.right.output)).getOrElse(TRUE)
    val joinType = toSubstraitJoin(p.joinType)

    if (joinType == relation.Join.JoinType.INNER && TRUE == condition) {
      relation.Cross.builder
        .left(left)
        .right(right)
        .deriveRecordType(
          Type.Struct.builder
            .from(left.getRecordType)
            .from(right.getRecordType)
            .build)
        .build
    } else {
      relation.Join.builder
        .condition(condition)
        .joinType(joinType)
        .left(left)
        .right(right)
        .build
    }
  }

  override def visitLocalLimit(p: LocalLimit): relation.Rel = t(p)

  override def visitPivot(p: Pivot): relation.Rel = t(p)

  override def visitProject(p: Project): relation.Rel = {
    val expressions = p.projectList.map(toExpression(p.child.output)).toList
    relation.Project.builder
      .remap(Rel.Remap.offset(p.child.output.size, expressions.size))
      .expressions(expressions.asJava)
      .input(visit(p.child))
      .build()
  }

  override def visitRepartition(p: Repartition): relation.Rel = t(p)

  override def visitRepartitionByExpr(p: RepartitionByExpression): relation.Rel = t(p)

  override def visitSample(p: Sample): relation.Rel = t(p)

  override def visitScriptTransform(p: ScriptTransformation): relation.Rel = t(p)

  override def visitUnion(p: Union): relation.Rel = t(p)

  override def visitWindow(p: Window): relation.Rel = t(p)

  override def visitTail(p: Tail): relation.Rel = t(p)

  private def toSortField(output: Seq[Attribute] = Nil)(order: SortOrder): SExpression.SortField = {
    val direction = (order.direction, order.nullOrdering) match {
      case (Ascending, NullsFirst) => SExpression.SortDirection.ASC_NULLS_FIRST
      case (Descending, NullsFirst) => SExpression.SortDirection.DESC_NULLS_FIRST
      case (Ascending, NullsLast) => SExpression.SortDirection.ASC_NULLS_LAST
      case (Descending, NullsLast) => SExpression.SortDirection.DESC_NULLS_LAST
    }
    val expr = toExpression(output)(order.child)
    SExpression.SortField.builder().expr(expr).direction(direction).build()
  }
  override def visitSort(sort: Sort): relation.Rel = {
    val input = visit(sort.child)
    val fields = sort.order.map(toSortField(sort.child.output)).asJava
    relation.Sort.builder.addAllSortFields(fields).input(input).build
  }

  override def visitWithCTE(p: WithCTE): relation.Rel = t(p)

  private def toExpression(output: Seq[Attribute] = Nil)(e: Expression): SExpression = {
    ExpressionConverter.defaultConverter(e, output)
  }

  private def buildNamedScan(schema: StructType, tableNames: List[String]): relation.NamedScan = {
    val namedStruct = toNamedStruct(schema)
    val namedScan = relation.NamedScan.builder
      .initialSchema(namedStruct)
      .addAllNames(tableNames.asJava)
      .build
    namedScan
  }
  private def buildVirtualTableScan(localRelation: LocalRelation): relation.AbstractReadRel = {
    val namedStruct = toNamedStruct(localRelation.schema)

    if (localRelation.data.isEmpty) {
      relation.EmptyScan.builder().initialSchema(namedStruct).build()
    } else {
      relation.VirtualTableScan
        .builder()
        .addAllDfsNames(namedStruct.names())
        .addAllRows(
          localRelation.data
            .map(
              row => {
                var idx = 0
                val buf = new ArrayBuffer[SExpression.Literal](row.numFields)
                while (idx < row.numFields) {
                  val l = Literal.apply(row.get(idx, localRelation.schema(idx).dataType))
                  buf += LiteralConverter.convertWithThrow(l)
                  idx += 1
                }
                ExpressionCreator.struct(false, buf.asJava)
              })
            .asJava)
        .build()
    }
  }

  /** Read Operator: https://substrait.io/relations/logical_relations/#read-operator */
  private def convertReadOperator(plan: LeafNode): relation.AbstractReadRel = {
    var tableNames: List[String] = null
    plan match {
      case logicalRelation: LogicalRelation if logicalRelation.catalogTable.isDefined =>
        tableNames = logicalRelation.catalogTable.get.identifier.unquotedString.split("\\.").toList
        buildNamedScan(logicalRelation.schema, tableNames)
      case dataSourceV2ScanRelation: DataSourceV2ScanRelation =>
        tableNames = dataSourceV2ScanRelation.relation.identifier.get.toString.split("\\.").toList
        buildNamedScan(dataSourceV2ScanRelation.schema, tableNames)
      case dataSourceV2Relation: DataSourceV2Relation =>
        tableNames = dataSourceV2Relation.identifier.get.toString.split("\\.").toList
        buildNamedScan(dataSourceV2Relation.schema, tableNames)
      case hiveTableRelation: HiveTableRelation =>
        tableNames = hiveTableRelation.tableMeta.identifier.unquotedString.split("\\.").toList
        buildNamedScan(hiveTableRelation.schema, tableNames)
      // TODO: LocalRelation,Range=>Virtual Table,LogicalRelation(HadoopFsRelation)=>LocalFiles
      case localRelation: LocalRelation => buildVirtualTableScan(localRelation)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unable to convert the plan to a substrait NamedScan: $plan")
    }
  }
}
