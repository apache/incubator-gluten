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

import io.substrait.spark.SparkExtension
import io.substrait.spark.expression._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.substrait.ToSubstraitType
import org.apache.spark.substrait.ToSubstraitType.toNamedStruct

import io.substrait.{proto, relation}
import io.substrait.debug.TreePrinter
import io.substrait.expression.{Expression => SExpression, ExpressionCreator}
import io.substrait.extension.ExtensionCollector
import io.substrait.plan.{ImmutablePlan, ImmutableRoot, Plan}
import io.substrait.relation.RelProtoConverter

import java.util.Collections

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ToSubstraitRel extends AbstractLogicalPlanVisitor with Logging {

  private val toSubstraitExp = new WithLogicalSubQuery(this)

  private val TRUE = ExpressionCreator.bool(false, true)

  override def default(p: LogicalPlan): relation.Rel = p match {
    case p: LeafNode => convertReadOperator(p)
    case other => t(other)
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
    val invocation =
      SparkExtension.toAggregateFunction.apply(expression, substraitExps)
    relation.Aggregate.Measure.builder.function(invocation).build()
  }

  private def collectAggregates(
      resultExpressions: Seq[NamedExpression],
      aggExprToOutputOrdinal: mutable.HashMap[Expression, Int]): Seq[AggregateExpression] = {
    var ordinal = 0
    resultExpressions.flatMap {
      expr =>
        expr.collect {
          // Do not push down duplicated aggregate expressions. For example,
          // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
          // `max(a)` to the data source.
          case agg: AggregateExpression if !aggExprToOutputOrdinal.contains(agg.canonicalized) =>
            aggExprToOutputOrdinal(agg.canonicalized) = ordinal
            ordinal += 1
            agg
        }
    }
  }

  private def translateAggregation(
      groupBy: Seq[Expression],
      aggregates: Seq[AggregateExpression],
      output: Seq[Attribute],
      input: relation.Rel): relation.Aggregate = {
    val groupings = Collections.singletonList(fromGroupSet(groupBy, output))
    val aggCalls = aggregates.map(fromAggCall(_, output)).asJava

    relation.Aggregate.builder
      .input(input)
      .addAllGroupings(groupings)
      .addAllMeasures(aggCalls)
      .build
  }

  /**
   * The current substrait [[relation.Aggregate]] can't specify output, but spark [[Aggregate]]
   * allow. So To support #1 <code>select max(b) from table group by a</code>, and #2 <code>select
   * a, max(b) + 1 from table group by a</code>, We need create [[Project]] on top of [[Aggregate]]
   * to correctly support it.
   *
   * TODO: support [[Rollup]] and [[GroupingSets]]
   */
  override def visitAggregate(agg: Aggregate): relation.Rel = {
    val input = visit(agg.child)
    val actualResultExprs = agg.aggregateExpressions
    val actualGroupExprs = agg.groupingExpressions

    val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
    val aggregates = collectAggregates(actualResultExprs, aggExprToOutputOrdinal)
    val aggOutputMap = aggregates.zipWithIndex.map {
      case (e, i) =>
        AttributeReference(s"agg_func_$i", e.dataType)() -> e
    }
    val aggOutput = aggOutputMap.map(_._1)

    // collect group by
    val groupByExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
    actualGroupExprs.zipWithIndex.foreach {
      case (expr, ordinal) =>
        if (!groupByExprToOutputOrdinal.contains(expr.canonicalized)) {
          groupByExprToOutputOrdinal(expr.canonicalized) = ordinal
        }
    }
    val groupOutputMap = actualGroupExprs.zipWithIndex.map {
      case (e, i) =>
        AttributeReference(s"group_col_$i", e.dataType)() -> e
    }
    val groupOutput = groupOutputMap.map(_._1)

    val substraitAgg = translateAggregation(actualGroupExprs, aggregates, agg.child.output, input)
    val newOutput = groupOutput ++ aggOutput

    val projectExpressions = actualResultExprs.map {
      expr =>
        expr.transformDown {
          case agg: AggregateExpression =>
            val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
            aggOutput(ordinal)
          case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
            val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
            groupOutput(ordinal)
        }
    }
    val projects = projectExpressions.map(toExpression(newOutput))
    relation.Project.builder
      .remap(relation.Rel.Remap.offset(newOutput.size, projects.size))
      .expressions(projects.asJava)
      .input(substraitAgg)
      .build()
  }

  private def asLong(e: Expression): Long = e match {
    case IntegerLiteral(limit) => limit
    case other => throw new UnsupportedOperationException(s"Unknown type: $other")
  }

  private def fetchBuilder(limit: Long, global: Boolean): relation.ImmutableFetch.Builder = {
    val offset = if (global) 1L else -1L
    relation.Fetch
      .builder()
      .count(limit)
      .offset(offset)
  }
  override def visitGlobalLimit(p: GlobalLimit): relation.Rel = {
    fetchBuilder(asLong(p.limitExpr), global = true)
      .input(visit(p.child))
      .build()
  }

  override def visitLocalLimit(p: LocalLimit): relation.Rel = {
    fetchBuilder(asLong(p.limitExpr), global = false)
      .input(visit(p.child))
      .build()
  }

  override def visitFilter(p: Filter): relation.Rel = {
    val condition = toExpression(p.child.output)(p.condition)
    relation.Filter.builder().condition(condition).input(visit(p.child)).build()
  }

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

  override def visitProject(p: Project): relation.Rel = {
    val expressions = p.projectList.map(toExpression(p.child.output)).toList
    relation.Project.builder
      .remap(relation.Rel.Remap.offset(p.child.output.size, expressions.size))
      .expressions(expressions.asJava)
      .input(visit(p.child))
      .build()
  }

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

  private def toExpression(output: Seq[Attribute])(e: Expression): SExpression = {
    toSubstraitExp(e, output)
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
                  buf += ToSubstraitLiteral.apply(l)
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
  def apply(p: LogicalPlan): Plan = {
    val rel = visit(p)
    ImmutablePlan.builder
      .roots(
        Collections.singletonList(
          ImmutableRoot.builder().input(rel).addAllNames(p.output.map(_.name).asJava).build()
        ))
      .build()
  }

  def tree(p: LogicalPlan): String = {
    TreePrinter.tree(visit(p))
  }

  def toProtoSubstrait(p: LogicalPlan): Array[Byte] = {
    val substraitRel = visit(p)

    val extensionCollector = new ExtensionCollector
    val relProtoConverter = new RelProtoConverter(extensionCollector)
    val builder = proto.Plan
      .newBuilder()
      .addRelations(
        proto.PlanRel
          .newBuilder()
          .setRel(substraitRel
            .accept(relProtoConverter))
      )
    extensionCollector.addExtensionsToPlan(builder)
    builder.build().toByteArray
  }
}

private[logical] class WithLogicalSubQuery(toSubstraitRel: ToSubstraitRel)
  extends ToSubstraitExpression {
  override protected val toScalarFunction: ToScalarFunction =
    ToScalarFunction(SparkExtension.SparkScalarFunctions)

  override protected def translateSubQuery(expr: PlanExpression[_]): Option[SExpression] = {
    expr match {
      case s: ScalarSubquery if s.outerAttrs.isEmpty && s.joinCond.isEmpty =>
        val rel = toSubstraitRel.visit(s.plan)
        Some(
          SExpression.ScalarSubquery.builder
            .input(rel)
            .`type`(ToSubstraitType.apply(s.dataType, s.nullable))
            .build())
      case other => default(other)
    }
  }
}
