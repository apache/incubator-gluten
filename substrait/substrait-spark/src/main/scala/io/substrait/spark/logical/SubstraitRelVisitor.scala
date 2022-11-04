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
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.substrait.TypeConverter.toNamedStruct

import io.substrait.expression.{Expression => SExpression, ExpressionCreator}
import io.substrait.relation
import io.substrait.relation.Rel

import java.util.Collections

import scala.annotation.tailrec
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.mutable.ArrayBuffer
class SubstraitRelVisitor extends LogicalPlanVisitor[relation.Rel] with Logging {

  private def t(): relation.Rel = throw new UnsupportedOperationException()

  override def default(p: LogicalPlan): relation.Rel = p match {
    case p: LeafNode => convertReadOperator(p)
    case _ => t()
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
      t()
    }
  }

  override def visitDistinct(p: Distinct): relation.Rel = t()

  override def visitExcept(p: Except): relation.Rel = t()

  override def visitExpand(p: Expand): relation.Rel = t()

  override def visitFilter(p: Filter): relation.Rel = {
    val condition = toExpression(p.child.output)(p.condition)
    relation.Filter.builder().condition(condition).input(visit(p.child)).build()
  }

  override def visitGenerate(p: Generate): relation.Rel = t()

  override def visitGlobalLimit(p: GlobalLimit): relation.Rel = t()

  override def visitIntersect(p: Intersect): relation.Rel = t()

  override def visitJoin(p: Join): relation.Rel = t()

  override def visitLocalLimit(p: LocalLimit): relation.Rel = t()

  override def visitPivot(p: Pivot): relation.Rel = t()

  override def visitProject(p: Project): relation.Rel = {
    val expressions = p.projectList.map(toExpression(p.child.output)).toList
    relation.Project.builder
      .remap(Rel.Remap.offset(p.child.output.size, expressions.size))
      .expressions(expressions.asJava)
      .input(visit(p.child))
      .build()
  }

  override def visitRepartition(p: Repartition): relation.Rel = t()

  override def visitRepartitionByExpr(p: RepartitionByExpression): relation.Rel = t()

  override def visitSample(p: Sample): relation.Rel = t()

  override def visitScriptTransform(p: ScriptTransformation): relation.Rel = t()

  override def visitUnion(p: Union): relation.Rel = t()

  override def visitWindow(p: Window): relation.Rel = t()

  override def visitTail(p: Tail): relation.Rel = t()

  override def visitSort(sort: Sort): relation.Rel = t()

  override def visitWithCTE(p: WithCTE): relation.Rel = t()

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
