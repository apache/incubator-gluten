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
import org.apache.gluten.execution._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types._

/*
 * For aggregation with grouping sets, we need to expand the grouping sets
 * to individual group by.
 * 1. It need to make copies of the original data.
 * 2. run the aggregation on the multi copied data.
 * Both of these two are expensive.
 *
 * We could do this as following
 * 1. Run the aggregation on full grouping keys.
 * 2. Expand the aggregation result to the full grouping sets.
 * 3. Run the aggregation on the expanded data.
 *
 * So the plan is transformed from
 *   expand -> partial aggregating -> shuffle -> final merge aggregating
 * to
 *   partial aggregating -> expand -> shuffle -> final merge aggregating
 *
 * Notice:
 * If the aggregation involves distinct, we can't do this optimization.
 */

case class LazyAggregateExpandRule(session: SparkSession) extends Rule[SparkPlan] with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {
    logDebug(
      s"xxx 1031 enable lazy aggregate expand: " +
        s"${CHBackendSettings.enableLazyAggregateExpand}")
    if (!CHBackendSettings.enableLazyAggregateExpand) {
      return plan
    }
    plan.transformUp {
      case shuffle @ ColumnarShuffleExchangeExec(
            HashPartitioning(hashExpressions, _),
            CHHashAggregateExecTransformer(
              _,
              groupingExpressions,
              aggregateExpressions,
              _,
              _,
              resultExpressions,
              ExpandExecTransformer(projections, output, child)),
            _,
            _,
            _
          ) =>
        logDebug(s"xxx match plan:$shuffle")
        val partialAggregate = shuffle.child.asInstanceOf[CHHashAggregateExecTransformer]
        val expand = partialAggregate.child.asInstanceOf[ExpandExecTransformer]
        logDebug(
          s"xxx partialAggregate: groupingExpressions:" +
            s"${partialAggregate.groupingExpressions}\n" +
            s"aggregateAttributes:${partialAggregate.aggregateAttributes}\n" +
            s"aggregateExpressions:${partialAggregate.aggregateExpressions}\n" +
            s"resultExpressions:${partialAggregate.resultExpressions}")
        if (isSupportedAggregate(partialAggregate, expand, shuffle)) {

          val attributesToReplace = buildReplaceAttributeMap(expand)
          logDebug(s"xxx attributesToReplace: $attributesToReplace")

          val newPartialAggregate = buildNewAggregateExec(
            partialAggregate,
            expand,
            attributesToReplace
          )

          val newExpand = buildNewExpandExec(
            expand,
            partialAggregate,
            newPartialAggregate,
            attributesToReplace
          )

          val newShuffle = shuffle.copy(child = newExpand)
          logDebug(s"xxx new plan: $newShuffle")
          newShuffle
        } else {
          shuffle
        }
      case shuffle @ ColumnarShuffleExchangeExec(
            HashPartitioning(hashExpressions, _),
            CHHashAggregateExecTransformer(
              _,
              groupingExpressions,
              aggregateExpressions,
              _,
              _,
              resultExpressions,
              FilterExecTransformer(_, ExpandExecTransformer(projections, output, child))),
            _,
            _,
            _
          ) =>
        val partialAggregate = shuffle.child.asInstanceOf[CHHashAggregateExecTransformer]
        val filter = partialAggregate.child.asInstanceOf[FilterExecTransformer]
        val expand = filter.child.asInstanceOf[ExpandExecTransformer]
        logDebug(
          s"xxx partialAggregate: groupingExpressions:" +
            s"${partialAggregate.groupingExpressions}\n" +
            s"aggregateAttributes:${partialAggregate.aggregateAttributes}\n" +
            s"aggregateExpressions:${partialAggregate.aggregateExpressions}\n" +
            s"resultExpressions:${partialAggregate.resultExpressions}")
        if (isSupportedAggregate(partialAggregate, expand, shuffle)) {
          val attributesToReplace = buildReplaceAttributeMap(expand)
          logDebug(s"xxx attributesToReplace: $attributesToReplace")

          val newPartialAggregate = buildNewAggregateExec(
            partialAggregate,
            expand,
            attributesToReplace
          )

          val newExpand = buildNewExpandExec(
            expand,
            partialAggregate,
            newPartialAggregate,
            attributesToReplace
          )

          val newFilter = filter.copy(child = newExpand)

          val newShuffle = shuffle.copy(child = newFilter)
          logDebug(s"xxx new plan: $newShuffle")
          newShuffle

        } else {
          shuffle
        }
    }
  }

  // Just enable for simple cases. Some of cases that are not supported:
  // 1. select count(a),count(b), count(1), count(distinct(a)), count(distinct(b)) from values
  //    (1, null), (2,2) as data(a,b);
  // 2. select n_name, count(distinct n_regionkey) as col1,
  //    count(distinct concat(n_regionkey, n_nationkey)) as col2 from
  //    nation group by n_name;
  def isSupportedAggregate(
      aggregate: CHHashAggregateExecTransformer,
      expand: ExpandExecTransformer,
      shuffle: ColumnarShuffleExchangeExec): Boolean = {
    // all grouping keys must be attribute references
    val expandOutputAttributes = expand.child.output.toSet
    if (aggregate.groupingExpressions.exists(!_.isInstanceOf[Attribute])) {
      logDebug(s"xxx Not all grouping expression are attribute references")
      return false
    }
    // all shuffle keys must be attribute references
    if (
      shuffle.outputPartitioning
        .asInstanceOf[HashPartitioning]
        .expressions
        .exists(!_.isInstanceOf[Attribute])
    ) {
      logDebug(s"xxx Not all shuffle hash expression are attribute references")
      return false
    }

    // 1. for safty, we don't enbale this optimization for all aggregate functions.
    // 2. if any aggregate function uses attributes from expand's child, we don't enable this
    if (
      !aggregate.aggregateExpressions.forall(
        e =>
          isSupportedAggregateFunction(e) &&
            e.aggregateFunction.references.forall(expandOutputAttributes.contains(_)))
    ) {
      logDebug(s"xxx Some aggregate functions are not supported")
      return false
    }

    // ensure the last column of expand is grouping id
    val groupIdIndex = findGroupingIdIndex(expand)
    logDebug(s"xxx Find group id at index: $groupIdIndex")
    if (groupIdIndex == -1) {
      return false;
    }
    val groupIdAttribute = expand.output(groupIdIndex)
    if (
      !groupIdAttribute.name.startsWith("grouping_id") && !groupIdAttribute.name.startsWith("gid")
      && !groupIdAttribute.name.startsWith("spark_grouping_id")
    ) {
      logDebug(s"xxx Not found group id column at index $groupIdIndex")
      return false
    }
    expand.projections.forall {
      projection =>
        val groupId = projection(groupIdIndex)
        groupId
          .isInstanceOf[Literal] && (groupId.dataType.isInstanceOf[LongType] || groupId.dataType
          .isInstanceOf[IntegerType])
    }
  }

  def findGroupingIdIndex(expand: ExpandExecTransformer): Int = {
    var groupIdIndexes = Seq[Int]()
    for (col <- 0 until expand.output.length) {
      val expandCol = expand.projections(0)(col)
      if (
        expandCol.isInstanceOf[Literal] && (expandCol.dataType
          .isInstanceOf[LongType] || expandCol.dataType.isInstanceOf[IntegerType])
      ) {
        if (
          expand.projections.forall {
            projection =>
              val e = projection(col)
              e.isInstanceOf[Literal] &&
              (e.dataType.isInstanceOf[LongType] || e.dataType.isInstanceOf[IntegerType])
          }
        ) {
          groupIdIndexes +:= col
        }
      }
    }
    if (groupIdIndexes.length == 1) {
      groupIdIndexes(0)
    } else {
      -1
    }
  }

  // Some of aggregate functions' output columns are not consistent with the output of gluten.
  // - average: in partial aggregation, the outputs are sum and count, but gluten only generates one
  //   column, avg.
  // - sum: if the input's type is decimal, the output are sum and isEmpty, but gluten doesn't use
  //   the isEmpty column.
  def isSupportedAggregateFunction(aggregateExpression: AggregateExpression): Boolean = {
    if (aggregateExpression.filter.isDefined) {
      return false
    }
    aggregateExpression.aggregateFunction match {
      case _: Count => true
      case _: Max => true
      case _: Min => true
      case sum: Sum => !sum.dataType.isInstanceOf[DecimalType]
      case _ => false
    }
  }

  def getReplaceAttribute(
      toReplace: Attribute,
      attributesToReplace: Map[Attribute, Attribute]): Attribute = {
    attributesToReplace.getOrElse(toReplace, toReplace)
  }

  def buildReplaceAttributeMap(expand: ExpandExecTransformer): Map[Attribute, Attribute] = {
    var fullExpandProjection = Seq[Expression]()
    for (i <- 0 until expand.projections(0).length) {
      val attr = expand.projections.find(x => x(i).isInstanceOf[Attribute]) match {
        case Some(projection) => projection(i).asInstanceOf[Attribute]
        case None => null
      }
      fullExpandProjection = fullExpandProjection :+ attr
    }

    var attributeMap = Map[Attribute, Attribute]()
    for (i <- 0 until expand.output.length) {
      if (fullExpandProjection(i).isInstanceOf[Attribute]) {
        attributeMap += (expand.output(i) -> fullExpandProjection(i).asInstanceOf[Attribute])
      }
    }
    attributeMap
  }

  def buildNewExpandProjections(
      originalExpandProjections: Seq[Seq[Expression]],
      originalExpandOutput: Seq[Attribute],
      newExpandOutput: Seq[Attribute]): Seq[Seq[Expression]] = {
    val newExpandProjections = originalExpandProjections.map {
      projection =>
        newExpandOutput.map {
          attr =>
            val index = originalExpandOutput.indexWhere(_.semanticEquals(attr))
            if (index != -1) {
              projection(index)
            } else {
              attr
            }
        }
    }
    newExpandProjections
  }

  // Make the child of expand be the child of aggregate
  // Need to replace some attributes
  def buildNewAggregateExec(
      partialAggregate: CHHashAggregateExecTransformer,
      expand: ExpandExecTransformer,
      attributesToReplace: Map[Attribute, Attribute]): SparkPlan = {
    val groupIdAttribute = expand.output(findGroupingIdIndex(expand))

    // if the grouping keys contains literal, they should not be in attributesToReplace
    // And we need to remove them from the grouping keys
    val groupingExpressions = partialAggregate.groupingExpressions
    val newGroupingExpresion =
      groupingExpressions
        .filter(
          e => e.toAttribute != groupIdAttribute && attributesToReplace.contains(e.toAttribute))
        .map(e => getReplaceAttribute(e.toAttribute, attributesToReplace))
        .distinct
    logDebug(
      s"xxx newGroupingExpresion: $newGroupingExpresion,\n" +
        s"groupingExpressions: $groupingExpressions")

    // Also need to remove literal grouping keys from the result expressions
    val resultExpressions = partialAggregate.resultExpressions
    val newResultExpressions =
      resultExpressions
        .filter {
          e =>
            e.toAttribute != groupIdAttribute && (groupingExpressions
              .find(_.toAttribute == e.toAttribute)
              .isEmpty || attributesToReplace.contains(e.toAttribute))
        }
        .map(e => getReplaceAttribute(e.toAttribute, attributesToReplace))
        .distinct
    logDebug(
      s"xxx newResultExpressions: $newResultExpressions\n" +
        s"resultExpressions:$resultExpressions")
    partialAggregate.copy(
      groupingExpressions = newGroupingExpresion,
      resultExpressions = newResultExpressions,
      child = expand.child)
  }

  def buildNewExpandExec(
      expand: ExpandExecTransformer,
      partialAggregate: CHHashAggregateExecTransformer,
      child: SparkPlan,
      attributesToReplace: Map[Attribute, Attribute]): SparkPlan = {
    // The output of the native plan is not completely consistent with Spark.
    val aggregateOutput = partialAggregate.output
    logDebug(s"xxx aggregateResultAttributes: ${partialAggregate.aggregateResultAttributes}")
    logDebug(s"xxx aggregateOutput: $aggregateOutput")

    val newExpandProjections = buildNewExpandProjections(
      expand.projections,
      expand.output,
      aggregateOutput
    )
    logDebug(s"xxx newExpandProjections: $newExpandProjections\nprojections:${expand.projections}")
    ExpandExecTransformer(newExpandProjections, aggregateOutput, child)
  }

}
