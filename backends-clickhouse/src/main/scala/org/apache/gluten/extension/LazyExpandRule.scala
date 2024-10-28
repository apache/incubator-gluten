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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.exchange._

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

case class LazyExpandRule(session: SparkSession) extends Rule[SparkPlan] with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {
    logDebug(s"xxx enable lazy aggregate expand: ${CHBackendSettings.enableLazyAggregateExpand}")
    if (!CHBackendSettings.enableLazyAggregateExpand) {
      return plan
    }
    plan.transformUp {
      case finalAggregate @ HashAggregateExec(
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            ShuffleExchangeExec(
              HashPartitioning(hashExpressions, _),
              HashAggregateExec(
                _,
                _,
                _,
                groupingExpressions,
                aggregateExpressions,
                _,
                _,
                resultExpressions,
                ExpandExec(projections, output, child)),
              _
            )
          ) =>
        logError(s"xxx match plan:$finalAggregate")
        // move expand node after shuffle node
        if (
          groupingExpressions.forall(_.isInstanceOf[Attribute]) &&
          hashExpressions.forall(_.isInstanceOf[Attribute]) &&
          aggregateExpressions.forall(_.filter.isEmpty)
        ) {
          val shuffle =
            finalAggregate.asInstanceOf[HashAggregateExec].child.asInstanceOf[ShuffleExchangeExec]
          val partialAggregate = shuffle.child.asInstanceOf[HashAggregateExec]
          val expand = partialAggregate.child.asInstanceOf[ExpandExec]

          val attributesToReplace = buildReplaceAttributeMapForAggregate(
            groupingExpressions,
            projections,
            output
          )
          logError(s"xxx attributesToReplace: $attributesToReplace")

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
          val newFinalAggregate = finalAggregate.copy(child = newShuffle)
          logError(s"xxx new plan: $newFinalAggregate")
          newFinalAggregate
        } else {
          finalAggregate
        }
      case finalAggregate @ HashAggregateExec(
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            _,
            ShuffleExchangeExec(
              HashPartitioning(hashExpressions, _),
              HashAggregateExec(
                _,
                _,
                _,
                groupingExpressions,
                aggregateExpressions,
                _,
                _,
                resultExpressions,
                FilterExec(_, ExpandExec(projections, output, child))),
              _
            )
          ) =>
        logError(s"xxx match plan:$finalAggregate")
        if (
          groupingExpressions.forall(_.isInstanceOf[Attribute]) &&
          hashExpressions.forall(_.isInstanceOf[Attribute]) &&
          aggregateExpressions.forall(_.filter.isEmpty)
        ) {
          val shuffle =
            finalAggregate.asInstanceOf[HashAggregateExec].child.asInstanceOf[ShuffleExchangeExec]
          val partialAggregate = shuffle.child.asInstanceOf[HashAggregateExec]
          val filter = partialAggregate.child.asInstanceOf[FilterExec]
          val expand = filter.child.asInstanceOf[ExpandExec]

          val attributesToReplace = buildReplaceAttributeMapForAggregate(
            groupingExpressions,
            projections,
            output
          )
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
          val newFinalAggregate = finalAggregate.copy(child = newShuffle)
          logError(s"xxx new plan: $newFinalAggregate")
          newFinalAggregate
        } else {
          finalAggregate
        }
    }
  }

  def getReplaceAttribute(
      toReplace: Attribute,
      attributesToReplace: Map[Attribute, Attribute]): Attribute = {
    attributesToReplace.getOrElse(toReplace, toReplace)
  }

  def buildReplaceAttributeMapForAggregate(
      originalGroupingExpressions: Seq[NamedExpression],
      originalExpandProjections: Seq[Seq[Expression]],
      originalExpandOutput: Seq[Attribute]): Map[Attribute, Attribute] = {

    var fullExpandProjection = Seq[Expression]()
    for (i <- 0 until originalExpandProjections(0).length) {
      val attr = originalExpandProjections.find(x => x(i).isInstanceOf[Attribute]) match {
        case Some(projection) => projection(i).asInstanceOf[Attribute]
        case None => null
      }
      fullExpandProjection = fullExpandProjection :+ attr
    }
    var attributeMap = Map[Attribute, Attribute]()
    val groupIdAttribute = originalExpandOutput(originalExpandOutput.length - 1)
    originalGroupingExpressions.filter(_.toAttribute != groupIdAttribute).foreach {
      e =>
        val index = originalExpandOutput.indexWhere(_.semanticEquals(e.toAttribute))
        val attr = fullExpandProjection(index).asInstanceOf[Attribute]
        // if the grouping key is a literal, cast it to Attribute will be null
        if (attr != null) {
          attributeMap += (e.toAttribute -> attr)
        }
        // attributeMap +=(e.toAttribute -> fullExpandProjection(index).asInstanceOf[Attribute])
    }
    attributeMap
  }

  def buildNewExpandProjections(
      originalGroupingExpressions: Seq[NamedExpression],
      originalExpandProjections: Seq[Seq[Expression]],
      originalExpandOutput: Seq[Attribute],
      newExpandOutput: Seq[Attribute]): Seq[Seq[Expression]] = {
    var groupingKeysPosition = Map[String, Int]()
    originalGroupingExpressions.foreach {
      e =>
        e match {
          case ne: NamedExpression =>
            val index = originalExpandOutput.indexWhere(_.semanticEquals(ne.toAttribute))
            if (index != -1) {
              groupingKeysPosition += (ne.name -> index)
            }
          case _ =>
        }
    }

    val newExpandProjections = originalExpandProjections.map {
      projection =>
        val res = newExpandOutput.map {
          attr =>
            if (attr.isInstanceOf[Attribute]) {
              groupingKeysPosition.get(attr.name) match {
                case Some(attrPos) => projection(attrPos)
                case None => attr
              }
            } else {
              attr
            }
        }
        res
    }
    newExpandProjections
  }

  // Make the child of expand be the child of aggregate
  // Need to replace some attributes
  def buildNewAggregateExec(
      partialAggregate: HashAggregateExec,
      expand: ExpandExec,
      attributesToReplace: Map[Attribute, Attribute]): SparkPlan = {
    val expandOutput = expand.output
    // As far as know, the last attribute in the output is the groupId attribute.
    val groupIdAttribute = expandOutput(expandOutput.length - 1)

    // if the grouping keys contains literal, they should not be in attributesToReplace
    // And we need to remove them from the grouping keys
    val groupingExpressions = partialAggregate.groupingExpressions
    val newGroupingExpresion =
      groupingExpressions
        .filter(
          e => e.toAttribute != groupIdAttribute && attributesToReplace.contains(e.toAttribute))
        .map(e => getReplaceAttribute(e.toAttribute, attributesToReplace))
    logError(
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
    logError(
      s"xxx newResultExpressions: $newResultExpressions\n" +
        s"resultExpressions:$resultExpressions")
    partialAggregate.copy(
      groupingExpressions = newGroupingExpresion,
      resultExpressions = newResultExpressions,
      child = expand.child)
  }

  def buildNewExpandExec(
      expand: ExpandExec,
      partialAggregate: HashAggregateExec,
      child: SparkPlan,
      attributesToReplace: Map[Attribute, Attribute]): SparkPlan = {
    val newExpandProjectionTemplate =
      partialAggregate.output
        .map(e => getReplaceAttribute(e, attributesToReplace))
    logError(s"xxx newExpandProjectionTemplate: $newExpandProjectionTemplate")

    val newExpandProjections = buildNewExpandProjections(
      partialAggregate.groupingExpressions,
      expand.projections,
      expand.output,
      newExpandProjectionTemplate
    )
    logError(s"xxx newExpandProjections: $newExpandProjections\nprojections:${expand.projections}")
    ExpandExec(newExpandProjections, partialAggregate.output, child)
  }

}
