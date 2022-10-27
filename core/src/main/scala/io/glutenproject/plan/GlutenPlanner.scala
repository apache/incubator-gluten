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
package io.glutenproject.plan

import io.substrait.spark.expression.FunctionMappings

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, BroadcastExchangeLike}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.types._

import java.util.concurrent.atomic.AtomicInteger

/** Given a [[SparkPlan]], returns a [[GlutenPlan]] SparkPlan if possible */
class GlutenPlanner {

  /**
   * Visit [[SparkPlan]] `s` where `rule` has been recursively applied first to all of its children
   * and then itself (post-order). When `rule` does not apply to a given node, it returns a
   * [[PlaceHolder]].
   *
   * @param rule
   *   the function used to transform this nodes children
   */
  private def transformUp(s: SparkPlan)(
      rule: PartialFunction[(SparkPlan, Seq[SparkPlan]), GlutenPlan]): GlutenPlan = {
    val children = s.children.map(c => transformUp(c)(rule)).toList
    if (glutenSupport(s)) {
      rule.applyOrElse((s, children), placeHolder)
    } else {
      placeHolder((s, children))
    }
  }

  private def placeHolder(tuple: (SparkPlan, Seq[SparkPlan])): GlutenPlan =
    PlaceHolder(tuple._1, tuple._2)

  def plan(plan: SparkPlan): GlutenPlan = transformUp(plan) {
    case (f: FileSourceScanExec, _) => GlutenFileScan(f)
    case (f: FilterExec, (child: GlutenPlan) :: Nil) => GlutenFilterExec(f, child)
    case (b: BroadcastExchangeExec, (child: GlutenPlan) :: Nil) =>
      GlutenBroadcastExchangeExec(b.mode, child)
    case (b: BroadcastHashJoinExec, (left: GlutenPlan) :: (right: GlutenPlan) :: Nil) =>
      GlutenBroadcastHashJoinExec(b, left, right)
  }

  // def plan(s: SparkPlan): GlutenPlan = visit(s)

  private def supportType(d: DataType): Boolean = d match {
    case BooleanType => true
    case ByteType => true
    case ShortType => true
    case IntegerType => true
    case LongType => true
    case FloatType => true
    case DoubleType => true
    case _ => false
  }

  /** This is a litter trick, since we can return true at parent node */
  def fastCheckSupport(e: Expression): Boolean = supportType(e.dataType) && {
    e match {
      case _: AttributeReference => true
      case _: Alias => true
      case a: AggregateExpression =>
        FunctionMappings.aggregate_functions_map.contains(a.aggregateFunction.getClass)
      case u: UnaryExpression => FunctionMappings.scalar_functions_map.contains(u.getClass)
      case b: BinaryOperator => FunctionMappings.scalar_functions_map.contains(b.getClass)
      case _ => false
    }
  }

  private def glutenSupport(s: SparkPlan) = {
    // TODO: spark 3.3 using exists instead of find
    val willFallback = s.expressions.exists(_.find(e => !fastCheckSupport(e)).isDefined)
    !willFallback
  }
}

class ReplacePlaceHolder(codegenStageCounter: AtomicInteger = new AtomicInteger(0)) {
  private def findExchange(s: SparkPlan): SparkPlan = s match {
    case gluten: BroadcastExchangeLike =>
      gluten.withNewChildren(Seq(insertWholeStage(gluten.child)))
    case other => other.withNewChildren(other.children.map(findExchange))
  }
  private def insertWholeStage(g: SparkPlan): SparkPlan = g match {
    case PlaceHolder(jvmPlan, children) =>
      jvmPlan.withNewChildren(children.map(insertWholeStage))
    case substrait if substrait.isInstanceOf[SubstraitSupport[_]] =>
      GlutenWholeStage(substrait.withNewChildren(substrait.children.map(findExchange)))(
        codegenStageCounter.incrementAndGet())
  }
}

object ReplacePlaceHolder {
  def apply(gluten: GlutenPlan): SparkPlan = {
    new ReplacePlaceHolder().insertWholeStage(gluten)
  }
}
