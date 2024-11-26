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
package org.apache.gluten.extension.columnar.transition

import org.apache.gluten.exception.GlutenException

import org.apache.spark.sql.execution.SparkPlan

/**
 * Transition is a simple function to convert a query plan to interested [[ConventionReq]].
 *
 * Transitions can be registered through the utility APIs in
 * [[org.apache.gluten.extension.columnar.transition.Convention.BatchType]]'s definition.
 */
trait Transition {
  final def apply(plan: SparkPlan): SparkPlan = {
    val out = apply0(plan)
    out
  }

  final lazy val isEmpty: Boolean = {
    // Tests if a transition is actually no-op.
    val plan = DummySparkPlan()
    val out = apply0(plan)
    val identical = out eq plan
    identical
  }

  protected def apply0(plan: SparkPlan): SparkPlan
}

object Transition {
  val empty: Transition = (plan: SparkPlan) => plan
  private val abort: Transition = (_: SparkPlan) => throw new UnsupportedOperationException("Abort")
  private[transition] val graph: TransitionGraph.Builder = TransitionGraph.builder()

  def factory(): Factory = Factory.newBuiltin(graph.build())

  def notFound(plan: SparkPlan): GlutenException = {
    new GlutenException(s"No viable transition found from plan's child to itself: $plan")
  }

  def notFound(plan: SparkPlan, required: ConventionReq): GlutenException = {
    new GlutenException(s"No viable transition to [$required] found for plan: $plan")
  }

  trait Factory {
    final def findTransition(
        from: Convention,
        to: ConventionReq,
        otherwise: Exception): Transition = {
      findTransition(from, to) {
        throw otherwise
      }
    }

    final def satisfies(conv: Convention, req: ConventionReq): Boolean = {
      val transition = findTransition(conv, req)(abort)
      transition.isEmpty
    }

    protected def findTransition(from: Convention, to: ConventionReq)(
        orElse: => Transition): Transition
  }

  private object Factory {
    def newBuiltin(graph: TransitionGraph): Factory = {
      new BuiltinFactory(graph)
    }

    private class BuiltinFactory(graph: TransitionGraph) extends Factory {
      override def findTransition(from: Convention, to: ConventionReq)(
          orElse: => Transition): Transition = {
        assert(
          !from.isNone,
          "#findTransition called with on a plan that doesn't support either row or columnar " +
            "output")
        val out = (to.requiredRowType, to.requiredBatchType) match {
          case (ConventionReq.RowType.Is(toRowType), ConventionReq.BatchType.Is(toBatchType)) =>
            if (from.rowType == toRowType && from.batchType == toBatchType) {
              return Transition.empty
            } else {
              throw new UnsupportedOperationException(
                "Transiting to plan that both have row and columnar-batch output is not yet " +
                  "supported")
            }
          case (ConventionReq.RowType.Is(toRowType), ConventionReq.BatchType.Any) =>
            from.rowType match {
              case Convention.RowType.None =>
                // Input query plan doesn't have recognizable row-based output,
                // find columnar-to-row transition.
                graph.transitionOfOption(from.batchType, toRowType).getOrElse(orElse)
              case fromRowType if toRowType == fromRowType =>
                // We have only one single built-in row type.
                Transition.empty
              case _ =>
                throw new UnsupportedOperationException(
                  "Row-to-row transition is not yet supported")
            }
          case (ConventionReq.RowType.Any, ConventionReq.BatchType.Is(toBatchType)) =>
            from.batchType match {
              case Convention.BatchType.None =>
                // Input query plan doesn't have recognizable columnar output,
                // find row-to-columnar transition.
                graph.transitionOfOption(from.rowType, toBatchType).getOrElse(orElse)
              case fromBatchType if toBatchType == fromBatchType =>
                Transition.empty
              case fromBatchType =>
                // Find columnar-to-columnar transition.
                graph.transitionOfOption(fromBatchType, toBatchType).getOrElse(orElse)
            }
          case (ConventionReq.RowType.Any, ConventionReq.BatchType.Any) =>
            Transition.empty
          case _ =>
            throw new UnsupportedOperationException(
              s"Illegal convention requirement: $ConventionReq")
        }
        out
      }
    }
  }
}
