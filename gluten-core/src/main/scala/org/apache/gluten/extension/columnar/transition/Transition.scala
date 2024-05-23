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

import scala.collection.mutable

/**
 * Transition is a simple function to convert a query plan to interested [[ConventionReq]].
 *
 * Transitions can be registered through the utility APIs in
 * [[org.apache.gluten.extension.columnar.transition.Convention.BatchType]]'s definition.
 */
trait Transition {
  final def apply(plan: SparkPlan): SparkPlan = {
    val out = apply0(plan)
    if (out eq plan) {
      assert(
        this == Transition.empty,
        "TransitionDef.empty / Transition.empty should be used when defining an empty transition.")
    }
    out
  }

  final def isEmpty: Boolean = {
    this == Transition.empty
  }

  protected def apply0(plan: SparkPlan): SparkPlan
}

trait TransitionDef {
  def create(): Transition
}

object TransitionDef {
  val empty: TransitionDef = () => Transition.empty
}

object Transition {
  val empty: Transition = (plan: SparkPlan) => plan
  val factory: Factory = Factory.newBuiltin()

  def notFound(plan: SparkPlan): GlutenException = {
    new GlutenException(s"No viable transition found from plan's child to itself: $plan")
  }

  def notFound(plan: SparkPlan, required: ConventionReq): GlutenException = {
    new GlutenException(s"No viable transition to [$required] found for plan: $plan")
  }

  private class ChainedTransition(first: Transition, second: Transition) extends Transition {
    override def apply0(plan: SparkPlan): SparkPlan = {
      second(first(plan))
    }
  }

  private def chain(first: Transition, second: Transition): Transition = {
    if (first.isEmpty && second.isEmpty) {
      return Transition.empty
    }
    new ChainedTransition(first, second)
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
      val none = new Transition {
        override protected def apply0(plan: SparkPlan): SparkPlan =
          throw new UnsupportedOperationException()
      }
      val transition = findTransition(conv, req)(none)
      transition.isEmpty
    }

    protected def findTransition(from: Convention, to: ConventionReq)(
        orElse: => Transition): Transition
    private[transition] def update(): MutableFactory
  }

  trait MutableFactory extends Factory {
    def defineFromRowTransition(to: Convention.BatchType, transitionDef: TransitionDef): Unit
    def defineToRowTransition(from: Convention.BatchType, transitionDef: TransitionDef): Unit
    def defineBatchTransition(
        from: Convention.BatchType,
        to: Convention.BatchType,
        transitionDef: TransitionDef): Unit
  }

  private object Factory {
    def newBuiltin(): Factory = {
      new BuiltinFactory
    }

    private class BuiltinFactory extends MutableFactory {
      private val fromRowTransitions: mutable.Map[Convention.BatchType, TransitionDef] =
        mutable.Map()
      private val toRowTransitions: mutable.Map[Convention.BatchType, TransitionDef] = mutable.Map()
      private val batchTransitions
          : mutable.Map[(Convention.BatchType, Convention.BatchType), TransitionDef] =
        mutable.Map()

      override def defineFromRowTransition(
          to: Convention.BatchType,
          transitionDef: TransitionDef): Unit = {
        assert(!fromRowTransitions.contains(to))
        fromRowTransitions += to -> transitionDef
      }

      override def defineToRowTransition(
          from: Convention.BatchType,
          transitionDef: TransitionDef): Unit = {
        assert(!toRowTransitions.contains(from))
        toRowTransitions += from -> transitionDef
      }

      override def defineBatchTransition(
          from: Convention.BatchType,
          to: Convention.BatchType,
          transitionDef: TransitionDef): Unit = {
        assert(!batchTransitions.contains((from, to)))
        batchTransitions += (from, to) -> transitionDef
      }

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
                toRowTransitions.get(from.batchType).map(_.create()).getOrElse(orElse)
              case fromRowType =>
                // We have only one single built-in row type.
                assert(toRowType == fromRowType)
                Transition.empty
            }
          case (ConventionReq.RowType.Any, ConventionReq.BatchType.Is(toBatchType)) =>
            from.batchType match {
              case Convention.BatchType.None =>
                fromRowTransitions.get(toBatchType).map(_.create()).getOrElse(orElse)
              case fromBatchType =>
                if (toBatchType == fromBatchType) {
                  Transition.empty
                } else {
                  // Batch type conversion needed.
                  //
                  // We first look up for batch-to-batch transition. If found one, return that
                  // transition to caller. Otherwise, look for from/to row transitions, then
                  // return a bridged batch-to-row-to-batch transition.
                  if (batchTransitions.contains((fromBatchType, toBatchType))) {
                    // 1. Found batch-to-batch transition.
                    batchTransitions((fromBatchType, toBatchType)).create()
                  } else {
                    // 2. Otherwise, build up batch-to-row-to-batch transition.
                    val batchToRow =
                      toRowTransitions.get(fromBatchType).map(_.create()).getOrElse(orElse)
                    val rowToBatch =
                      fromRowTransitions.get(toBatchType).map(_.create()).getOrElse(orElse)
                    chain(batchToRow, rowToBatch)
                  }
                }
            }
          case (ConventionReq.RowType.Any, ConventionReq.BatchType.Any) =>
            Transition.empty
          case _ =>
            throw new UnsupportedOperationException(
              s"Illegal convention requirement: $ConventionReq")
        }
        out
      }

      override private[transition] def update(): MutableFactory = this
    }
  }
}
