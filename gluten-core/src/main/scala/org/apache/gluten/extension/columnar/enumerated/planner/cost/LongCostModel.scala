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
package org.apache.gluten.extension.columnar.enumerated.planner.cost

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.extension.columnar.enumerated.planner.plan.GlutenPlanModel.GroupLeafExec
import org.apache.gluten.ras.{Cost, CostModel}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

abstract class LongCostModel extends CostModel[SparkPlan] {
  private val infLongCost = Long.MaxValue

  override def costOf(node: SparkPlan): LongCost = node match {
    case _: GroupLeafExec => throw new IllegalStateException()
    case _ => LongCost(longCostOf(node))
  }

  private def longCostOf(node: SparkPlan): Long = node match {
    case n =>
      val selfCost = selfLongCostOf(n)

      // Sum with ceil to avoid overflow.
      def safeSum(a: Long, b: Long): Long = {
        assert(a >= 0)
        assert(b >= 0)
        val sum = a + b
        if (sum < a || sum < b) Long.MaxValue else sum
      }

      (n.children.map(longCostOf).toList :+ selfCost).reduce(safeSum)
  }

  def selfLongCostOf(node: SparkPlan): Long

  override def costComparator(): Ordering[Cost] = Ordering.Long.on {
    case LongCost(value) => value
    case _ => throw new IllegalStateException("Unexpected cost type")
  }

  override def makeInfCost(): Cost = LongCost(infLongCost)
}

object LongCostModel extends Logging {
  def registry(): Registry = {
    new Registry()
  }

  /**
   * Kind of a cost model. Output of #name() will be used as alias to identify the cost model
   * instance from the registry.
   */
  sealed trait Kind {
    import Kind._
    all.synchronized {
      val n = name()
      if (all.contains(n)) {
        throw new GlutenException(s"Cost mode kind $n already registered")
      }
      all += n -> this
    }

    def name(): String
  }

  object Kind {
    private val all: mutable.Map[String, Kind] = mutable.Map()
    def values(): Map[String, Kind] = all.toMap
  }

  /**
   * A cost model that is supposed to drive RAS planner create the same query plan with legacy
   * planner.
   */
  case object Legacy extends Kind {
    override def name(): String = "legacy"
  }

  /** A rough cost model with some empirical heuristics. */
  case object Rough extends Kind {
    override def name(): String = "rough"
  }

  /** Compared with rough, rough2 can be more precise to avoid the costly r2c. */
  case object Rough2 extends Kind {
    override def name(): String = "rough2"
  }

  class Registry private[LongCostModel] {
    private val lookup: mutable.Map[Kind, LongCosterChain.Builder] = mutable.Map()

    // The registered coster will take lower precedence than all the existing
    // registered costers in cost estimation.
    def register(coster: LongCoster): Unit = synchronized {
      val chainBuilder = builderOf(coster.kind())
      chainBuilder.register(coster)
    }

    def get(kind: Kind): LongCostModel = synchronized {
      builderOf(kind).build()
    }

    private def builderOf(kind: Kind): LongCosterChain.Builder = {
      lookup.getOrElseUpdate(kind, LongCosterChain.builder())
    }
  }
}
