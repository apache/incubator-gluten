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

package org.apache.spark.sql.execution

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Base class for operators that exchange data among multiple threads or processes.
 *
 * ColumnarExchanges are the key class of operators that enable parallelism. Although the implementation
 * differs significantly, the concept is similar to the exchange operator described in
 * "Volcano -- An Extensible and Parallel Query Evaluation System" by Goetz Graefe.

abstract class ColumnarExchange extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[id=#$id]")
}
 */
/**
 * Find out duplicated exchanges in the spark plan, then use the same exchange for all the
 * references.
 */
case class ReuseColumnarExchange() extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    def exchangeReuseEnabled = true //TODO(): allow to config
      
    if (!exchangeReuseEnabled) {
      return plan
    }
    // Build a hash map using schema of exchanges to avoid O(N*N) sameResult calls.
    val exchanges = mutable.HashMap[StructType, ArrayBuffer[Exchange]]()

    // Replace a ColumnarExchange duplicate with a ReusedColumnarExchange
    def reuse: PartialFunction[Exchange, SparkPlan] = {
      case exchange: Exchange =>
        val sameSchema =
          exchanges.getOrElseUpdate(exchange.schema, ArrayBuffer[Exchange]())
        val samePlan = sameSchema.find { e => exchange.sameResult(e) }
        if (samePlan.isDefined) {
          // Keep the output of this exchange, the following plans require that to resolve
          // attributes.
          exchange match {
            case b: BroadcastExchangeExec =>
              System.out.println(s"Reused ${samePlan.get}")
            case s: ColumnarShuffleExchangeExec =>
              System.out.println(s"Reused ${samePlan.get}")
            case other =>
          }
          ReusedExchangeExec(exchange.output, samePlan.get)
        } else {
          sameSchema += exchange
          exchange
        }
    }

    plan transformUp {
      case exchange: Exchange => reuse(exchange)
    } transformAllExpressions {
      // Lookup inside subqueries for duplicate exchanges
      case in: InSubqueryExec =>
        val newIn = in.plan.transformUp {
          case exchange: Exchange => reuse(exchange)
        }
        in.copy(plan = newIn.asInstanceOf[BaseSubqueryExec])
    }
  }
}
case class ReuseColumnarSubquery() extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    def exchangeReuseEnabled = true //TODO(): allow to config
    if (!exchangeReuseEnabled) {
      return plan
    }
    // Build a hash map using schema of subqueries to avoid O(N*N) sameResult calls.
    val subqueries = mutable.HashMap[StructType, ArrayBuffer[BaseSubqueryExec]]()
    plan transformAllExpressions {
      case sub: ExecSubqueryExpression =>
        val sameSchema =
          subqueries.getOrElseUpdate(sub.plan.schema, ArrayBuffer[BaseSubqueryExec]())
        val sameResult = sameSchema.find(_.sameResult(sub.plan))
        if (sameResult.isDefined) {
          System.out.println(s"reused subquery ${sameResult.get}")
          sub.withNewPlan(ReusedSubqueryExec(sameResult.get))
        } else {
          sameSchema += sub.plan
          sub
        }
    }
  }
}
