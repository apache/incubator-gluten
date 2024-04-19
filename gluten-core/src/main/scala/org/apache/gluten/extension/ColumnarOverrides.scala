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

import org.apache.gluten.{GlutenConfig, GlutenSparkExtensionsInjector}
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.enumerated.EnumeratedApplier
import org.apache.gluten.extension.columnar.heuristic.HeuristicApplier
import org.apache.gluten.utils.LogLevelUtil

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

object ColumnarOverrideRules {

  // Utilities to infer columnar rule's caller's property:
  // ApplyColumnarRulesAndInsertTransitions#outputsColumnar.

  case class DummyRowOutputExec(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = false
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }

  case class DummyColumnarOutputExec(override val child: SparkPlan) extends UnaryExecNode {
    override def supportsColumnar: Boolean = true
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
    override def doExecuteBroadcast[T](): Broadcast[T] =
      throw new UnsupportedOperationException()
    override def output: Seq[Attribute] = child.output
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }

  object OutputsColumnarTester {
    def wrap(plan: SparkPlan): SparkPlan = {
      if (plan.supportsColumnar) {
        DummyColumnarOutputExec(plan)
      } else {
        DummyRowOutputExec(plan)
      }
    }

    def inferOutputsColumnar(plan: SparkPlan): Boolean = plan match {
      case DummyRowOutputExec(_) => false
      case RowToColumnarExec(DummyRowOutputExec(_)) => true
      case DummyColumnarOutputExec(_) => true
      case ColumnarToRowExec(DummyColumnarOutputExec(_)) => false
      case _ =>
        throw new IllegalStateException(
          "This should not happen. Please leave a issue at" +
            " https://github.com/apache/incubator-gluten.")
    }

    def unwrap(plan: SparkPlan): SparkPlan = plan match {
      case DummyRowOutputExec(child) => child
      case RowToColumnarExec(DummyRowOutputExec(child)) => child
      case DummyColumnarOutputExec(child) => child
      case ColumnarToRowExec(DummyColumnarOutputExec(child)) => child
      case _ =>
        throw new IllegalStateException(
          "This should not happen. Please leave a issue at" +
            " https://github.com/apache/incubator-gluten.")
    }
  }
}

case class ColumnarOverrideRules(session: SparkSession)
  extends ColumnarRule
  with Logging
  with LogLevelUtil {

  import ColumnarOverrideRules._

  /**
   * Note: Do not implement this API. We basically inject all of Gluten's physical rules through
   * `postColumnarTransitions`.
   *
   * See: https://github.com/oap-project/gluten/pull/4790
   */
  final override def preColumnarTransitions: Rule[SparkPlan] = plan => {
    // To infer caller's property: ApplyColumnarRulesAndInsertTransitions#outputsColumnar.
    OutputsColumnarTester.wrap(plan)
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    val outputsColumnar = OutputsColumnarTester.inferOutputsColumnar(plan)
    val unwrapped = OutputsColumnarTester.unwrap(plan)
    val vanillaPlan = ColumnarTransitions.insertTransitions(unwrapped, outputsColumnar)
    val applier: ColumnarRuleApplier = if (GlutenConfig.getConf.enableRas) {
      new EnumeratedApplier(session)
    } else {
      new HeuristicApplier(session)
    }
    val out = applier.apply(vanillaPlan, outputsColumnar)
    out
  }

}

object ColumnarOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(spark => ColumnarOverrideRules(spark))
  }
}
