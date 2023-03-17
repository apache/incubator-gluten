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

import io.glutenproject.execution.{TransformSupport}
import io.glutenproject.extension.ColumnarOverrideRules
import io.glutenproject.metrics.MetricsUpdater
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.vectorized.ColumnarBatch

class FallbackStrategiesSuite extends GlutenSQLTestsTrait {

  test("Fall back the whole plan if meeting the configured threshold") {
    withSQLConf(("spark.gluten.sql.columnar.wholeStage.fallback.threshold", "3")) {
      val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      rule.enableAdaptiveContext
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer.
      val planAfterPreOverride =
        UnaryOp2(UnaryOp1Transformer(UnaryOp2(UnaryOp1Transformer(LeafOp()))))
      val planWithTransition = rule.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to fall back the entire plan.
      assert(outputPlan == originalPlan)
    }
  }

  test("Don't fall back the whole plan if NOT meeting the configured threshold") {
    withSQLConf(("spark.gluten.sql.columnar.wholeStage.fallback.threshold", "4")) {
      val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      rule.enableAdaptiveContext
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer.
      val planAfterPreOverride =
        UnaryOp2(UnaryOp1Transformer(UnaryOp2(UnaryOp1Transformer(LeafOp()))))
      val planWithTransition = rule.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to get the plan with columnar rule applied.
      assert(outputPlan != originalPlan)
    }
  }

  test("Fall back the whole plan if meeting the configured threshold (leaf node is" +
      " transformable)") {
    withSQLConf(("spark.gluten.sql.columnar.wholeStage.fallback.threshold", "2")) {
      val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      rule.enableAdaptiveContext
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer
      // and replacing LeafOp with LeafOpTransformer.
      val planAfterPreOverride =
        UnaryOp2(UnaryOp1Transformer(UnaryOp2(UnaryOp1Transformer(LeafOpTransformer()))))
      val planWithTransition = rule.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to fall back the entire plan.
      assert(outputPlan == originalPlan)
    }
  }

  test("Don't Fall back the whole plan if NOT meeting the configured threshold (" +
      "leaf node is transformable)") {
    withSQLConf(("spark.gluten.sql.columnar.wholeStage.fallback.threshold", "3")) {
      val originalPlan = UnaryOp2(UnaryOp1(UnaryOp2(UnaryOp1(LeafOp()))))
      val rule = ColumnarOverrideRules(spark)
      rule.preColumnarTransitions(originalPlan)
      rule.enableAdaptiveContext
      // Fake output of preColumnarTransitions, mocking replacing UnaryOp1 with UnaryOp1Transformer
      // and replacing LeafOp with LeafOpTransformer.
      val planAfterPreOverride =
      UnaryOp2(UnaryOp1Transformer(UnaryOp2(UnaryOp1Transformer(LeafOpTransformer()))))
      val planWithTransition = rule.insertTransitions(planAfterPreOverride, false)
      val outputPlan = rule.postColumnarTransitions(planWithTransition)
      // Expect to get the plan with columnar rule applied.
      assert(outputPlan != originalPlan)
    }
  }

}

case class LeafOp(override val supportsColumnar: Boolean = false) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = Seq.empty
}

case class UnaryOp1(child: SparkPlan, override val supportsColumnar: Boolean = false)
    extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: SparkPlan): UnaryOp1 =
    copy(child = newChild)
}

case class UnaryOp2(child: SparkPlan, override val supportsColumnar: Boolean = false)
    extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: SparkPlan): UnaryOp2 =
    copy(child = newChild)
}

// For replacing LeafOp.
case class LeafOpTransformer(override val supportsColumnar: Boolean = true) extends LeafExecNode
    with TransformSupport {
  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] =
    throw new UnsupportedOperationException()
  override def getStreamedLeafPlan: SparkPlan = throw new UnsupportedOperationException()
  override def getChild: SparkPlan = throw new UnsupportedOperationException()
  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] =
    throw new UnsupportedOperationException()
  override def metricsUpdater(): MetricsUpdater = throw new UnsupportedOperationException()
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = Seq.empty
}

// For replacing UnaryOp1.
case class UnaryOp1Transformer(override val child: SparkPlan,
                               override val supportsColumnar: Boolean = true)
    extends UnaryExecNode with TransformSupport {
  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] =
    throw new UnsupportedOperationException()
  override def getStreamedLeafPlan: SparkPlan = throw new UnsupportedOperationException()
  override def getChild: SparkPlan = child
  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] =
    throw new UnsupportedOperationException()
  override def metricsUpdater(): MetricsUpdater = throw new UnsupportedOperationException()
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: SparkPlan): UnaryOp1Transformer =
    copy(child = newChild)
}
