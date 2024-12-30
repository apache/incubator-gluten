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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.VeloxBloomFilterMightContain
import org.apache.gluten.expression.aggregate.VeloxBloomFilterAggregate
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

case class BloomFilterMightContainJointRewriteRule(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableNativeBloomFilter) {
      return plan
    }
    val out = plan.transformWithSubqueries {
      case p =>
        applyForNode(p)
    }
    out
  }

  private def applyForNode(p: SparkPlan) = {
    p.transformExpressions {
      case e =>
        SparkShimLoader.getSparkShims.replaceMightContain(
          SparkShimLoader.getSparkShims
            .replaceBloomFilterAggregate(e, VeloxBloomFilterAggregate.apply),
          VeloxBloomFilterMightContain.apply)
    }
  }
}
