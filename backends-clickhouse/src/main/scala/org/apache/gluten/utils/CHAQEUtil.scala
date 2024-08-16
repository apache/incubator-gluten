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
package org.apache.gluten.execution

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._

object CHAQEUtil {

  // All TransformSupports have lost the logicalLink. So we need iterate the plan to find the
  // first ShuffleQueryStageExec and get the runtime stats.
  def getShuffleQueryStageStats(plan: SparkPlan): Option[Statistics] = {
    plan match {
      case stage: ShuffleQueryStageExec =>
        Some(stage.getRuntimeStatistics)
      case _ =>
        if (plan.children.length == 1) {
          getShuffleQueryStageStats(plan.children.head)
        } else {
          None
        }
    }
  }
}
