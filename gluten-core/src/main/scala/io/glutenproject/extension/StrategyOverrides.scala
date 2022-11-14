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

package io.glutenproject.extension

import io.glutenproject.{BackendLib, GlutenConfig, GlutenSparkExtensionsInjector}
import io.glutenproject.sql.shims.SparkShimLoader

import org.apache.spark.sql.{SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

object JoinSelectionOverrides extends Strategy with JoinSelectionHelper with SQLConfHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    SparkShimLoader.getSparkShims.applyPlan(plan,
      GlutenConfig.getSessionConf.forceShuffledHashJoin,
      BackendLib.valueOf(GlutenConfig.getConf.glutenBackendLib.toUpperCase()))
  }
}

object StrategyOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => JoinSelectionOverrides)
  }
}
