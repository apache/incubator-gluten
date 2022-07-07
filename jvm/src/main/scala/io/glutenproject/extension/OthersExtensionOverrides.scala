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

import io.glutenproject.{GlutenConfig, GlutenSparkExtensionsInjector}
import io.glutenproject.backendsapi.BackendsApiManager

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSessionExtensions

object OthersExtensionOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    // Spark extension rules for ClickHouse backend.
    if (SparkEnv.get.conf.get(GlutenConfig.GLUTEN_BACKEND_LIB)
      .equalsIgnoreCase(GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)) {
      extensions.injectResolutionRule { session =>
        BackendsApiManager.getSparkPlanExecApiInstance
          .genExtendedAnalyzer(session, session.sessionState.conf)
      }
      extensions.injectPlannerStrategy { spark =>
        BackendsApiManager.getSparkPlanExecApiInstance.genExtendedDataSourceV2Strategy(spark)
      }
    }
    if (SparkEnv.get.conf.get(GlutenConfig.GLUTEN_BACKEND_LIB)
      .equalsIgnoreCase(GlutenConfig.GLUTEN_VELOX_BACKEND)) {
      extensions.injectColumnar { session =>
        BackendsApiManager.getSparkPlanExecApiInstance.genExtendedRule(session)
      }
      extensions.injectPlannerStrategy { session =>
        BackendsApiManager.getSparkPlanExecApiInstance.genExtendedStrategy()
      }
    }
  }
}
