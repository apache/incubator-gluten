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
package org.apache.gluten.config

/**
 * Gluten Static configuration is a cross-session, immutable Spark configuration. External users can
 * see the static sql configs via `SparkSession.conf`, but can NOT set/unset them.
 */
object GlutenStaticConfig {
  import GlutenConfig.buildStaticConf

  val GLUTEN_UI_ENABLED = buildStaticConf("spark.gluten.ui.enabled")
    .doc(
      "Whether to enable the gluten web UI, If true, attach the gluten UI page " +
        "to the Spark web UI.")
    .booleanConf
    .createWithDefault(true)

  val VANILLA_VECTORIZED_READERS_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.enableVanillaVectorizedReaders")
      .internal()
      .doc("Enable or disable vanilla vectorized scan.")
      .booleanConf
      .createWithDefault(true)

  val DEBUG_KEEP_JNI_WORKSPACE =
    buildStaticConf("spark.gluten.sql.debug.keepJniWorkspace")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val DEBUG_KEEP_JNI_WORKSPACE_DIR =
    buildStaticConf("spark.gluten.sql.debug.keepJniWorkspaceDir")
      .internal()
      .stringConf
      .createWithDefault("/tmp")

  val UT_STATISTIC =
    buildStaticConf("spark.gluten.sql.ut.statistic")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val COST_EVALUATOR_ENABLED =
    buildStaticConf("spark.gluten.sql.adaptive.costEvaluator.enabled")
      .internal()
      .doc(
        "If true, use " +
          "org.apache.spark.sql.execution.adaptive.GlutenCostEvaluator as custom cost " +
          "evaluator class, else follow the configuration " +
          "spark.sql.adaptive.customCostEvaluatorClass.")
      .booleanConf
      .createWithDefault(true)

  val CELEBORN_FALLBACK_ENABLED =
    buildStaticConf("spark.gluten.sql.columnar.shuffle.celeborn.fallback.enabled")
      .internal()
      .doc("If enabled, fall back to ColumnarShuffleManager when celeborn service is unavailable." +
        "Otherwise, throw an exception.")
      .booleanConf
      .createWithDefault(true)

  val HDFS_VIEWFS_ENABLED =
    buildStaticConf("spark.gluten.storage.hdfsViewfs.enabled")
      .internal()
      .doc("If enabled, gluten will convert the viewfs path to hdfs path in scala side")
      .booleanConf
      .createWithDefault(false)
}
