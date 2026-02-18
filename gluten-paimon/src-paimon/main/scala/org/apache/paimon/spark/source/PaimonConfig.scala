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
package org.apache.paimon.spark.source

import org.apache.spark.sql.internal.SQLConf.buildConf

object PaimonConfig {

  val PAIMON_NATIVE_SOURCE_ENABLED =
    buildConf("spark.gluten.paimon.native.source.enabled")
      .doc("When true, enable the paimon native source.")
      .internal()
      .booleanConf
      .createWithDefault(true)

  val PAIMON_NATIVE_MOR_ENABLED =
    buildConf("spark.gluten.paimon.native.mor.source.enabled")
      .doc("When true, enables the paimon mor source.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val PAIMON_NATIVE_MOR_AGGREGATE_ENGINE_ENABLED =
    buildConf("spark.gluten.paimon.native.mor.aggregate.engine.enabled")
      .doc("When true, enables the paimon mor aggregate engine.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val PAIMON_NATIVE_MOR_PARTIAL_UPDATE_ENGINE_ENABLED =
    buildConf("spark.gluten.paimon.native.mor.partial.update.engine.enabled")
      .doc("When true, enables the paimon mor partial update engine.")
      .internal()
      .booleanConf
      .createWithDefault(false)

  val PAIMON_NATIVE_SPLIT_ENABLED =
    buildConf("spark.gluten.paimon.native.split.enabled")
      .doc("When true, enables the paimon split creation.")
      .internal()
      .booleanConf
      .createWithDefault(true)

}
