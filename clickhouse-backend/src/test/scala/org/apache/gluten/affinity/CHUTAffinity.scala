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
package org.apache.gluten.affinity

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.softaffinity.AffinityManager

import org.apache.spark.affinity.MixedAffinity

/** Get the locations by SoftAffinityManager */
object CHUTAffinity extends MixedAffinity(CHUTSoftAffinityManager) {
  override lazy val affinityMode: String = CHBackendSettings.affinityMode
}

object CHUTSoftAffinityManager extends AffinityManager {
  override lazy val usingSoftAffinity: Boolean = true

  override lazy val minOnTargetHosts: Int =
    GlutenConfig.GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS.defaultValue.get

  override lazy val detectDuplicateReading: Boolean = true

  override lazy val duplicateReadingMaxCacheItems: Int =
    GlutenConfig.GLUTEN_SOFT_AFFINITY_DUPLICATE_READING_MAX_CACHE_ITEMS.defaultValue.get
}
