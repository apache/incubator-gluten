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
package org.apache.spark.sql.execution.datasources.mergetree

import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.types.StructType

case class DeltaMetaReader(metadata: Metadata)
  extends TablePropertiesReader
  with StorageConfigProvider {

  override protected val rawPartitionColumns: Seq[String] = metadata.partitionColumns

  override val configuration: Map[String, String] = metadata.configuration

  override protected lazy val tableSchema: StructType = metadata.schema

  lazy val storageConf: Map[String, String] = {
    Map(
      StorageMeta.DB -> configuration(StorageMeta.DB),
      StorageMeta.TABLE -> configuration(StorageMeta.TABLE),
      StorageMeta.SNAPSHOT_ID -> configuration(StorageMeta.SNAPSHOT_ID),
      StorageMeta.POLICY -> configuration.getOrElse(StorageMeta.POLICY, "default"),
      StorageMeta.ORDER_BY_KEY -> orderByKey,
      StorageMeta.LOW_CARD_KEY -> lowCardKey,
      StorageMeta.MINMAX_INDEX_KEY -> minmaxIndexKey,
      StorageMeta.BF_INDEX_KEY -> bfIndexKey,
      StorageMeta.SET_INDEX_KEY -> setIndexKey,
      StorageMeta.PRIMARY_KEY -> primaryKey
    )
  }
}
