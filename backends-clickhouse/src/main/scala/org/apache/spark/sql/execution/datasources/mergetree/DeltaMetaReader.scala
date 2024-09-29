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

class DeltaMetaReader(
    override val metadata: Metadata,
    override val configuration: Map[String, String])
  extends TablePropertiesReader {

  def storageDB: String = configuration(StorageMeta.STORAGE_DB)
  def storageTable: String = configuration(StorageMeta.STORAGE_TABLE)
  def storageSnapshotId: String = configuration(StorageMeta.STORAGE_SNAPSHOT_ID)
  def storagePath: String = configuration(StorageMeta.STORAGE_PATH)
}

object DeltaMetaReader {
  def apply(metadata: Metadata): DeltaMetaReader = {
    new DeltaMetaReader(metadata, metadata.configuration)
  }
}
