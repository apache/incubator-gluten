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

import org.apache.gluten.expression.ConverterUtils.normalizeColName

import org.apache.spark.sql.delta.actions.Metadata

import org.apache.hadoop.conf.Configuration

case class DeltaMetaReader(metadata: Metadata) extends TablePropertiesReader {

  def storageDB: String = configuration(StorageMeta.DB)
  def storageTable: String = configuration(StorageMeta.TABLE)
  def storageSnapshotId: String = configuration(StorageMeta.SNAPSHOT_ID)
  def storagePath: String = configuration(StorageMeta.STORAGE_PATH)

  def updateToHadoopConf(conf: Configuration): Unit = {
    conf.set(StorageMeta.DB, storageDB)
    conf.set(StorageMeta.TABLE, storageTable)
    conf.set(StorageMeta.SNAPSHOT_ID, storageSnapshotId)
    writeConfiguration.foreach { case (k, v) => conf.set(k, v) }
  }

  override lazy val partitionColumns: Seq[String] =
    metadata.partitionColumns.map(normalizeColName)

  override lazy val configuration: Map[String, String] = metadata.configuration
}
