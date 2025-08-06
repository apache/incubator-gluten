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

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.Path

import java.net.URI

case class MergeTreePartRange(
    name: String,
    dirName: String,
    targetNode: String,
    bucketNum: String,
    start: Long,
    marks: Long,
    size: Long,
    rowIndexFilterType: String,
    rowIndexFilterIdEncoded: String) {
  override def toString: String = {
    s"part name: $name, range: $start-${start + marks}"
  }
}

case class MergeTreePartSplit private (
    name: String,
    dirName: String,
    targetNode: String,
    start: Long,
    length: Long,
    bytesOnDisk: Long,
    rowIndexFilterType: String,
    rowIndexFilterIdEncoded: String) {
  override def toString: String = {
    s"part name: $name, range: $start-${start + length}"
  }
}

object MergeTreePartSplit {
  def apply(
      name: String,
      dirName: String,
      targetNode: String,
      start: Long,
      length: Long,
      bytesOnDisk: Long,
      rowIndexFilterType: String,
      rowIndexFilterIdEncoded: String
  ): MergeTreePartSplit = {
    // Ref to org.apache.spark.sql.delta.files.TahoeFileIndex.absolutePath
    val uriDecodeName = new Path(new URI(name)).toString
    val uriDecodeDirName = new Path(new URI(dirName)).toString
    new MergeTreePartSplit(
      uriDecodeName,
      uriDecodeDirName,
      targetNode,
      start,
      length,
      bytesOnDisk,
      rowIndexFilterType,
      rowIndexFilterIdEncoded)
  }
}

case class GlutenMergeTreePartition(
    index: Int,
    engine: String,
    database: String,
    table: String,
    snapshotId: String,
    relativeTablePath: String,
    absoluteTablePath: String,
    orderByKey: String,
    lowCardKey: String,
    minmaxIndexKey: String,
    bfIndexKey: String,
    setIndexKey: String,
    primaryKey: String,
    partList: Array[MergeTreePartSplit],
    tableSchema: StructType,
    clickhouseTableConfigs: Map[String, String])
  extends InputPartition {
  override def preferredLocations(): Array[String] = {
    Array.empty[String]
  }
}
