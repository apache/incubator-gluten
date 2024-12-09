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
package org.apache.spark.sql.delta.util

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.StringType

import org.apache.hadoop.fs.Path

/**
 * `OptimizeTableCommandOverwrites` does not use `DelayedCommitProtocol`, so we can't use
 * `DelayedCommitProtocol.parsePartitions`. This is a copied version. </br> TODO: Remove it.
 */
object MergeTreePartitionUtils {

  private val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"

  def parsePartitions(dir: String): Map[String, String] = {
    // TODO: timezones?
    // TODO: enable validatePartitionColumns?
    val dateFormatter = DateFormatter()
    val timestampFormatter =
      TimestampFormatter(timestampPartitionPattern, java.util.TimeZone.getDefault)
    val parsedPartition =
      PartitionUtils
        .parsePartition(
          new Path(dir),
          typeInference = false,
          Set.empty,
          Map.empty,
          validatePartitionColumns = false,
          java.util.TimeZone.getDefault,
          dateFormatter,
          timestampFormatter)
        ._1
        .get
    parsedPartition.columnNames
      .zip(
        parsedPartition.literals
          .map(l => Cast(l, StringType).eval())
          .map(Option(_).map(_.toString).orNull))
      .toMap
  }
}
