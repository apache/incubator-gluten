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
package io.glutenproject.backendsapi.velox

import io.glutenproject.backendsapi.{BackendsApiManager, TransformerApi}
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.utils.InputPartitionsUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.BitSet

import java.util

class TransformerHandler extends TransformerApi with Logging {

  /**
   * Do validate for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def validateColumnarShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan): Boolean = {
    new Validator().doSchemaValidate(child.schema)
  }

  /**
   * Used for table scan validation.
   *
   * @return
   *   true if backend supports reading the file format.
   */
  override def supportsReadFileFormat(
      fileFormat: ReadFileFormat,
      fields: Array[StructField],
      partTable: Boolean,
      paths: Seq[String]): Boolean = {
    BackendsApiManager.getSettings
      .supportFileFormatRead(fileFormat, fields, partTable, paths)
  }

  /** Generate Seq[InputPartition] for FileSourceScanExecTransformer. */
  def genInputPartitionSeq(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean): Seq[InputPartition] = {
    InputPartitionsUtil(
      relation,
      selectedPartitions,
      output,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan)
      .genInputPartitionSeq()
  }

  override def postProcessNativeConfig(
      nativeConfMap: util.Map[String, String],
      backendPrefix: String): Unit = {
    // TODO: IMPLEMENT SPECIAL PROCESS FOR VELOX BACKEND
  }
}
