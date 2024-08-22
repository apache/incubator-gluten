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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.FileFormatWriter.ConcurrentOutputWriterSpec

/**
 * This class is copied from Spark 3.4 and modified for Gluten. Spark 3.4 introduced a new operator,
 * WriteFiles. In order to support the WriteTransformer in Spark 3.4, we need to copy the WriteFiles
 * file into versions 3.2 and 3.3 to resolve compilation issues. Within WriteFiles, the
 * doExecuteWrite method overrides the same method in SparkPlan. To avoid modifying SparkPlan, we
 * have changed the doExecuteWrite method to a non-overridden method.
 */

/**
 * The write files spec holds all information of [[V1WriteCommand]] if its provider is
 * [[FileFormat]].
 */
case class WriteFilesSpec(
    description: WriteJobDescription,
    committer: FileCommitProtocol,
    concurrentOutputWriterSpecFunc: SparkPlan => Option[ConcurrentOutputWriterSpec])

/**
 * During Optimizer, [[V1Writes]] injects the [[WriteFiles]] between [[V1WriteCommand]] and query.
 * [[WriteFiles]] must be the root plan as the child of [[V1WriteCommand]].
 */
case class WriteFiles(
    child: LogicalPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec)
  extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def stringArgs: Iterator[Any] = Iterator(child)
  override protected def withNewChildInternal(newChild: LogicalPlan): WriteFiles =
    copy(child = newChild)
}

/** Responsible for writing files. */
case class WriteFilesExec(
    child: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec)
  extends UnaryExecNode {
  override def output: Seq[Attribute] = Seq.empty

  def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    throw new UnsupportedOperationException(s"$nodeName does not support doExecuteWrite")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"$nodeName does not support doExecute")
  }

  override protected def stringArgs: Iterator[Any] = Iterator(child)

  override protected def withNewChildInternal(newChild: SparkPlan): WriteFilesExec =
    copy(child = newChild)
}
