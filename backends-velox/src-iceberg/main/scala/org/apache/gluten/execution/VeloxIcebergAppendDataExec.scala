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

import org.apache.gluten.connector.write.{ColumnarBatchDataWriterFactory, IcebergDataWriteFactory}
import org.apache.gluten.proto.{IcebergPartitionField, IcebergPartitionSpec}

import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.types.StructType

import org.apache.iceberg.spark.source.IcebergWriteUtil
import org.apache.iceberg.transforms.IcebergTransformUtil

import java.util.stream.Collectors

case class VeloxIcebergAppendDataExec(query: SparkPlan, refreshCache: () => Unit, write: Write)
  extends IcebergAppendDataExec {

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergAppendDataExec =
    copy(query = newChild)

  override def createFactory(schema: StructType): ColumnarBatchDataWriterFactory =
    IcebergDataWriteFactory(
      schema,
      getFileFormat(IcebergWriteUtil.getFileFormat(write)),
      IcebergWriteUtil.getDirectory(write),
      getCodec,
      getPartitionSpec)
}

object VeloxIcebergAppendDataExec {
  def apply(original: AppendDataExec): IcebergAppendDataExec = {
    VeloxIcebergAppendDataExec(
      original.query,
      original.refreshCache,
      original.write
    )
  }
}
