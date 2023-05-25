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

package io.glutenproject.utils

import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.spark.sql.execution.datasources.velox.DatasourceJniWrapper
import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.vector.util.SchemaUtility
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkSchemaUtil
import org.apache.spark.sql.types.StructType

object VeloxDatasourceUtil {
  def readSchema(files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) {
      throw new IllegalArgumentException("No input file specified")
    }
    readSchema(files.toList.head)
  }

  def readSchema(file: FileStatus): Option[StructType] = {
    val allocator = ArrowBufferAllocators.contextInstance()
    val datasourceJniWrapper = new DatasourceJniWrapper()
    val instanceId = datasourceJniWrapper.nativeInitDatasource(file.getPath.toString, -1)
    val cSchema = ArrowSchema.allocateNew(allocator)
    datasourceJniWrapper.inspectSchema(instanceId, cSchema.memoryAddress())
    try {
      Option(SparkSchemaUtil.fromArrowSchema(
        GlutenArrowAbiUtil.importToSchema(allocator, cSchema)))
    } finally {
      cSchema.close()
      datasourceJniWrapper.close(instanceId)
    }
  }
}
