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

package org.apache.spark.sql.utils

import java.net.URI

import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import org.apache.arrow.dataset.file.{FileFormat, FileSystemDatasetFactory}
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.execution.datasources.v2.arrow.SparkSchemaUtils
import org.apache.spark.sql.types.StructType

object GazelleArrowUtils {

  private def rewriteUri(encodeUri: String): String = {
    val decodedUri = encodeUri
    val uri = URI.create(decodedUri)
    if (uri.getScheme == "s3" || uri.getScheme == "s3a") {
      val s3Rewritten = new URI("s3", uri.getAuthority,
        uri.getPath, uri.getQuery, uri.getFragment).toString
      return s3Rewritten
    }
    val sch = uri.getScheme match {
      case "hdfs" => "hdfs"
      case "file" => "file"
    }
    val ssp = uri.getScheme match {
      case "hdfs" => uri.getSchemeSpecificPart
      case "file" => "//" + uri.getSchemeSpecificPart
    }
    val rewritten = new URI(sch, ssp, uri.getFragment)
    rewritten.toString
  }

  private def makeArrowDiscovery(encodedUri: String,
                         startOffset: Long, length: Long): FileSystemDatasetFactory = {

    val format = FileFormat.PARQUET
    val allocator = ArrowBufferAllocators.contextInstance()
    val factory = new FileSystemDatasetFactory(allocator,
      null, // SparkMemoryUtils.contextMemoryPool()
      format,
      rewriteUri(encodedUri),
      startOffset,
      length)
    factory
  }

  private def readSchema(file: FileStatus): Option[StructType] = {
    val factory: FileSystemDatasetFactory =
      makeArrowDiscovery(file.getPath.toString, -1L, -1L)
    val schema = factory.inspect()
    try {
      Option(SparkSchemaUtils.fromArrowSchema(schema))
    } finally {
      factory.close()
    }
  }

  def readSchema(files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) {
      throw new IllegalArgumentException("No input file specified")
    }
    readSchema(files.toList.head) // todo merge schema
  }

}
