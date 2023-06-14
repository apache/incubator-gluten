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
package org.apache.spark.sql.hive

import io.glutenproject.backendsapi.BackendsApiManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{BZip2Codec, CompressionCodecFactory, DefaultCodec}

class CHJsonScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter],
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
  extends JsonScan(
    sparkSession,
    fileIndex,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    options,
    pushedFilters,
    partitionFilters,
    dataFilters) {

  override def isSplitable(path: Path): Boolean = {
    val codecFactory: CompressionCodecFactory = new CompressionCodecFactory(
      sparkSession.sessionState.newHadoopConfWithOptions(Map.empty))
    val compressionCodec = codecFactory.getCodec(path)
    if (compressionCodec == null) {
      super.isSplitable(path)
    } else {
      var compressionMethod = null.asInstanceOf[String]
      compressionCodec match {
        case d: DefaultCodec =>
          compressionMethod = CHCompressionCodec.zlib.name
        case b: BZip2Codec =>
          compressionMethod = CHCompressionCodec.bzip2.name
        case _ =>
          compressionMethod = CHCompressionCodec.unknown.name
      }
      super.isSplitable(path) && BackendsApiManager
          .getValidatorApiInstance.doCompressionSplittableValidate(compressionMethod)
    }
  }
}
