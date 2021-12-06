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

package com.intel.oap.spark.sql.execution.datasources.v2.arrow

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class ArrowOptions(val parameters: CaseInsensitiveMap[String])
  extends Serializable {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val originalFormat = parameters
    .get(ArrowOptions.KEY_ORIGINAL_FORMAT)
    .getOrElse(ArrowOptions.DEFAULT_ORIGINAL_FORMAT)
  val targetFormat = parameters
      .get(ArrowOptions.KEY_TARGET_FORMAT)
      .getOrElse(ArrowOptions.DEFAULT_TARGET_FORMAT)

  @deprecated
  val filesystem = parameters
    .get(ArrowOptions.KEY_FILESYSTEM)
    .getOrElse(ArrowOptions.DEFAULT_FILESYSTEM)
}

object ArrowOptions {
  val KEY_ORIGINAL_FORMAT = "originalFormat"
  val DEFAULT_ORIGINAL_FORMAT = "parquet"
  val KEY_TARGET_FORMAT = "targetFormat"
  val DEFAULT_TARGET_FORMAT = "parquet"

  @deprecated
  val KEY_FILESYSTEM = "filesystem"
  val DEFAULT_FILESYSTEM = "hdfs"
}
