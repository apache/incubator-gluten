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

package com.intel.oap.spark.sql.execution.datasources.v2.parquet

import java.util.Objects

import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetFileFormatIndicator}

object ServiceLoaderUtil {
  def ensureParquetFileFormatOverwritten(): Unit = {
    val fmt = new ParquetFileFormat()
    if (!Objects.equals(fmt.toString(), ParquetFileFormatIndicator.OVERWRITTEN_INDICATOR)) {
      throw new ClassNotFoundException("ParquetFileFormat is not overwritten by Arrow. Consider " +
        "reordering jar dependencies to let the overwritten version to be recognized by JVM")
    }
  }
}
