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

package com.intel.oap.spark.sql

import org.apache.spark.sql.{DataFrame, DataFrameReader}

class DataFrameReaderImplicits(r: DataFrameReader) {

  /**
   * Loads a file via Arrow Datasets API and returns the result as a `DataFrame`.
   *
   * @param path input path
   * @since 3.0.0-SNAPSHOT
   */
  def arrow(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    arrow(Seq(path): _*)
  }

  /**
   * Loads files via Arrow Datasets API and returns the result as a `DataFrame`.
   *
   * @param paths input paths
   * @since 3.0.0-SNAPSHOT
   */
  @scala.annotation.varargs
  def arrow(paths: String*): DataFrame = r.format("arrow").load(paths: _*)
}

object DataFrameReaderImplicits {
  implicit def readerConverter(r: DataFrameReader): DataFrameReaderImplicits = {
    new DataFrameReaderImplicits(r)
  }
}
