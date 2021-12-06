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

import org.apache.spark.sql.DataFrameWriter

class DataFrameWriterImplicits[T](w: DataFrameWriter[T]) {

  def arrow(path: String): Unit = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    w.format("arrow").save(path)
  }
}

object DataFrameWriterImplicits {
  implicit def writerConverter[T](w: DataFrameWriter[T]): DataFrameWriterImplicits[T] = {
    new DataFrameWriterImplicits[T](w)
  }
}
