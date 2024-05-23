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
package org.apache.spark.shuffle.utils

import org.apache.spark.shuffle.{ColumnarShuffleWriter, GenShuffleWriterParameters, GlutenShuffleWriterWrapper}

object ShuffleUtil {

  def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {
    GlutenShuffleWriterWrapper(
      new ColumnarShuffleWriter[K, V](
        parameters.shuffleBlockResolver,
        parameters.columnarShuffleHandle,
        parameters.mapId,
        parameters.metrics,
        parameters.isSort))
  }
}
