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
package org.apache.spark.shuffle

import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.spark.shuffle.sort.ColumnarShuffleHandle

case class GlutenShuffleWriterWrapper[K, V](shuffleWriter: ShuffleWriter[K, V])

case class GenShuffleWriterParameters[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    columnarShuffleHandle: ColumnarShuffleHandle[K, V],
    mapId: Long,
    metrics: ShuffleWriteMetricsReporter)

object GlutenShuffleWriterWrapper {

  def genColumnarShuffleWriter[K, V](
      shuffleBlockResolver: IndexShuffleBlockResolver,
      columnarShuffleHandle: ColumnarShuffleHandle[K, V],
      mapId: Long,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] =
    BackendsApiManager.getSparkPlanExecApiInstance
      .genColumnarShuffleWriter(
        GenShuffleWriterParameters(shuffleBlockResolver, columnarShuffleHandle, mapId, metrics))
      .shuffleWriter
}
