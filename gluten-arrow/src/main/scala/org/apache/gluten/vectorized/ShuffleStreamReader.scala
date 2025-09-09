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
package org.apache.gluten.vectorized

import org.apache.spark.storage.BlockId

import java.io.InputStream

case class ShuffleStreamReader(streams: Iterator[(BlockId, InputStream)]) {
  private val jniStreams = streams.map {
    case (blockId, in) =>
      (blockId, JniByteInputStreams.create(in))
  }

  private var currentStream: JniByteInputStream = _

  // Called from native side to get the next stream.
  def nextStream(): JniByteInputStream = {
    if (currentStream != null) {
      currentStream.close()
    }
    if (!jniStreams.hasNext) {
      currentStream = null
    } else {
      currentStream = jniStreams.next._2
    }
    currentStream
  }

  def close(): Unit = {
    // The reader may not attempt to read all streams from `nextStream`, so we need to close the
    // current stream if it's not null.
    if (currentStream != null) {
      currentStream.close()
      currentStream = null
    }
  }
}
