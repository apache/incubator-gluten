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
package org.apache.spark.storage

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.SERIALIZER
import org.apache.spark.memory.MemoryManager
import org.apache.spark.serializer.{Serializer, SerializerManager}
import org.apache.spark.storage.memory.{BlockEvictionHandler, MemoryStore}
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

import scala.reflect.ClassTag

object BlockManagerUtils {
  def setTestMemoryStore(conf: SparkConf, memoryManager: MemoryManager, isDriver: Boolean): Unit = {
    val store = new MemoryStore(
      conf,
      new BlockInfoManager,
      new SerializerManager(
        Utils.instantiateSerializerFromConf[Serializer](SERIALIZER, conf, isDriver),
        conf),
      memoryManager,
      new BlockEvictionHandler {
        override private[storage] def dropFromMemory[T: ClassTag](
            blockId: BlockId,
            data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {
          throw new UnsupportedOperationException(
            s"Cannot drop block ID $blockId from test memory store")
        }
      }
    )
    memoryManager.setMemoryStore(store)
  }
}
