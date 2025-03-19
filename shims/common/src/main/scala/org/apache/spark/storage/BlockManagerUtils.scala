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
