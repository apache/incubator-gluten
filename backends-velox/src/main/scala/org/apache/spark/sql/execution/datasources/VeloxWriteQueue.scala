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
package org.apache.spark.sql.execution.datasources

import io.glutenproject.spark.sql.execution.datasources.velox.DatasourceJniWrapper

import org.apache.spark.sql.execution.datasources.VeloxWriteQueue.EOS_BATCH
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern

class VeloxWriteQueue(
    dsHandle: Long,
    schema: Schema,
    allocator: BufferAllocator,
    datasourceJniWrapper: DatasourceJniWrapper,
    outputFileURI: String)
  extends AutoCloseable {
  private val scanner = new VeloxColumnarBatchIterator(schema, allocator)
  private val writeException = new AtomicReference[Throwable]

  private val writeThread = new Thread(
    () => {
      try {
        datasourceJniWrapper.write(dsHandle, scanner)
      } catch {
        case e: Throwable =>
          writeException.set(e)
      }
    },
    "VeloxWriteQueue - " + UUID.randomUUID().toString
  )

  writeThread.start()

  private def checkWriteException(): Unit = {
    // check if VeloxWriteQueue thread was failed
    val exception = writeException.get()
    if (exception != null) {
      throw exception
    }
  }

  def enqueue(batch: ColumnarBatch): Unit = {
    scanner.enqueue(batch)
    checkWriteException()
  }

  override def close(): Unit = {
    scanner.enqueue(EOS_BATCH)
    writeThread.join()
    checkWriteException()
  }
}

object VeloxWriteQueue {
  val EOS_BATCH = new ColumnarBatch(null)
  val TAILING_FILENAME_REGEX = Pattern.compile("^(.*)/([^/]+)$")
}
