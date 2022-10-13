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

package org.apache.spark.sql.execution.datasource.arrow

import java.lang
import java.net.URI
import java.util.{Collections, UUID}
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern

import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.utils.ArrowAbiUtil
import org.apache.arrow.c.ArrowArray
import org.apache.arrow.dataset.file.DatasetFileWriter
import org.apache.arrow.dataset.file.FileFormat
import org.apache.arrow.dataset.scanner.{Scanner, ScanTask}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasource.arrow.ArrowWriteQueue.{EOS_ARROW_ARRAY, ScannerImpl}

class ArrowWriteQueue(schema: Schema, fileFormat: FileFormat, outputFileURI: String)
  extends AutoCloseable with Logging {
  private val scanner = new ScannerImpl(schema)
  private val writeException = new AtomicReference[Throwable]

  private val writeThread = new Thread(() => {
    URI.create(outputFileURI) // validate uri
    val matcher = ArrowWriteQueue.TAILING_FILENAME_REGEX.matcher(outputFileURI)
    if (!matcher.matches()) {
      throw new IllegalArgumentException("illegal out put file uri: " + outputFileURI)
    }
    val dirURI = matcher.group(1)
    val fileName = matcher.group(2)

    try {
      DatasetFileWriter.write(ArrowBufferAllocators.contextInstance(),
        scanner, fileFormat, dirURI, Array(), 1, "{i}_" + fileName)
    } catch {
      case e: Throwable =>
        writeException.set(e)
    }
  }, "ArrowWriteQueue - " + UUID.randomUUID().toString)

  writeThread.start()

  private def checkWriteException(): Unit = {
    // check if ArrowWriteQueue thread was failed
    val exception = writeException.get()
    if (exception != null) {
      logWarning("Failed to write arrow.", exception)
      throw exception
    }
  }

  def enqueue(array: ArrowArray): Unit = {
    scanner.enqueue(array)
    checkWriteException()
  }

  override def close(): Unit = {
    scanner.enqueue(EOS_ARROW_ARRAY)
    writeThread.join()
    checkWriteException()
  }
}

object ArrowWriteQueue {
  private val TAILING_FILENAME_REGEX = Pattern.compile("^(.*)/([^/]+)$")
  private val EOS_ARROW_ARRAY = ArrowArray.allocateNew(
    ArrowBufferAllocators.contextInstance())

  class ScannerImpl(schema: Schema) extends Scanner {
    private val writeQueue = new ArrayBlockingQueue[ArrowArray](1024)

    def enqueue(array: ArrowArray): Unit = {
      writeQueue.put(array)
    }

    override def scan(): lang.Iterable[_ <: ScanTask] = {
      Collections.singleton(new ScanTask {
        override def execute(): ArrowReader = {
          val allocator = ArrowBufferAllocators.contextInstance()
          new ArrowReader(allocator) {
            override def loadNextBatch(): Boolean = {
              val currentArray = try {
                writeQueue.poll(30L, TimeUnit.MINUTES)
              } catch {
                case _: InterruptedException =>
                  Thread.currentThread().interrupt()
                  EOS_ARROW_ARRAY
              }
              if (currentArray == null) {
                throw new RuntimeException("ArrowWriter: Timeout waiting for data")
              }
              if (currentArray == EOS_ARROW_ARRAY) {
                EOS_ARROW_ARRAY.release()
                return false
              }
              ArrowAbiUtil.importIntoVectorSchemaRoot(
                allocator, currentArray, getVectorSchemaRoot(), this)
              true
            }

            override def bytesRead(): Long = 0L

            override def closeReadSource(): Unit = {}

            override def readSchema(): Schema = schema
          }
        }

        override def close(): Unit = {

        }
      })
    }

    override def schema(): Schema = {
      schema
    }

    override def close(): Unit = {

    }
  }
}
