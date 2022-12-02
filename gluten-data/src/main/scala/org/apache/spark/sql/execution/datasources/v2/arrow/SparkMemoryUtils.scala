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

package org.apache.spark.sql.execution.datasources.v2.arrow

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.memory.TaskMemoryResources

object SparkMemoryUtils extends Logging {

  class UnsafeItr[T <: AutoCloseable](delegate: Iterator[T])
    extends Iterator[T] {
    val holder = new GenericRetainer[T]()

    TaskMemoryResources.addLeakSafeTaskCompletionListener[Unit]((_: TaskContext) => {
      holder.release()
    })

    override def hasNext: Boolean = {
      holder.release()
      val hasNext = delegate.hasNext
      hasNext
    }

    override def next(): T = {
      val b = delegate.next()
      holder.retain(b)
      b
    }
  }

  class GenericRetainer[T <: AutoCloseable] {
    private var retained: Option[T] = None

    def retain(batch: T): Unit = {
      if (retained.isDefined) {
        throw new IllegalStateException
      }
      retained = Some(batch)
    }

    def release(): Unit = {
      retained.foreach(b => b.close())
      retained = None
    }
  }
}
