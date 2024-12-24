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
package org.apache.spark.broadcast

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

import scala.reflect.ClassTag

class BroadcastFactoryUnsafeAccessSuite extends SharedSparkSession {
  import BroadcastFactoryUnsafeAccessSuite._
  test("Get") {
    val factory = BroadcastFactoryUnsafeAccess.get()
    assert(factory.isInstanceOf[TorrentBroadcastFactory])
  }

  test("Inject") {
    BroadcastFactoryUnsafeAccess.get().stop()
    val factory = new DummyBroadcastFactory()
    assert(!factory.initialized)
    BroadcastFactoryUnsafeAccess.set(factory)
    assert(BroadcastFactoryUnsafeAccess.get() eq factory)
    assert(factory.initialized)
    val text: String = "DUMMY"
    val b = SparkShimLoader.getSparkShims.broadcastInternal(sparkContext, text)
    assert(b.isInstanceOf[DummyBroadcast[String]])
  }
}

object BroadcastFactoryUnsafeAccessSuite {
  private class DummyBroadcast[T: ClassTag](id: Long, value: T) extends Broadcast[T](id) {
    override protected def getValue(): T = value
    override protected def doUnpersist(blocking: Boolean): Unit = {
      // No-op.
    }
    override protected def doDestroy(blocking: Boolean): Unit = {
      // No-op.
    }
  }

  private class DummyBroadcastFactory() extends CompatibleBroadcastFactory {
    var initialized = false

    override def initialize(isDriver: Boolean, conf: SparkConf): Unit = {
      initialized = true
    }
    override def newBroadcast[T: ClassTag](value: T, isLocal: Boolean, id: Long): Broadcast[T] = {
      newBroadcast(value, isLocal, id, serializedOnly = true)
    }
    override def newBroadcast[T: ClassTag](
        value: T,
        isLocal: Boolean,
        id: Long,
        serializedOnly: Boolean): Broadcast[T] = {
      new DummyBroadcast[T](id, value)
    }
    override def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
      // No-op
    }
    override def stop(): Unit = {
      // No-op.
    }
  }
}
