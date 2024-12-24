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

import org.apache.spark.SparkEnv

object BroadcastFactoryInjection {
  private val FIELD_INITIALIZED = {
    val f = classOf[BroadcastManager].getDeclaredField("initialized")
    f.setAccessible(true)
    f
  }
  private val FIELD_BROADCAST_FACTORY = {
    val f = classOf[BroadcastManager].getDeclaredField("broadcastFactory")
    f.setAccessible(true)
    f
  }
  private val METHOD_INITIALIZE = {
    val m = classOf[BroadcastManager].getDeclaredMethod("initialize")
    m.setAccessible(true)
    m
  }

  def get(): BroadcastFactory = {
    require(SparkEnv.get != null, "No SparkEnv instance founded")
    val bm = SparkEnv.get.broadcastManager
    bm.synchronized {
      assert(FIELD_INITIALIZED.get(bm) == true)
      val bf = FIELD_BROADCAST_FACTORY.get(bm).asInstanceOf[BroadcastFactory]
      bf
    }
  }

  def inject(factory: BroadcastFactory): Unit = {
    require(SparkEnv.get != null, "No SparkEnv instance founded")
    val bm = SparkEnv.get.broadcastManager
    bm.synchronized {
      // Assert BroadcastManager.initialized == true.
      assert(FIELD_INITIALIZED.get(bm) == true)

      // Reset BroadcastManager.initialized to false.
      FIELD_INITIALIZED.set(bm, false)
      assert(FIELD_INITIALIZED.get(bm) == false)

      // Set the new factory.
      FIELD_BROADCAST_FACTORY.set(bm, factory)

      // Initialize the broadcast manager. This maps to BroadcastManager#initialzie().
      factory.initialize(bm.isDriver, SparkEnv.get.conf)
      FIELD_INITIALIZED.set(bm, true)

      // Assert BroadcastManager.initialized == true.
      assert(FIELD_INITIALIZED.get(bm) == true)

      // Make a call attempt to BroadcastManager.initialize(), to make sure the new factory is
      // still kept and not replaced by TorrentBroadcastFactory accidentally.
      METHOD_INITIALIZE.invoke(bm)
      assert(FIELD_INITIALIZED.get(bm) == true)
      assert(FIELD_BROADCAST_FACTORY.get(bm) eq factory)
    }
  }
}
