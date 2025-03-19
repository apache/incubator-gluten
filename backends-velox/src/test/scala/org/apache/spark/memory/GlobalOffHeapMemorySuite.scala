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
package org.apache.spark.memory;

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.memory.memtarget.{Spillers, TreeMemoryTarget}
import org.apache.gluten.memory.memtarget.spark.TreeMemoryConsumers

import org.apache.spark.TaskContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.task.TaskResources

import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections;

class GlobalOffHeapMemorySuite extends AnyFunSuite with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    val conf = SQLConf.get
    conf.setConfString("spark.memory.offHeap.enabled", "true")
    conf.setConfString("spark.memory.offHeap.size", "400")
    conf.setConfString(GlutenConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES.key, "100")
  }

  test("Sanity") {
    TaskResources.runUnsafe {
      val factory =
        TreeMemoryConsumers.factory(TaskContext.get().taskMemoryManager())
      val consumer =
        factory
          .legacyRoot()
          .newChild(
            "FOO",
            TreeMemoryTarget.CAPACITY_UNLIMITED,
            Spillers.NOOP,
            Collections.emptyMap())
      Assert.assertEquals(300, consumer.borrow(300))
      Assert.assertTrue(GlobalOffHeapMemory.acquire(50))
      Assert.assertTrue(GlobalOffHeapMemory.acquire(40))
      Assert.assertFalse(GlobalOffHeapMemory.acquire(30))
      Assert.assertFalse(GlobalOffHeapMemory.acquire(11))
      Assert.assertTrue(GlobalOffHeapMemory.acquire(10))
      Assert.assertTrue(GlobalOffHeapMemory.acquire(0))
      Assert.assertFalse(GlobalOffHeapMemory.acquire(1))
    }
  }

  test("Task OOM by global occupation") {
    TaskResources.runUnsafe {
      val factory =
        TreeMemoryConsumers.factory(TaskContext.get().taskMemoryManager())
      val consumer =
        factory
          .legacyRoot()
          .newChild(
            "FOO",
            TreeMemoryTarget.CAPACITY_UNLIMITED,
            Spillers.NOOP,
            Collections.emptyMap())
      Assert.assertTrue(GlobalOffHeapMemory.acquire(200))
      Assert.assertEquals(100, consumer.borrow(100))
      Assert.assertEquals(100, consumer.borrow(200))
      Assert.assertEquals(0, consumer.borrow(50))
    }
  }

  test("Release global") {
    TaskResources.runUnsafe {
      val factory =
        TreeMemoryConsumers.factory(TaskContext.get().taskMemoryManager())
      val consumer =
        factory
          .legacyRoot()
          .newChild(
            "FOO",
            TreeMemoryTarget.CAPACITY_UNLIMITED,
            Spillers.NOOP,
            Collections.emptyMap())
      Assert.assertTrue(GlobalOffHeapMemory.acquire(300))
      Assert.assertEquals(100, consumer.borrow(200))
      GlobalOffHeapMemory.free(10)
      Assert.assertEquals(10, consumer.borrow(50))
    }
  }

  test("Release task") {
    TaskResources.runUnsafe {
      val factory =
        TreeMemoryConsumers.factory(TaskContext.get().taskMemoryManager())
      val consumer =
        factory
          .legacyRoot()
          .newChild(
            "FOO",
            TreeMemoryTarget.CAPACITY_UNLIMITED,
            Spillers.NOOP,
            Collections.emptyMap())
      Assert.assertEquals(300, consumer.borrow(300))
      Assert.assertFalse(GlobalOffHeapMemory.acquire(200))
      Assert.assertEquals(100, consumer.repay(100))
      Assert.assertTrue(GlobalOffHeapMemory.acquire(200))
    }
  }
}
