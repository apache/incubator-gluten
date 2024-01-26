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
package io.glutenproject.utils

import io.glutenproject.exec.Runtimes
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.memory.nmm.NativeMemoryManagers

import org.apache.spark.util.TaskResources

import org.scalatest.funsuite.AnyFunSuite

class MemoryTaskResourceSuite extends AnyFunSuite {
  test("Run unsafe - runtime") {
    TaskResources.runUnsafe {
      assert(Runtimes.contextInstance() != null)
    }
  }

  test("Run unsafe - memory allocation, Arrow") {
    TaskResources.runUnsafe {
      val allocator = ArrowBufferAllocators.contextInstance()
      val buf = allocator.buffer(100)
      assert(buf != null)
    }
  }

  test("Run unsafe - memory allocation, Native") {
    TaskResources.runUnsafe {
      val nmm = NativeMemoryManagers.contextInstance("test")
      assert(nmm != null)
    }
  }
}
