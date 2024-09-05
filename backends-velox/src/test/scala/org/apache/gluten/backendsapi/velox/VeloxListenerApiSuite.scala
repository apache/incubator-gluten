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
package org.apache.gluten.backendsapi.velox

import org.apache.gluten.test.VeloxBackendTestBase

import org.scalatest.funsuite.AnyFunSuite

class VeloxListenerApiSuite extends AnyFunSuite {
  test("Unsupported arch") {
    val arch = System.getProperty("os.arch")
    System.setProperty("os.arch", "unknown-arch")
    try {
      val api = new VeloxListenerApi()
      val error = intercept[RuntimeException] {
        api.onDriverStart(
          VeloxBackendTestBase.mockSparkContext(),
          VeloxBackendTestBase.mockPluginContext())
      }
      assert(error.getMessage.contains("FileNotFoundException"))
      assert(error.getMessage.contains("unknown-arch/libgluten.so"))
    } finally {
      System.setProperty("os.arch", arch)
    }
  }
}
