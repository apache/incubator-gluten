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
package org.apache.gluten.shims

import org.apache.gluten.sql.shims.SparkShimDescriptor

import org.scalatest.funsuite.AnyFunSuite

class SparkShimDescriptorSuite extends AnyFunSuite {

  test("Spark shims descriptor") {
    val descriptor: SparkShimDescriptor = SparkShimDescriptor.DESCRIPTOR
    assert(SparkShimDescriptor.DESCRIPTOR.matches(descriptor))
    assert(descriptor.matches(SparkShimDescriptor.DESCRIPTOR))
    assert(descriptor === SparkShimDescriptor.DESCRIPTOR)
    assert(SparkShimDescriptor("3.2.2") === SparkShimDescriptor(3, 2, 2))
    assert(SparkShimDescriptor("3.2.2-SNAPSHOT") === SparkShimDescriptor(3, 2, 2))
    assert(SparkShimDescriptor("3.3") === SparkShimDescriptor(3, 3, 0))
    assert(SparkShimDescriptor("4.0.0-preview1") === SparkShimDescriptor(4, 0, 0))

  }
}
