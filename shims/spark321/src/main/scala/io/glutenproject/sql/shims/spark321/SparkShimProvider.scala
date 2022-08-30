/*
 * Copyright 2020 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.sql.shims.spark321

import io.glutenproject.sql.shims.{SparkShims, SparkShimDescriptor}

object SparkShimProvider {
  val DESCRIPTOR = SparkShimDescriptor(3, 2, 1)
  val DESCRIPTOR_STRINGS = Seq(s"$DESCRIPTOR")
}

class SparkShimProvider extends io.glutenproject.sql.shims.SparkShimProvider {
  def createShim: SparkShims = {
    new Spark321Shims()
  }

  def matches(version: String): Boolean = {
    SparkShimProvider.DESCRIPTOR_STRINGS.contains(version)
  }  
}
