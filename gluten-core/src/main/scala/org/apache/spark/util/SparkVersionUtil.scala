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
package org.apache.spark.util

object SparkVersionUtil {
  // Returns X. X < 0 if one < other, x == 0 if one == other, x > 0 if one > other.
  def compareMajorMinorVersion(other: (Int, Int)): Int = {
    val current = VersionUtils.majorMinorVersion(org.apache.spark.SPARK_VERSION)
    val base = 1000
    assert(current._2 < base && other._2 < base)
    current._1 * base + current._2 - (other._1 * base + other._2)
  }
}
