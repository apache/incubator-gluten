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

package org.apache.gluten.integration

package object action {
  implicit class DualOptionsOps[T](value: (Option[T], Option[T])) {
    def onBothProvided[R](func: (T, T) => R): Option[R] = {
      if (value._1.isEmpty || value._2.isEmpty) {
        return None
      }
      Some(func(value._1.get, value._2.get))
    }
  }

  implicit class DualMetricsOps(value: (Map[String, Long], Map[String, Long])) {
    def sumUp: Map[String, Long] = {
      assert(value._1.keySet == value._2.keySet)
      value._1.map { case (k, v) => k -> (v + value._2(k)) }
    }
  }
}
