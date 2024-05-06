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
package org.apache.spark.sql

import org.apache.spark.SparkConf

object ConfUtils {
  object ConfImplicits {
    // thread-safe
    implicit class SparkConfWrapper(conf: SparkConf) {

      def setWarningOnOverriding(key: String, value: String): SparkConf = {
        setConfSafe(
          key,
          value,
          onOverriding => {
            Console.err.println(
              s"Overriding SparkConf key ${onOverriding.key}, old value: ${onOverriding.value}, new value: ${onOverriding.newValue}. ")
          })
      }

      def setAllWarningOnOverriding(others: Iterable[(String, String)]): SparkConf = {
        var tmp: SparkConf = conf

        others.foreach(c => {
          tmp = new SparkConfWrapper(tmp).setWarningOnOverriding(c._1, c._2)
        })

        tmp
      }

      private def setConfSafe(
          key: String,
          value: String,
          onOverriding: OnOverriding => Unit): SparkConf = synchronized {
        if (conf.contains(key)) {
          val oldValue = conf.get(key)
          onOverriding(OnOverriding(key, oldValue, value))
        }
        conf.set(key, value)
      }

      private case class OnOverriding(key: String, value: String, newValue: String)
    }
  }
}
