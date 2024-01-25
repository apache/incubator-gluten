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
package org.apache.spark.sql.execution.datasources.utils

object MergeTreeDeltaUtil {

  val DEFAULT_ORDER_BY_KEY = "tuple()"

  def genOrderByAndPrimaryKeyStr(
      orderByKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]]): (String, String) = {
    val orderByKey =
      if (orderByKeyOption.isDefined && orderByKeyOption.get.nonEmpty) {
        orderByKeyOption.get.mkString(",")
      } else DEFAULT_ORDER_BY_KEY

    val primaryKey =
      if (
        !orderByKey.equals(DEFAULT_ORDER_BY_KEY) && primaryKeyOption.isDefined &&
        primaryKeyOption.get.nonEmpty
      ) {
        primaryKeyOption.get.mkString(",")
      } else ""

    (orderByKey, primaryKey)
  }
}
