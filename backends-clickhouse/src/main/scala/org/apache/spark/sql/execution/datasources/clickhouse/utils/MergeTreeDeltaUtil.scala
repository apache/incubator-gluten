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
package org.apache.spark.sql.execution.datasources.clickhouse.utils

import org.apache.gluten.expression.ConverterUtils.normalizeColName

import org.apache.spark.sql.execution.datasources.mergetree.StorageMeta.DEFAULT_ORDER_BY_KEY

object MergeTreeDeltaUtil {

  def genOrderByAndPrimaryKeyStr(
      orderByKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]]): (String, String) = {

    val orderByKey =
      orderByKeyOption.filter(_.nonEmpty).map(columnsToStr).getOrElse(DEFAULT_ORDER_BY_KEY)
    val primaryKey = primaryKeyOption
      .filter(p => orderByKey != DEFAULT_ORDER_BY_KEY && p.nonEmpty)
      .map(columnsToStr)
      .getOrElse("")

    (orderByKey, primaryKey)
  }

  def columnsToStr(option: Option[Seq[String]]): String = option.map(columnsToStr).getOrElse("")

  def columnsToStr(keys: Seq[String]): String = {
    keys.map(normalizeColName).mkString(",")
  }
}
