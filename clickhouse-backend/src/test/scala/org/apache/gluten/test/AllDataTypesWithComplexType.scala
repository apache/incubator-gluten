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
package org.apache.gluten.test

import java.sql.{Date, Timestamp}

case class AllDataTypesWithComplexType(
    string_field: String = null,
    int_field: java.lang.Integer = null,
    long_field: java.lang.Long = null,
    float_field: java.lang.Float = null,
    double_field: java.lang.Double = null,
    short_field: java.lang.Short = null,
    byte_field: java.lang.Byte = null,
    boolean_field: java.lang.Boolean = null,
    decimal_field: java.math.BigDecimal = null,
    date_field: java.sql.Date = null,
    timestamp_field: java.sql.Timestamp = null,
    array: Seq[Int] = null,
    arrayContainsNull: Seq[Option[Int]] = null,
    map: Map[Int, Long] = null,
    mapValueContainsNull: Map[Int, Option[Long]] = null
)

object AllDataTypesWithComplexType {
  def genTestData(): Seq[AllDataTypesWithComplexType] = {
    (0 to 199).map {
      i =>
        if (i % 100 == 1) {
          AllDataTypesWithComplexType()
        } else {
          AllDataTypesWithComplexType(
            s"$i",
            i,
            i.toLong,
            i.toFloat,
            i.toDouble,
            i.toShort,
            i.toByte,
            i % 2 == 0,
            new java.math.BigDecimal(i + ".56"),
            Date.valueOf(new Date(System.currentTimeMillis()).toLocalDate.plusDays(i % 10)),
            Timestamp.valueOf(
              new Timestamp(System.currentTimeMillis()).toLocalDateTime.plusDays(i % 10)),
            Seq.apply(i + 1, i + 2, i + 3),
            Seq.apply(Option.apply(i + 1), Option.empty, Option.apply(i + 3)),
            Map.apply((i + 1, i + 2), (i + 3, i + 4)),
            Map.empty
          )
        }
    }
  }
}
