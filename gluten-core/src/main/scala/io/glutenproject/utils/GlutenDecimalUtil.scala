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

import scala.math.min

import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType}
import org.apache.spark.sql.types.DecimalType.{MAX_PRECISION, MAX_SCALE}

object GlutenDecimalUtil {
  object Fixed {
    def unapply(t: DecimalType): Option[(Int, Int)] = Some((t.precision, t.scale))
  }

  def bounded(precision: Int, scale: Int): DecimalType = {
    DecimalType(min(precision, MAX_PRECISION), min(scale, MAX_SCALE))
  }

  def getAvgSumDataType(avg: Average): DataType = avg.dataType match {
    // avg.dataType is Decimal(p + 4, s + 4) and sumType is Decimal(p + 10, s)
    // we need to get sumType, so p = p - 4 + 10 and s = s - 4
    case _ @ GlutenDecimalUtil.Fixed(p, s) => GlutenDecimalUtil.bounded(p - 4 + 10, s - 4)
    case _ => DoubleType
  }
}