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

package com.intel.oap.expression

import com.google.common.collect.Lists
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import com.intel.oap.expression.DateTimeExpressionsTransformer.{DateDiffTransformer, UnixTimestampTransformer}
import com.intel.oap.substrait.expression.ExpressionNode

/**
 * A version of add that supports columnar processing for longs.
 */
class DateAddIntervalTransformer(start: Expression, interval: Expression, original: DateAddInterval)
    extends DateAddInterval(start, interval, original.timeZoneId, original.ansiEnabled)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = null
}

object BinaryExpressionTransformer {

  def create(left: Expression, right: Expression, original: Expression): Expression =
    original match {
      case s: DateAddInterval =>
        new DateAddIntervalTransformer(left, right, s)
      case s: DateDiff =>
        new DateDiffTransformer(left, right)
      case a: UnixTimestamp =>
        new UnixTimestampTransformer(left, right)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
}
