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
import com.intel.oap.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.IntervalUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.DateUnit
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.sql.types.CalendarIntervalType
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class LiteralTransformer(lit: Literal)
    extends Literal(lit.value, lit.dataType)
    with ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    dataType match {
      case t: DoubleType =>
        value match {
          case null =>
            throw new UnsupportedOperationException(s"null is not supported")
          case _ =>
            ExpressionBuilder.makeLiteral(value.asInstanceOf[java.lang.Double])
        }
      case other =>
        throw new UnsupportedOperationException(s"$other is not supported")
    }
  }
}
