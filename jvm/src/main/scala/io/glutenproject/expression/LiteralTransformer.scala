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

package io.glutenproject.expression

import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

class LiteralTransformer(lit: Literal)
  extends Literal(lit.value, lit.dataType) with ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    value match {
      // TODO: how to handle null literal in Substrait?
      case null =>
        throw new UnsupportedOperationException(s"$value is not supported.")
      case _ =>
    }
    dataType match {
      case _: IntegerType =>
        ExpressionBuilder.makeIntLiteral(value.asInstanceOf[java.lang.Integer])
      case _: LongType =>
        ExpressionBuilder.makeLongLiteral(value.asInstanceOf[java.lang.Long])
      case _: DoubleType =>
        ExpressionBuilder.makeDoubleLiteral(value.asInstanceOf[java.lang.Double])
      case _: DateType =>
        ExpressionBuilder.makeDateLiteral(value.asInstanceOf[java.lang.Integer])
      case _: StringType =>
        ExpressionBuilder.makeStringLiteral(value.toString)
      case other =>
        throw new UnsupportedOperationException(s"$other is not supported.")
    }
  }
}
