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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

// Similar with ToPrettyString Expression
// Because case class ToPrettyString is forbid to inheritable by code style
case class ToStringUtil(inputType: DataType, timeZoneId: Option[String] = None)
  extends UnaryExpression
  with TimeZoneAwareExpression
  with ToStringBase {

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  override def withTimeZone(timeZoneId: String): ToStringUtil =
    copy(timeZoneId = Some(timeZoneId))

  override protected def withNewChildInternal(newChild: Expression): Expression =
    throw new UnsupportedOperationException()

  override protected def leftBracket: String = "{"
  override protected def rightBracket: String = "}"

  override protected def nullString: String = "NULL"

  override protected def useDecimalPlainString: Boolean = true

  override protected def useHexFormatForBinary: Boolean = true

  private[this] lazy val castFunc: Any => Any = castToString(inputType)

  override def eval(input: InternalRow): Any = {
    throw new UnsupportedOperationException()
  }

  def evalValue(input: Any): Any = {
    if (input == null) UTF8String.fromString(nullString) else castFunc(input)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw new UnsupportedOperationException()
  }

  override def child: Expression = null
}
