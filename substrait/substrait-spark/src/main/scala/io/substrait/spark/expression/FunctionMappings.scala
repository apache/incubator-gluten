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
package io.substrait.spark.expression

import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.IntegerType

import scala.reflect.ClassTag

case class Sig(expClass: Class[_], name: String, builder: Seq[Expression] => Expression) {
  def makeCall(args: Seq[Expression]): Expression =
    builder(args)
}

class FunctionMappings {
  private def s[T <: Expression: ClassTag](
      name: String,
      builder: Option[Seq[Expression] => Expression] = None): Sig = {
    val b = builder.getOrElse(
      FunctionRegistryBase.build[T](name, None)._2
    )
    Sig(scala.reflect.classTag[T].runtimeClass, name, b)
  }
  private def makeDecimal(args: Seq[Expression]): Expression = {
    require(args.size == 4)
    val precision = args(1).asInstanceOf[Literal].value.asInstanceOf[Int]
    val scale = args(2).asInstanceOf[Literal].value.asInstanceOf[Int]
    val nullOnOverflow = args(3).asInstanceOf[Literal].value.asInstanceOf[Boolean]
    new MakeDecimal(args.head, precision, scale, nullOnOverflow)
  }

  val SCALAR_SIGS: Seq[Sig] = Seq(
    s[Add]("add"),
    s[Subtract]("subtract"),
    s[Multiply]("multiply"),
    s[Divide]("divide"),
    s[And]("and"),
    s[Or]("or"),
    s[Not]("not"),
    s[LessThan]("lt"),
    s[LessThanOrEqual]("lte"),
    s[GreaterThan]("gt"),
    s[GreaterThanOrEqual]("gte"),
    s[EqualTo]("equal"),
    // s[BitwiseXor]("xor"),
    s[IsNull]("is_null"),
    s[IsNotNull]("is_not_null"),
    s[EndsWith]("ends_with"),
    s[Like]("like"),
    s[Contains]("contains"),
    s[StartsWith]("starts_with"),
    s[Substring]("substring"),
    s[Year]("year"),
    s[Concat]("concat"),
    s[Coalesce]("coalesce"),

    // internal
    s[UnscaledValue]("unscaled"),
    s[MakeDecimal]("make_decimal", Some(makeDecimal)),
    s[EqualNullSafe]("equal_nullsafe")
  )

  val AGGREGATE_SIGS: Seq[Sig] = Seq(
    s[Sum]("sum"),
    s[Average]("avg"),
    s[Count]("count"),
    s[Min]("min"),
    s[Max]("max"),
    s[HyperLogLogPlusPlus]("approx_count_distinct")
  )

  lazy val scalar_functions_map: Map[Class[_], Sig] = SCALAR_SIGS.map(s => (s.expClass, s)).toMap
  lazy val aggregate_functions_map: Map[Class[_], Sig] =
    AGGREGATE_SIGS.map(s => (s.expClass, s)).toMap
}

object FunctionMappings extends FunctionMappings
