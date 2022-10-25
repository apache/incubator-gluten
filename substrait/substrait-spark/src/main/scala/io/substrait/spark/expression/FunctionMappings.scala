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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum

import scala.reflect.ClassTag

case class Sig(expClass: Class[_], name: String)

class FunctionMappings {

  private def s[T <: Expression: ClassTag](name: String): Sig =
    Sig(scala.reflect.classTag[T].runtimeClass, name)

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
    s[IsNotNull]("is_not_null")
  )

  val AGGREGATE_SIGS: Seq[Sig] = Seq(
    s[Sum]("sum")
  )

  lazy val scalar_functions_map: Map[Class[_], Sig] = SCALAR_SIGS.map(s => (s.expClass, s)).toMap
  lazy val aggregate_functions_map: Map[Class[_], Sig] =
    AGGREGATE_SIGS.map(s => (s.expClass, s)).toMap
}

object FunctionMappings extends FunctionMappings
