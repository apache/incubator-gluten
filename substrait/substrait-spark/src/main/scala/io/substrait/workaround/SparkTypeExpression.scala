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
package io.substrait.workaround

import io.substrait.spark.expression.ToSparkExpression

import org.apache.spark.sql.catalyst.expressions.{Expression, Unevaluable}
import org.apache.spark.sql.catalyst.trees.LeafLike
import org.apache.spark.sql.types.DataType

/**
 * It's a workaround to avoid [[ToSparkExpression]] only returns Spark [[Expression]], but
 * [[io.substrait.expression.FunctionArg]] has three derived classes.
 *
 * [[SparkTypeExpression]] simulates [[io.substrait.type.Type]]
 */
case class SparkTypeExpression(dataType: DataType)
  extends Expression
  with Unevaluable
  with LeafLike[Expression] {
  override def nullable: Boolean = false

}
