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
package org.apache.gluten.expression

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, DecimalType}

case class DecimalRoundTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Round)
  extends BinaryExpressionTransformer {

  val toScale: Int = original.scale.eval(EmptyRow).asInstanceOf[Int]

  // Use the same result type for different Spark versions in velox.
  // The same result type with spark in ch.
  override val dataType: DataType = original.child.dataType match {
    case decimalType: DecimalType =>
      BackendsApiManager.getSparkPlanExecApiInstance.genDecimalRoundExpressionOutput(
        decimalType,
        toScale)
    case _ =>
      throw new GlutenNotSupportException(
        s"Decimal type is expected but received ${original.child.dataType.typeName}.")
  }

  override def left: ExpressionTransformer = child
  override def right: ExpressionTransformer = LiteralTransformer(toScale)
}
