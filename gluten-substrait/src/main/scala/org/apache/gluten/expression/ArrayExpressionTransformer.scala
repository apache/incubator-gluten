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

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.substrait.expression.ExpressionNode

import org.apache.spark.sql.catalyst.expressions._

case class CreateArrayTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: CreateArray)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // If children is empty,
    // transformation is only supported when useStringTypeWhenEmpty is false
    // because ClickHouse and Velox currently doesn't support this config.
    if (original.useStringTypeWhenEmpty && children.isEmpty) {
      throw new GlutenNotSupportException(s"$original not supported yet.")
    }

    super.doTransform(args)
  }
}
