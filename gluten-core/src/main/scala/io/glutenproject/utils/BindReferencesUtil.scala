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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BindReferences, BoundReference, Expression, NamedExpression}

object BindReferencesUtil {

  // get the nullable attr from the attributes of child
  def bindReferencesWithNullable(expr: NamedExpression,
                                 attributeSeq: Seq[Attribute]): Attribute = expr match {
    case a: AttributeReference =>
      val boundRef = BindReferences.bindReference[Expression](expr, attributeSeq, true)
      if (boundRef.isInstanceOf[BoundReference]) {
        val b = boundRef.asInstanceOf[BoundReference]
        expr.toAttribute.withNullability(b.nullable)
      } else {
        expr.toAttribute
      }
    case _ =>
      expr.toAttribute
  }

  def bindReferencesWithNullable(exprs: Seq[NamedExpression],
                                 attributeSeq: Seq[Attribute]): Seq[Attribute] = {
    exprs.map(BindReferencesUtil.bindReferencesWithNullable(_, attributeSeq))
  }
}
