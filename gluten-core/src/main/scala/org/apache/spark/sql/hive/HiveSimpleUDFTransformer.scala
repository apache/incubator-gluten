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
package org.apache.spark.sql.hive

import io.glutenproject.expression.{ExpressionConverter, ExpressionTransformer, GenericExpressionTransformer, UDFMappings}

import org.apache.spark.sql.catalyst.expressions._

import java.util.Locale

object HiveSimpleUDFTransformer {
  def isHiveSimpleUDF(expr: Expression): Boolean = {
    expr match {
      case _: HiveSimpleUDF => true
      case _ => false
    }
  }

  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    if (!isHiveSimpleUDF(expr)) {
      throw new UnsupportedOperationException(s"Expression $expr is not a HiveSimpleUDF")
    }

    val udf = expr.asInstanceOf[HiveSimpleUDF]
    val substraitExprName =
      UDFMappings.hiveUDFMap.get(udf.name.stripPrefix("default.").toLowerCase(Locale.ROOT))
    substraitExprName match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          udf.children.map(ExpressionConverter.replaceWithExpressionTransformer(_, attributeSeq)),
          udf)
      case _ =>
        throw new UnsupportedOperationException(
          s"Not supported hive simple udf:$udf"
            + s" name:${udf.name} hiveUDFMap:${UDFMappings.hiveUDFMap}")
    }
  }
}
