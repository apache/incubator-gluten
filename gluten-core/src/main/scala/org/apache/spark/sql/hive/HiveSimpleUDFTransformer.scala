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

import io.glutenproject.expression.ConverterUtils
import io.glutenproject.expression.ExpressionConverter
import io.glutenproject.expression.ExpressionTransformer
import io.glutenproject.expression.UDFMappings
import io.glutenproject.substrait.expression.ExpressionBuilder
import io.glutenproject.substrait.expression.ExpressionNode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._

import java.util.ArrayList

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
    val substraitExprName = UDFMappings.hiveUDFMap.get(udf.name.stripPrefix("default."))
    substraitExprName match {
      case Some(name) =>
        HiveSimpleUDFTransformer(
          name,
          udf.children.map(ExpressionConverter.replaceWithExpressionTransformer(_, attributeSeq)),
          udf)
      case None =>
        throw new UnsupportedOperationException(s"Not supported hive simple udf: $udf.")
    }
  }
}

case class HiveSimpleUDFTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: HiveSimpleUDF)
  extends ExpressionTransformer
  with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        ConverterUtils.FunctionConfig.OPT))

    val expressionNodes = new ArrayList[ExpressionNode]
    children.foreach(child => expressionNodes.add(child.doTransform(args)))

    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}
