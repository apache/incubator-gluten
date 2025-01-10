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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

import java.util

/**
 * Collapse nested expressions for optimization, to reduce expression calls. Now support `and`,
 * `or`. e.g. select ... and(and(a=1, b=2), c=3) => select ... and(a=1, b=2, c=3).
 */
case class CHCollapseNestedExpressionsTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: Object): ExpressionNode = {
    if (canBeOptimized(original)) {
      val functionMap = args.asInstanceOf[util.HashMap[String, java.lang.Long]]
      val newExprNode = doTransform(original, children, functionMap)
      logDebug("The new expression node: " + newExprNode.toProtobuf)
      newExprNode
    } else {
      super.doTransform(args)
    }
  }

  def getExpressionName(expr: Expression): Option[String] = expr match {
    case _: And => ExpressionMappings.expressionsMap.get(classOf[And])
    case _: Or => ExpressionMappings.expressionsMap.get(classOf[Or])
    case _ => Option.empty[String]
  }

  private def canBeOptimized(expr: Expression): Boolean = {
    var exprCall = expr
    expr match {
      case a: Alias => exprCall = a.child
      case _ =>
    }
    val exprName = getExpressionName(exprCall)
    exprName match {
      case None =>
        exprCall match {
          case _: LeafExpression => false
          case _ => exprCall.children.exists(c => canBeOptimized(c))
        }
      case Some(f) =>
        GlutenConfig.get.getSupportedCollapsedExpressions.split(",").exists(c => c.equals(f))
    }
  }

  private def doTransform0(
      expr: Expression,
      dataType: DataType,
      childNodes: Seq[ExpressionNode],
      childTypes: Seq[DataType],
      functionMap: util.Map[String, java.lang.Long]): ExpressionNode = {
    val funcName: String = ConverterUtils.makeFuncName(substraitExprName, childTypes)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, funcName)
    val childNodeList = new util.ArrayList[ExpressionNode]()
    childNodes.foreach(c => childNodeList.add(c))
    val typeNode = ConverterUtils.getTypeNode(dataType, expr.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childNodeList, typeNode)
  }

  private def doTransform(
      expr: Expression,
      transformers: Seq[ExpressionTransformer],
      functionMap: util.Map[String, java.lang.Long]): ExpressionNode = {

    var dataType = null.asInstanceOf[DataType]
    var children = Seq.empty[ExpressionNode]
    var childTypes = Seq.empty[DataType]

    def f(
        e: Expression,
        ts: ExpressionTransformer = null,
        parent: Option[Expression] = Option.empty): Unit = {
      parent match {
        case None =>
          dataType = e.dataType
          e match {
            case a: And if canBeOptimized(a) =>
              f(a.left, transformers.head, Option.apply(a))
              f(a.right, transformers(1), Option.apply(a))
            case o: Or if canBeOptimized(o) =>
              f(o.left, transformers.head, Option.apply(o))
              f(o.right, transformers(1), Option.apply(o))
            case _ =>
          }
        case Some(_: And) =>
          e match {
            case a: And if canBeOptimized(a) =>
              val childTransformers = ts.children
              f(a.left, childTransformers.head, Option.apply(a))
              f(a.right, childTransformers(1), Option.apply(a))
            case _ =>
              children +:= ts.doTransform(functionMap)
              childTypes +:= e.dataType
          }
        case Some(_: Or) =>
          e match {
            case o: Or if canBeOptimized(o) =>
              val childTransformers = ts.children
              f(o.left, childTransformers.head, Option.apply(o))
              f(o.right, childTransformers(1), Option.apply(o))
            case _ =>
              children +:= ts.doTransform(functionMap)
              childTypes +:= e.dataType
          }
        case _ =>
      }
    }
    f(expr)
    doTransform0(expr, dataType, children, childTypes, functionMap)
  }
}
