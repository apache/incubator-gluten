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
package io.glutenproject.execution.extension

import io.glutenproject.expression._
import io.glutenproject.extension.ExpressionExtensionTrait

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

import scala.collection.mutable.ListBuffer

case class CustomAggExpressionTransformer() extends ExpressionExtensionTrait {

  lazy val expressionSigs = Seq(
    Sig[CustomSum]("custom_sum")
  )

  /** Generate the extension expressions list, format: Sig[XXXExpression]("XXXExpressionName") */
  override def expressionSigList: Seq[Sig] = expressionSigs

  /** Get the attribute index of the extension aggregate functions. */
  override def getAttrsForExtensionAggregateExpr(
      aggregateFunc: AggregateFunction,
      mode: AggregateMode,
      exp: AggregateExpression,
      aggregateAttributeList: Seq[Attribute],
      aggregateAttr: ListBuffer[Attribute],
      resIndex: Int): Int = {
    var reIndex = resIndex
    aggregateFunc match {
      case CustomSum(_, _) =>
        mode match {
          case Partial | PartialMerge =>
            val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
            if (aggBufferAttr.size == 2) {
              // decimal sum check sum.resultType
              aggregateAttr += ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
              val isEmptyAttr = ConverterUtils.getAttrFromExpr(aggBufferAttr(1))
              aggregateAttr += isEmptyAttr
              reIndex += 2
              reIndex
            } else {
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr.head)
              aggregateAttr += attr
              reIndex += 1
              reIndex
            }
          case Final =>
            aggregateAttr += aggregateAttributeList(reIndex)
            reIndex += 1
            reIndex
          case other =>
            throw new UnsupportedOperationException(s"Unsupported aggregate mode: $other.")
        }
    }
  }
}
