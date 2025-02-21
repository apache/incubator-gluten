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

import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode, IfThenNode}

import org.apache.spark.sql.catalyst.expressions._

import java.util.{ArrayList => JArrayList}

/** A version of substring that supports columnar processing for utf8. */
case class CaseWhenTransformer(
    substraitExprName: String,
    branches: Seq[(ExpressionTransformer, ExpressionTransformer)],
    elseValue: Option[ExpressionTransformer],
    original: CaseWhen)
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] =
    branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue

  override def doTransform(context: SubstraitContext): ExpressionNode = {
    // generate branches nodes
    val ifNodes = new JArrayList[ExpressionNode]
    val thenNodes = new JArrayList[ExpressionNode]
    branches.foreach(
      branch => {
        ifNodes.add(branch._1.doTransform(context))
        thenNodes.add(branch._2.doTransform(context))
      })
    val branchDataType = original.asInstanceOf[CaseWhen].inputTypesForMerging(0)
    // generate else value node, maybe null
    val elseValueNode = elseValue
      .map(_.doTransform(context))
      .getOrElse(ExpressionBuilder.makeLiteral(null, branchDataType, true))
    new IfThenNode(ifNodes, thenNodes, elseValueNode)
  }
}

case class IfTransformer(
    substraitExprName: String,
    predicate: ExpressionTransformer,
    trueValue: ExpressionTransformer,
    falseValue: ExpressionTransformer,
    original: If)
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = predicate :: trueValue :: falseValue :: Nil

  override def doTransform(context: SubstraitContext): ExpressionNode = {
    val ifNodes = new JArrayList[ExpressionNode]
    ifNodes.add(predicate.doTransform(context))

    val thenNodes = new JArrayList[ExpressionNode]
    thenNodes.add(trueValue.doTransform(context))

    val elseValueNode = falseValue.doTransform(context)
    new IfThenNode(ifNodes, thenNodes, elseValueNode)
  }
}
