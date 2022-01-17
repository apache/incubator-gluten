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

package com.intel.oap.expression

import com.google.common.collect.Lists
import com.intel.oap.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.expression._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

class AliasTransformer(child: Expression, name: String)(
    override val exprId: ExprId,
    override val qualifier: Seq[String],
    override val explicitMetadata: Option[Metadata])
    extends Alias(child, name)(exprId, qualifier, explicitMetadata)
    with ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val child_node = child.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!child_node.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet")
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, "ALIAS")
    val expressNodes = Lists.newArrayList(child_node.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(child.dataType, name, child.nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class AttributeReferenceTransformer(
    name: String,
    ordinal: Int,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    override val exprId: ExprId,
    override val qualifier: Seq[String])
    extends AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
    with ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    ExpressionBuilder.makeSelection(ordinal.asInstanceOf[java.lang.Integer])
  }
}
