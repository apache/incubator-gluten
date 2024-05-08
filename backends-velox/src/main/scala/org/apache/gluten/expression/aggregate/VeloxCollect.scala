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
package org.apache.gluten.expression.aggregate

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{ArrayType, DataType}

abstract class VeloxCollect extends DeclarativeAggregate with UnaryLike[Expression] {

  override def defaultResult: Option[Literal] = Option(Literal.create(Array(), dataType))

  override lazy val initialValues: Seq[Expression] = {
    throw new UnsupportedOperationException("Not yet implemented")
  }
  override lazy val updateExpressions: Seq[Expression] = {
    throw new UnsupportedOperationException("Not yet implemented")
  }
  override lazy val mergeExpressions: Seq[Expression] = {
    throw new UnsupportedOperationException("Not yet implemented")
  }
  override lazy val evaluateExpression: Expression = {
    throw new UnsupportedOperationException("Not yet implemented")
  }
}

case class VeloxCollectSet(override val child: Expression) extends VeloxCollect {
  private lazy val set = AttributeReference("set", dataType)()

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(child.dataType, false)

  override def aggBufferAttributes: Seq[AttributeReference] = List(set)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def prettyName: String = "velox_collect_set"
}

case class VeloxCollectList(override val child: Expression) extends VeloxCollect {
  private lazy val list = AttributeReference("list", dataType)()

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(child.dataType, false)

  override def aggBufferAttributes: Seq[AttributeReference] = List(list)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def prettyName: String = "velox_collect_list"
}
