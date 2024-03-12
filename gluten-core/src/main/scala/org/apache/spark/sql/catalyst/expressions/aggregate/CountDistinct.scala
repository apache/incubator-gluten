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
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * By default, spark execute count distinct as Expand + Count. This works reliably but may be
 * slower. We allow user to inject a custom count distinct function to speed up the execution. Check
 * the optimizer rule at CountDistinctWithoutExpand
 */
case class CountDistinct(children: Seq[Expression]) extends DeclarativeAggregate {

  override def nullable: Boolean = false

  // Return data type.
  override def dataType: DataType = LongType

  protected lazy val cd = AttributeReference("count_distinct", LongType, nullable = false)()

  override lazy val aggBufferAttributes = cd :: Nil

  override lazy val initialValues =
    throw new UnsupportedOperationException(
      "count distinct does not have non-columnar implementation")

  override lazy val mergeExpressions =
    throw new UnsupportedOperationException(
      "count distinct does not have non-columnar implementation")

  override lazy val evaluateExpression =
    throw new UnsupportedOperationException(
      "count distinct does not have non-columnar implementation")

  override def defaultResult: Option[Literal] = Option(Literal(0L))

  override lazy val updateExpressions =
    throw new UnsupportedOperationException(
      "count distinct does not have non-columnar implementation")

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): CountDistinct =
    copy(children = newChildren)
}
