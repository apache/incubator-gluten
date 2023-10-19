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
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, IntegerLiteral}

/**
 * A logical offset, which may removing a specified number of rows from the beginning of the output
 * of child logical plan.
 */
case class Offset(offsetExpr: Expression, child: LogicalPlan) extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output
  override def maxRows: Option[Long] = {
    import scala.math.max
    offsetExpr match {
      case IntegerLiteral(offset) => child.maxRows.map(x => max(x - offset, 0))
      case _ => None
    }
  }
  override protected def withNewChildInternal(newChild: LogicalPlan): Offset =
    copy(child = newChild)
}
