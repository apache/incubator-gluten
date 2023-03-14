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
package io.substrait.spark.workaround

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeMap, AttributeReference, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.rules.Rule

object RemoveColumn extends Rule[LogicalPlan] with Logging {
  protected def getAliasMap(exprs: Seq[NamedExpression]): AttributeMap[Alias] = {
    AttributeMap(exprs.collect { case al @ Alias(a: AttributeReference, _) => (a, al) })
  }
  def apply(p: LogicalPlan): LogicalPlan = p.transformDown {
    case s @ Sort(_, _, Project(projectList, _)) =>
      val x = getAliasMap(projectList)
      val newOrder = s.order.map {
        case order @ SortOrder(a: AttributeReference, _, _, _) if x.contains(a) =>
          order.copy(child = x.get(a).get.toAttribute)
        case other => other
      }
      if (newOrder.zip(s.order).exists(x => x._1 != x._2)) {
        s.copy(order = newOrder)
      } else {
        s
      }
  }
}
