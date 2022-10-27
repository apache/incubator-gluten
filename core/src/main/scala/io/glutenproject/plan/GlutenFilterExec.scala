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
package io.glutenproject.plan

import io.substrait.spark.ExpressionConverter

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}

import io.substrait.relation.Filter

case class GlutenFilterExec(filterExec: FilterExec, override val child: GlutenPlan)
  extends SingleRel[Filter] {

  override protected def withNewChildInternal(newChild: SparkPlan): GlutenFilterExec = {
    require(newChild.isInstanceOf[GlutenPlan])
    copy(child = newChild.asInstanceOf[GlutenPlan])
  }
  override def output: Seq[Attribute] = filterExec.output

  override def convert: Filter = {
    val exp = ExpressionConverter.defaultConverter(filterExec.condition, child.output)
    Filter.builder().condition(exp).input(substraitChild.convert).build()
  }
}
