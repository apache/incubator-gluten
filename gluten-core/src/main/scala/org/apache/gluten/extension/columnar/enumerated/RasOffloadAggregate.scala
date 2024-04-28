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
package org.apache.gluten.extension.columnar.enumerated

import org.apache.gluten.execution.HashAggregateExecBaseTransformer
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

object RasOffloadAggregate extends RasRule[SparkPlan] {
  override def shift(node: SparkPlan): Iterable[SparkPlan] = node match {
    case agg: HashAggregateExec => shiftAgg(agg)
    case _ => List.empty
  }

  private def shiftAgg(agg: HashAggregateExec): Iterable[SparkPlan] = {
    if (!HashAggregateExecBaseTransformer.canOffload(agg)) {
      return List.empty
    }
    val transformer = offload(agg)
    if (!transformer.doValidate().isValid) {
      return List.empty
    }
    List(transformer)
  }

  private def offload(agg: HashAggregateExec): HashAggregateExecBaseTransformer = {
    HashAggregateExecBaseTransformer.from(agg)()
  }

  override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
}
