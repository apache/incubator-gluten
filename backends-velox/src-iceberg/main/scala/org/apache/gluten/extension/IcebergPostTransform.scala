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
package org.apache.gluten.extension

import org.apache.gluten.execution.{VeloxIcebergAppendDataExec, VeloxIcebergOverwriteByExpressionExec, VeloxIcebergReplaceDataExec}
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

/**
 * If the columnar write's child is aqe, we make aqe "support columnar", then aqe itself will
 * guarantee to generate columnar outputs. thus avoiding the case of c2r->aqe->r2c->writer.
 */
object EnableColumnarAQEWrite extends Rule[SparkPlan] {

  private def updateIfAQE(query: SparkPlan): SparkPlan = query match {
    case aqe: AdaptiveSparkPlanExec =>
      aqe.copy(supportsColumnar = true)
    case _ =>
      query
  }

  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case a: VeloxIcebergAppendDataExec =>
      a.copy(query = updateIfAQE(a.query))

    case r: VeloxIcebergReplaceDataExec =>
      r.copy(query = updateIfAQE(r.query))

    case o: VeloxIcebergOverwriteByExpressionExec =>
      o.copy(query = updateIfAQE(o.query))

    case other => other
  }
}

object IcebergPostTransform {

  val rules = Seq(EnableColumnarAQEWrite)

  def inject(injector: Injector): Unit = {
    rules.foreach {
      r =>
        injector.gluten.legacy.injectPostTransform(_ => r)
        injector.gluten.ras.injectPostTransform(_ => r)
    }
  }
}
