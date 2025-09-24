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
package org.apache.gluten.extension.columnar

import org.apache.gluten.execution.ColumnarV2TableWriteExec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

case class V2WritePostRule() extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case write: ColumnarV2TableWriteExec =>
      /**
       * If the columnar write's child is aqe, we make aqe "support columnar", then aqe itself will
       * guarantee to generate columnar outputs. thus avoiding the case of c2r->aqe->r2c->writer.
       */
      write.query match {
        case aqe: AdaptiveSparkPlanExec if !aqe.supportsColumnar =>
          write.withNewQuery(aqe.copy(supportsColumnar = true))
        case _ => write
      }
    case other => other
  }
}
