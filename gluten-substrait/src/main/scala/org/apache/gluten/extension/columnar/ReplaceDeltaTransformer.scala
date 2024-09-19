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

import org.apache.gluten.execution.{FilterTransformerFactory, ProjectTransformerFactory}

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}

case class ReplaceDeltaTransformer() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case node =>
        applyDeltaTransformer(node)
    }
  }

  def applyDeltaTransformer(plan: SparkPlan): SparkPlan = plan match {
    case p: ProjectExec =>
      val transformer = ProjectTransformerFactory.createProjectTransformer(p)
      val validationResult = transformer.doValidate()
      if (validationResult.ok()) {
        logDebug(s"Columnar Processing for ${p.getClass} is currently supported.")
        transformer
      } else {
        logDebug(s"Columnar Processing for ${p.getClass} is currently unsupported.")
        FallbackTags.add(p, validationResult.reason())
        p
      }
    case f: FilterExec =>
      val transformer = FilterTransformerFactory.createFilterTransformer(f)
      val validationResult = transformer.doValidate()
      if (validationResult.ok()) {
        logDebug(s"Columnar Processing for ${f.getClass} is currently supported.")
        transformer
      } else {
        logDebug(s"Columnar Processing for ${f.getClass} is currently unsupported.")
        FallbackTags.add(f, validationResult.reason())
        f
      }
    case other => other
  }
}
