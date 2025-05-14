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

import org.apache.gluten.cudf.VeloxCudfPlanValidatorJniWrapper
import org.apache.gluten.execution.{TransformSupport, WholeStageTransformer}

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

// Add the node name prefix 'Cudf' when can offload to cudf
case class CudfNodeValidationRule() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case transformer: WholeStageTransformer =>
        if (
          VeloxCudfPlanValidatorJniWrapper.validate(
            transformer.substraitPlan.toProtobuf.toByteArray)
        ) {
          transformer.foreach(p => p.asInstanceOf[TransformSupport].setIsCudf)
        }
        print("cudf transformer plan " + transformer.toString())
        transformer
    }
  }
}
