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

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.cudf.VeloxCudfPlanValidatorJniWrapper
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.extension.CudfNodeValidationRule.{createGPUColumnarExchange, setTagForWholeStageTransformer}

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, GPUColumnarShuffleExchangeExec, SparkPlan}

// Add the node name prefix 'Cudf' to GlutenPlan when can offload to cudf
case class CudfNodeValidationRule(glutenConf: GlutenConfig) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConf.enableColumnarCudf) {
      return plan
    }
    val transformedPlan = plan.transformUp {
      case shuffle @ ColumnarShuffleExchangeExec(
            _,
            VeloxResizeBatchesExec(w: WholeStageTransformer, _, _),
            _,
            _,
            _) =>
        setTagForWholeStageTransformer(w)
        createGPUColumnarExchange(shuffle)
      case shuffle @ ColumnarShuffleExchangeExec(_, w: WholeStageTransformer, _, _, _) =>
        setTagForWholeStageTransformer(w)
        createGPUColumnarExchange(shuffle)
      case transformer: WholeStageTransformer =>
        setTagForWholeStageTransformer(transformer)
        transformer
    }
    transformedPlan
  }
}

object CudfNodeValidationRule {
  def setTagForWholeStageTransformer(transformer: WholeStageTransformer): Unit = {
    if (!VeloxConfig.get.cudfEnableTableScan) {
      // Spark3.2 does not have exists
      val hasLeaf = transformer.find {
        case _: LeafTransformSupport => true
        case _ => false
      }.isDefined
      if (!hasLeaf && VeloxConfig.get.cudfEnableValidation) {
        if (
          VeloxCudfPlanValidatorJniWrapper.validate(
            transformer.substraitPlan.toProtobuf.toByteArray)
        ) {
          transformer.foreach {
            case _: LeafTransformSupport =>
            case t: TransformSupport =>
              t.setTagValue(CudfTag.CudfTag, true)
            case _ =>
          }
          transformer.setTagValue(CudfTag.CudfTag, true)
        }
      } else {
        transformer.setTagValue(CudfTag.CudfTag, !hasLeaf)
      }
    } else {
      transformer.setTagValue(CudfTag.CudfTag, true)
    }
  }

  def createGPUColumnarExchange(shuffle: ColumnarShuffleExchangeExec): SparkPlan = {
    val exec = GPUColumnarShuffleExchangeExec(
      shuffle.outputPartitioning,
      shuffle.child,
      shuffle.shuffleOrigin,
      shuffle.projectOutputAttributes,
      shuffle.advisoryPartitionSize)
    val res = exec.doValidate()
    if (!res.ok()) {
      throw new GlutenNotSupportException(res.reason())
    }
    exec
  }
}
