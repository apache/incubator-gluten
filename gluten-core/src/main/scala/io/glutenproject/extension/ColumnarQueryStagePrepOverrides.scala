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

package io.glutenproject.extension

import io.glutenproject.{GlutenConfig, GlutenSparkExtensionsInjector}
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.extension.columnar.TransformHints
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

import io.glutenproject.extension.columnar.TransformHint.TRANSFORM_SUPPORTED
import io.glutenproject.extension.columnar.TransformHint.TRANSFORM_UNSUPPORTED
import io.glutenproject.extension.columnar.TransformHint.TransformHint

// e.g. BroadcastHashJoinExec and it's child BroadcastExec will be cut into different QueryStages,
// so the columnar rules will be applied to the two QueryStages separately, and they cannot
// see each other during transformation. In order to prevent BroadcastExec being transformed
// to columnar while BHJ fallbacks, we can add RowGuardTag to BroadcastExec when applying
// queryStagePrepRules and check the tag when applying columnarRules.
// RowGuardTag will be ignored if the plan is already guarded by RowGuard.
case class ColumnarQueryStagePrepRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val columnarConf: GlutenConfig = GlutenConfig.getSessionConf
    plan.transformDown {
      case bhj: BroadcastHashJoinExec =>
        if (columnarConf.enableColumnarBroadcastExchange &&
          columnarConf.enableColumnarBroadcastJoin) {
          val transformer = BackendsApiManager.getSparkPlanExecApiInstance
            .genBroadcastHashJoinExecTransformer(
              bhj.leftKeys,
              bhj.rightKeys,
              bhj.joinType,
              bhj.buildSide,
              bhj.condition,
              bhj.left,
              bhj.right,
              bhj.isNullAwareAntiJoin)

          val isTransformable = transformer.doValidate()
          TransformHints.tag(bhj, isTransformable.toTransformHint)
          if (!isTransformable) {
            bhj.children.foreach {
              // ResuedExchange is not created yet, so we don't need to handle that case.
              case be: BroadcastExchangeExec =>
                TransformHints.tagNotTransformable(be)
              case _ =>
            }
          }
        }
        bhj
      case plan => plan
    }
  }

  implicit class EncodeTransformableTagImplicits(transformable: Boolean) {
    def toTransformHint: TransformHint = {
      transformable match {
        case true => TRANSFORM_SUPPORTED
        case false => TRANSFORM_UNSUPPORTED
      }
    }
  }
}

object ColumnarQueryStagePrepOverrides extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectQueryStagePrepRule(ColumnarQueryStagePrepRule)
  }
}

