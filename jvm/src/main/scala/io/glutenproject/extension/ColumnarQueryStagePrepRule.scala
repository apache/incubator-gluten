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
import io.glutenproject.execution.BroadcastHashJoinExecTransformer

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

// RowGuardTag is useful to transform the plan and add guard tag before creating new QueryStages.
//
// e.g. BroadcastHashJoinExec and it's child BroadcastExec will be cut into different QueryStages,
// so the columnar rules will be applied to the two QueryStages separately, and they cannot
// see each other during transformation. In order to prevent BroadcastExec being transformed
// to columnar while BHJ fallbacks, we can add RowGuardTag to BroadcastExec when applying
// queryStagePrepRules and check the tag when applying columnarRules.
// RowGuardTag will be ignored if the plan is already guarded by RowGuard.
object RowGuardTag {
  val key: TreeNodeTag[Boolean] = TreeNodeTag[Boolean]("RowGuard")
  val value: Boolean = true
}

case class ColumnarQueryStagePrepRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val columnarConf: GlutenConfig = GlutenConfig.getSessionConf
    plan.transformDown {
      case bhj: BroadcastHashJoinExec =>
        if (columnarConf.enableColumnarBroadcastExchange &&
          columnarConf.enableColumnarBroadcastJoin) {
          val transformer = BroadcastHashJoinExecTransformer(
            bhj.leftKeys,
            bhj.rightKeys,
            bhj.joinType,
            bhj.buildSide,
            bhj.condition,
            bhj.left,
            bhj.right,
            bhj.isNullAwareAntiJoin)
          if (!transformer.doValidate()) {
            bhj.children.map {
              // ResuedExchange is not created yet, so we don't need to handle that case.
              case e: BroadcastExchangeExec => AddRowGuardTag(e)
              case plan => plan
            }
          }
        }
        bhj
      case plan => plan
    }
  }

  def AddRowGuardTag(plan: SparkPlan): SparkPlan = {
    plan.setTagValue(RowGuardTag.key, RowGuardTag.value)
    plan
  }
}

object ColumnarQueryStagePreparations extends GlutenSparkExtensionsInjector {
  override def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectQueryStagePrepRule(ColumnarQueryStagePrepRule)
  }
}

