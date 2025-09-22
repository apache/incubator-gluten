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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{VeloxIcebergAppendDataExec, VeloxIcebergOverwriteByExpressionExec, VeloxIcebergReplaceDataExec}
import org.apache.gluten.extension.columnar.enumerated.RasOffload
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec, ReplaceDataExec}

case class OffloadIcebergWrite() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case a: AppendDataExec =>
      VeloxIcebergAppendDataExec(a)
    case other => other
  }
}

case class OffloadIcebergDelete() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case r: ReplaceDataExec =>
      VeloxIcebergReplaceDataExec(r)
    case other => other
  }
}

case class OffloadIcebergOverwrite() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case r: OverwriteByExpressionExec =>
      VeloxIcebergOverwriteByExpressionExec(r)
    case other => other
  }
}

object OffloadIcebergWrite {
  def inject(injector: Injector): Unit = {
    // Inject legacy rule.
    injector.gluten.legacy.injectTransform {
      c =>
        val offload = Seq(OffloadIcebergWrite(), OffloadIcebergDelete(), OffloadIcebergOverwrite())
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload
        )
    }

    val offloads: Seq[RasOffload] = Seq(
      RasOffload.from[AppendDataExec](OffloadIcebergWrite()),
      RasOffload.from[ReplaceDataExec](OffloadIcebergDelete()),
      RasOffload.from[OverwriteByExpressionExec](OffloadIcebergOverwrite())
    )
    offloads.foreach(
      offload =>
        injector.gluten.ras.injectRasRule(
          c => RasOffload.Rule(offload, Validators.newValidator(new GlutenConfig(c.sqlConf)), Nil)))
  }
}
