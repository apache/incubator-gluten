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
package org.apache.gluten.planner.cost

import org.apache.gluten.GlutenConfig
import org.apache.gluten.ras.CostModel

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.utils.ReflectionUtil

object GlutenCostModel extends Logging {
  def find(): CostModel[SparkPlan] = {
    val aliases: Map[String, Class[_ <: CostModel[SparkPlan]]] =
      Map(
        "legacy" -> classOf[LegacyCostModel],
        "rough" -> classOf[RoughCostModel],
        "rough2" -> classOf[RoughCostModel2])
    val aliasOrClass = GlutenConfig.getConf.rasCostModel
    val clazz: Class[_ <: CostModel[SparkPlan]] = if (aliases.contains(aliasOrClass)) {
      aliases(aliasOrClass)
    } else {
      val userModel = ReflectionUtil.classForName(aliasOrClass)
      logInfo(s"Using user cost model: $aliasOrClass")
      userModel
    }
    val ctor = clazz.getDeclaredConstructor()
    ctor.setAccessible(true)
    val model = ctor.newInstance()
    model
  }

  def legacy(): CostModel[SparkPlan] = new LegacyCostModel()
}
