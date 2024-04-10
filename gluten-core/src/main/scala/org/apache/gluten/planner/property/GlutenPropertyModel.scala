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
package org.apache.gluten.planner.property

import org.apache.gluten.ras._

import org.apache.spark.sql.execution._

object GlutenPropertyModel {

  def apply(): PropertyModel[SparkPlan] = {
    PropertyModelImpl
  }

  private object PropertyModelImpl extends PropertyModel[SparkPlan] {
    override def propertyDefs: Seq[PropertyDef[SparkPlan, _ <: Property[SparkPlan]]] =
      Seq(ConventionDef)

    override def newEnforcerRuleFactory(
        propertyDef: PropertyDef[SparkPlan, _ <: Property[SparkPlan]])
        : EnforcerRuleFactory[SparkPlan] = (reqProp: Property[SparkPlan]) => {
      propertyDef match {
        case ConventionDef =>
          Seq(ConventionEnforcerRule(reqProp.asInstanceOf[Convention]))
      }
    }
  }
}
