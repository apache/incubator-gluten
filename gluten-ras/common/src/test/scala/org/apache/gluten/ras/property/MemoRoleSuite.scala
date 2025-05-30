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
package org.apache.gluten.ras.property

import org.apache.gluten.ras.{Ras, RasConfig, RasSuite}
import org.apache.gluten.ras.RasSuiteBase.{CostModelImpl, ExplainImpl, MetadataModelImpl, PlanModelImpl, PropertyModelImpl, TestNode}
import org.apache.gluten.ras.rule.RasRule

class MemoRoleSuite extends RasSuite {
  override protected def conf: RasConfig = RasConfig(plannerType = RasConfig.PlannerType.Dp)

  test("equality") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())
        .withNewConfig(_ => conf)
    val one = ras.userConstraintSet()
    val other = ras.withUserConstraint(PropertySet(Nil))
    assert(one == other)
  }
}
