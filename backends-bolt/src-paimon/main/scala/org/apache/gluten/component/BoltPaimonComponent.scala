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
package org.apache.gluten.component

import org.apache.gluten.backendsapi.bolt.BoltBackend
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.OffloadPaimonScan
import org.apache.gluten.extension.columnar.enumerated.RasOffload
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

class BoltPaimonComponent extends Component {
  override def name(): String = "bolt-paimon"
  override def buildInfo(): Component.BuildInfo =
    Component.BuildInfo("BoltPaimon", "N/A", "N/A", "N/A")
  override def dependencies(): Seq[Class[_ <: Component]] = classOf[BoltBackend] :: Nil
  override def injectRules(injector: Injector): Unit = {
    injector.gluten.legacy.injectTransform {
      c =>
        val offload = Seq(OffloadPaimonScan())
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload
        )
    }

    // Inject RAS rule.
    injector.gluten.ras.injectRasRule {
      c =>
        RasOffload.Rule(
          RasOffload.from[BatchScanExec](OffloadPaimonScan()),
          Validators.newValidator(new GlutenConfig(c.sqlConf)),
          Nil)
    }
  }
}
