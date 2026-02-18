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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.backendsapi.bolt.BoltBackend
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{AbstractPaimonScanTransformer, BasicScanExecTransformer}
import org.apache.gluten.extension.columnar.enumerated.RasOffload
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.validator.Validators
import org.apache.gluten.extension.injector.Injector
import org.apache.gluten.proto.PaimonTableEnhancement
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import scala.collection.JavaConverters._

class BoltPaimonComponent extends Component {
  override def name(): String = "bolt-paimon"
  override def buildInfo(): Component.BuildInfo =
    Component.BuildInfo("BoltPaimon", "N/A", "N/A", "N/A")
  override def dependencies(): Seq[Class[_ <: Component]] = classOf[BoltBackend] :: Nil
  override def injectRules(injector: Injector): Unit = {
    injector.gluten.legacy.injectTransform {
      c =>
        val offload = Seq(BoltOffloadPaimonScan())
        HeuristicTransform.Simple(
          Validators.newValidator(new GlutenConfig(c.sqlConf), offload),
          offload
        )
    }

    // Inject RAS rule.
    injector.gluten.ras.injectRasRule {
      c =>
        RasOffload.Rule(
          RasOffload.from[BatchScanExec](BoltOffloadPaimonScan()),
          Validators.newValidator(new GlutenConfig(c.sqlConf)),
          Nil)
    }
  }
}

case class BoltPaimonScanTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    @transient override val table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None)
  extends AbstractPaimonScanTransformer(
    output = output,
    scan = scan,
    runtimeFilters = runtimeFilters,
    table = table,
    keyGroupedPartitioning = keyGroupedPartitioning,
    commonPartitionValues = commonPartitionValues
  )
  with BasicScanExecTransformer {

  override def doCanonicalize(): AbstractPaimonScanTransformer = {
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output)
    )
  }

  override def getAdvancedExtension: Option[AdvancedExtensionNode] = {
    val tableEnhancement = PaimonTableEnhancement
      .newBuilder()
      .putAllTableProperties(tableProperties.asJava)
      .build

    Some(
      ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(tableEnhancement)))
  }
}

object BoltPaimonScanTransformer {
  def apply(batchScan: BatchScanExec): BoltPaimonScanTransformer = {
    BoltPaimonScanTransformer(
      batchScan.output,
      batchScan.scan,
      batchScan.runtimeFilters,
      table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScan),
      keyGroupedPartitioning = SparkShimLoader.getSparkShims.getKeyGroupedPartitioning(batchScan),
      commonPartitionValues = SparkShimLoader.getSparkShims.getCommonPartitionValues(batchScan)
    )
  }
}

case class BoltOffloadPaimonScan() extends OffloadSingleNode {
  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: BatchScanExec if AbstractPaimonScanTransformer.supportsBatchScan(scan.scan).ok() =>
      BoltPaimonScanTransformer(scan)
    case other => other
  }
}
