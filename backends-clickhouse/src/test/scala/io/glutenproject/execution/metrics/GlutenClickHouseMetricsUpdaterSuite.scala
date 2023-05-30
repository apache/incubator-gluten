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
package io.glutenproject.execution.metrics

import io.glutenproject.execution.{BasicScanExecTransformer, FileSourceScanExecTransformer, FilterExecTransformer, GlutenClickHouseTPCHAbstractSuite, HashAggregateExecBaseTransformer, ProjectExecTransformer, WholeStageTransformerExec}
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.vectorized.GeneralInIterator

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Attribute

class GlutenClickHouseMetricsUpdaterSuite extends GlutenClickHouseTPCHAbstractSuite {

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  protected val metricsJsonFilePath: String = rootPath + "metrics-json"
  protected val substraitPlansDatPath: String = rootPath + "substrait-plans"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createTPCHParquetTables(tablesPath)
  }

  test("TPCH Q1 scan metrics") {
    runTPCHQuery(1) {
      df =>
        val plans = df.queryExecution.executedPlan.collect {
          case scanExec: BasicScanExecTransformer => scanExec
          case hashAggExec: HashAggregateExecBaseTransformer => hashAggExec
        }
        assert(plans.size == 3)

        assert(plans(2).metrics("numFiles").value === 1)
        assert(plans(2).metrics("pruningTime").value === -1)
        assert(plans(2).metrics("filesSize").value === 17777735)

        assert(plans(1).metrics("outputRows").value === 4)
        assert(plans(1).metrics("outputVectors").value === 1)

        // Execute Sort operator, it will read the data twice.
        assert(plans(0).metrics("outputRows").value === 4)
        assert(plans(0).metrics("outputVectors").value === 1)
    }
  }

  test("Check the metrics values") {
    withSQLConf(("spark.gluten.sql.columnar.sort", "false")) {
      runTPCHQuery(1) {
        df =>
          val plans = df.queryExecution.executedPlan.collect {
            case scanExec: BasicScanExecTransformer => scanExec
            case hashAggExec: HashAggregateExecBaseTransformer => hashAggExec
          }
          assert(plans.size == 3)

          assert(plans(2).metrics("numFiles").value === 1)
          assert(plans(2).metrics("pruningTime").value === -1)
          assert(plans(2).metrics("filesSize").value === 17777735)

          assert(plans(1).metrics("outputRows").value === 4)
          assert(plans(1).metrics("outputVectors").value === 1)

          // Execute Sort operator, it will read the data twice.
          assert(plans(0).metrics("outputRows").value === 4)
          assert(plans(0).metrics("outputVectors").value === 1)
      }
    }
  }

  test("test tpch wholestage execute") {
    val inBatchIters = new java.util.ArrayList[GeneralInIterator](0)
    val outputAttributes = new java.util.ArrayList[Attribute](0)
    val nativeMetricsList = GlutenClickHouseMetricsUTUtils
      .executeSubstraitPlan(
        substraitPlansDatPath + "/tpch-q4-wholestage-2.json",
        basePath,
        inBatchIters,
        outputAttributes
      )

    assert(nativeMetricsList.size == 1)
    val nativeMetricsData = nativeMetricsList(0)
    assert(nativeMetricsData.metricsDataList.size() == 3)

    assert(nativeMetricsData.metricsDataList.get(0).getName.equals("kRead"))
    assert(
      nativeMetricsData.metricsDataList
        .get(0)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 600572)

    assert(nativeMetricsData.metricsDataList.get(1).getName.equals("kFilter"))
    assert(
      nativeMetricsData.metricsDataList
        .get(1)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getInputRows == 600572)
    assert(
      nativeMetricsData.metricsDataList
        .get(1)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 379809)

    assert(nativeMetricsData.metricsDataList.get(2).getName.equals("kProject"))
    assert(
      nativeMetricsData.metricsDataList
        .get(2)
        .getSteps
        .get(0)
        .getProcessors
        .get(0)
        .getOutputRows == 379809)
  }

  test("Check TPCH Q2 metrics updater") {
    val q2Df = GlutenClickHouseMetricsUTUtils
      .getTPCHQueryExecution(spark, 2, tpchQueries)
    val allWholeStageTransformers = q2Df.queryExecution.executedPlan.collect {
      case wholeStage: WholeStageTransformerExec => wholeStage
    }

    assert(allWholeStageTransformers.size == 10)

    val wholeStageTransformerExec0 = allWholeStageTransformers(2)

    GlutenClickHouseMetricsUTUtils.executeMetricsUpdater(
      wholeStageTransformerExec0,
      metricsJsonFilePath + "/tpch-q2-wholestage-1-metrics.json"
    ) {
      () =>
        wholeStageTransformerExec0.collect {
          case s: FileSourceScanExecTransformer =>
            assert(s.metrics("scanTime").value == 2)
            assert(s.metrics("inputWaitTime").value == 4)
            assert(s.metrics("outputWaitTime").value == 2)
            assert(s.metrics("outputRows").value == 20000)
            assert(s.metrics("outputBytes").value == 1451663)
          case f: FilterExecTransformer =>
            assert(f.metrics("totalTime").value == 3)
            assert(f.metrics("inputWaitTime").value == 14)
            assert(f.metrics("outputWaitTime").value == 1)
            assert(f.metrics("outputRows").value == 73)
            assert(f.metrics("outputBytes").value == 5304)
            assert(f.metrics("inputRows").value == 20000)
            assert(f.metrics("inputBytes").value == 1451663)
            assert(f.metrics("extraTime").value == 1)
          case p: ProjectExecTransformer =>
            assert(p.metrics("totalTime").value == 0)
            assert(p.metrics("inputWaitTime").value == 7)
            assert(p.metrics("outputWaitTime").value == 0)
            assert(p.metrics("outputRows").value == 73)
            assert(p.metrics("outputBytes").value == 2336)
            assert(p.metrics("inputRows").value == 73)
            assert(p.metrics("inputBytes").value == 5085)
        }
    }

    val wholeStageTransformerExec1 = allWholeStageTransformers(1)

    GlutenClickHouseMetricsUTUtils.executeMetricsUpdater(
      wholeStageTransformerExec1,
      metricsJsonFilePath + "/tpch-q2-wholestage-2-metrics.json"
    ) {
      () =>
        val allGlutenPlans = wholeStageTransformerExec1.collect { case g: GlutenPlan => g }

        val scanPlan = allGlutenPlans(10)
        assert(scanPlan.metrics("scanTime").value == 2)
        assert(scanPlan.metrics("inputWaitTime").value == 3)
        assert(scanPlan.metrics("outputWaitTime").value == 1)
        assert(scanPlan.metrics("outputRows").value == 80000)
        assert(scanPlan.metrics("outputBytes").value == 2160000)

        val filterPlan = allGlutenPlans(9)
        assert(filterPlan.metrics("totalTime").value == 1)
        assert(filterPlan.metrics("inputWaitTime").value == 13)
        assert(filterPlan.metrics("outputWaitTime").value == 1)
        assert(filterPlan.metrics("outputRows").value == 80000)
        assert(filterPlan.metrics("outputBytes").value == 2160000)
        assert(filterPlan.metrics("inputRows").value == 80000)
        assert(filterPlan.metrics("inputBytes").value == 2160000)

        val joinPlan = allGlutenPlans(3)
        assert(joinPlan.metrics("totalTime").value == 1)
        assert(joinPlan.metrics("inputWaitTime").value == 6)
        assert(joinPlan.metrics("outputWaitTime").value == 0)
        assert(joinPlan.metrics("outputRows").value == 292)
        assert(joinPlan.metrics("outputBytes").value == 16644)
        assert(joinPlan.metrics("inputRows").value == 80000)
        assert(joinPlan.metrics("inputBytes").value == 1920000)
    }

    val wholeStageTransformerExec2 = allWholeStageTransformers(0)

    GlutenClickHouseMetricsUTUtils.executeMetricsUpdater(
      wholeStageTransformerExec2,
      metricsJsonFilePath + "/tpch-q2-wholestage-11-metrics.json"
    ) {
      () =>
        val allGlutenPlans = wholeStageTransformerExec2.collect { case g: GlutenPlan => g }

        assert(allGlutenPlans.size == 61)

        val shjPlan = allGlutenPlans(8)
        assert(shjPlan.metrics("totalTime").value == 7)
        assert(shjPlan.metrics("inputWaitTime").value == 5)
        assert(shjPlan.metrics("outputWaitTime").value == 0)
        assert(shjPlan.metrics("outputRows").value == 44)
        assert(shjPlan.metrics("outputBytes").value == 3740)
        assert(shjPlan.metrics("inputRows").value == 11985)
        assert(shjPlan.metrics("inputBytes").value == 299625)
    }
  }
}
