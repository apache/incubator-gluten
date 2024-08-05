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
package org.apache.gluten.integration.ds

import org.apache.gluten.integration.action.Action
import org.apache.gluten.integration.ds.TpcdsSuite.{ALL_QUERY_IDS, HISTORY_WRITE_PATH, TPCDS_WRITE_RELATIVE_PATH}
import org.apache.gluten.integration.{DataGen, Suite, TableCreator, TypeModifier}
import org.apache.log4j.Level
import org.apache.spark.SparkConf

import java.io.File

class TpcdsSuite(
    val masterUrl: String,
    val actions: Array[Action],
    val testConf: SparkConf,
    val baselineConf: SparkConf,
    val extraSparkConf: Map[String, String],
    val logLevel: Level,
    val errorOnMemLeak: Boolean,
    val dataDir: String,
    val enableUi: Boolean,
    val enableHsUi: Boolean,
    val hsUiPort: Int,
    val disableAqe: Boolean,
    val disableBhj: Boolean,
    val disableWscg: Boolean,
    val shufflePartitions: Int,
    val scanPartitions: Int)
    extends Suite(
      masterUrl,
      actions,
      testConf,
      baselineConf,
      extraSparkConf,
      logLevel,
      errorOnMemLeak,
      enableUi,
      enableHsUi,
      hsUiPort,
      disableAqe,
      disableBhj,
      disableWscg,
      shufflePartitions,
      scanPartitions) {

  override protected def historyWritePath(): String = HISTORY_WRITE_PATH

  override private[integration] def dataWritePath(
      scale: Double,
      genPartitionedData: Boolean): String =
    new File(dataDir).toPath.resolve(TPCDS_WRITE_RELATIVE_PATH + s"-$scale").toFile.getAbsolutePath

  override private[integration] def createDataGen(
      scale: Double,
      genPartitionedData: Boolean): DataGen =
    new TpcdsDataGen(
      sessionSwitcher.spark(),
      scale,
      shufflePartitions,
      dataWritePath(scale, genPartitionedData),
      typeModifiers(),
      genPartitionedData)

  override private[integration] def queryResource(): String = {
    "/tpcds-queries"
  }

  private def typeModifiers(): List[TypeModifier] = {
    List()
  }

  override private[integration] def allQueryIds(): Array[String] = ALL_QUERY_IDS

  override private[integration] def desc(): String = "TPC-DS"

  override def tableCreator(): TableCreator = TableCreator.discoverSchema()
}

object TpcdsSuite {
  private val TPCDS_WRITE_RELATIVE_PATH = "tpcds-generated"
  private val ALL_QUERY_IDS = Array(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14a",
    "q14b",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21",
    "q22",
    "q23a",
    "q23b",
    "q24a",
    "q24b",
    "q25",
    "q26",
    "q27",
    "q28",
    "q29",
    "q30",
    "q31",
    "q32",
    "q33",
    "q34",
    "q35",
    "q36",
    "q37",
    "q38",
    "q39a",
    "q39b",
    "q40",
    "q41",
    "q42",
    "q43",
    "q44",
    "q45",
    "q46",
    "q47",
    "q48",
    "q49",
    "q50",
    "q51",
    "q52",
    "q53",
    "q54",
    "q55",
    "q56",
    "q57",
    "q58",
    "q59",
    "q60",
    "q61",
    "q62",
    "q63",
    "q64",
    "q65",
    "q66",
    "q67",
    "q68",
    "q69",
    "q70",
    "q71",
    "q72",
    "q73",
    "q74",
    "q75",
    "q76",
    "q77",
    "q78",
    "q79",
    "q80",
    "q81",
    "q82",
    "q83",
    "q84",
    "q85",
    "q86",
    "q87",
    "q88",
    "q89",
    "q90",
    "q91",
    "q92",
    "q93",
    "q94",
    "q95",
    "q96",
    "q97",
    "q98",
    "q99")
  private val HISTORY_WRITE_PATH = "/tmp/tpcds-history"
}
