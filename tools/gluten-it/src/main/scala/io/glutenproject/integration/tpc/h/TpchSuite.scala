package io.glutenproject.integration.tpc.h

import io.glutenproject.integration.tpc.{Constants, DataGen, TpcSuite, TypeModifier}
import io.glutenproject.integration.tpc.h.TpchSuite.{HISTORY_WRITE_PATH, TPCH_WRITE_PATH}
import org.apache.log4j.Level

import org.apache.spark.SparkConf

class TpchSuite(
  val testConf: SparkConf,
  val baselineConf: SparkConf,
  val scale: Double,
  val fixedWidthAsDouble: Boolean,
  val queryIds: Array[String],
  val logLevel: Level,
  val explain: Boolean,
  val errorOnMemLeak: Boolean,
  val enableHsUi: Boolean,
  val hsUiPort: Int,
  val cpus: Int,
  val offHeapSize: String,
  val iterations: Int,
  val disableAqe: Boolean,
  val disableBhj: Boolean,
  val disableWscg: Boolean,
  val useExistingData: Boolean) extends TpcSuite(testConf, baselineConf, scale, fixedWidthAsDouble,
  queryIds, logLevel, explain, errorOnMemLeak, enableHsUi, hsUiPort, cpus,
  offHeapSize, iterations, disableAqe, disableBhj, disableWscg, useExistingData) {

  override protected def dataWritePath(): String = TPCH_WRITE_PATH

  override protected def historyWritePath(): String = HISTORY_WRITE_PATH

  override protected def createDataGen(): DataGen = new TpchDataGen(sessionSwitcher.spark(),
    scale, cpus, TpchSuite.TPCH_WRITE_PATH, typeModifiers())

  override protected def allQueryIds(): Array[String] = TpchSuite.ALL_QUERY_IDS

  override protected def queryResource(): String = {
    if (fixedWidthAsDouble) {
      "/tpch-queries-noint-nodate"
    } else {
      "/tpch-queries"
    }
  }

  override protected def typeModifiers(): List[TypeModifier] = {
    if (fixedWidthAsDouble) {
      List(Constants.TYPE_MODIFIER_INTEGER_AS_DOUBLE, Constants.TYPE_MODIFIER_LONG_AS_DOUBLE,
        Constants.TYPE_MODIFIER_DATE_AS_DOUBLE)
    } else {
      List()
    }
  }

  override protected def desc(): String = "TPC-H"
}

object TpchSuite {
  private val TPCH_WRITE_PATH = "/tmp/tpch-generated"
  private val ALL_QUERY_IDS = Array("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
    "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22")
  private val HISTORY_WRITE_PATH = "/tmp/tpch-history"
}
