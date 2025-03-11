package org.apache.gluten.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.{DataFrame, Row}

class GlutenSQLCollectTailExecSuite extends WholeStageTransformerSuite {

  override protected val resourcePath: String = "N/A"
  override protected val fileFormat: String = "N/A"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
  }

  /**
   * Helper method for:
   * 1) Registers a Listener that checks for "ColumnarCollectTail" in the final plan.
   * 2) Calls df.tail(tailCount) to physically trigger CollectTailExec.
   * 3) Asserts the returned rows match expectedRows.
   */
  private def verifyTailExec(df: DataFrame, expectedRows: Seq[Row], tailCount: Int): Unit = {
    class TailExecListener extends QueryExecutionListener {
      var latestPlan: String = ""

      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        latestPlan = qe.executedPlan.toString()
        assert(
          latestPlan.contains("ColumnarCollectTail"),
          "ColumnarCollectTail was not found in the physical plan!"
        )
      }

      override def onFailure(funcName: String, qe: QueryExecution, error: Exception): Unit = {}
    }

    val tailExecListener = new TailExecListener()
    spark.listenerManager.register(tailExecListener)
    try {
      val tailArray = df.tail(tailCount)

      assert(
        tailArray.sameElements(expectedRows),
        s"""
           |Tail output [${tailArray.mkString(", ")}]
           |did not match expected [${expectedRows.mkString(", ")}].
         """.stripMargin
      )
    } finally {
      spark.listenerManager.unregister(tailExecListener)
    }
  }

  test("ColumnarCollectTailExec - verify CollectTailExec in physical plan") {
    val df = spark.range(0, 1000, 1).toDF("id").orderBy("id")
    val expected = Seq(Row(996L), Row(997L), Row(998L), Row(999L))
    verifyTailExec(df, expected, tailCount = 4)
  }

  test("ColumnarCollectTailExec - basic tail test") {
    val df = spark.range(0, 1000, 1).toDF("id").orderBy("id")
    val expected = Seq(Row(995L), Row(996L), Row(997L), Row(998L), Row(999L))
    verifyTailExec(df, expected, tailCount = 5)
  }

  test("ColumnarCollectTailExec - with filter") {
    val df = spark.range(0, 20, 1).toDF("id").filter("id % 2 == 0").orderBy("id")
    val expected = Seq(Row(10L), Row(12L), Row(14L), Row(16L), Row(18L))
    verifyTailExec(df, expected, tailCount = 5)
  }

  test("ColumnarCollectTailExec - range with repartition") {
    val df = spark.range(0, 10).toDF("id").repartition(3).orderBy("id")
    val expected = Seq(Row(7L), Row(8L), Row(9L))
    verifyTailExec(df, expected, tailCount = 3)
  }

  test("ColumnarCollectTailExec - with distinct values") {
    val df = spark.range(0, 10).toDF("id").distinct().orderBy("id")
    val expected = Seq(Row(5L), Row(6L), Row(7L), Row(8L), Row(9L))
    verifyTailExec(df, expected, tailCount = 5)
  }

  test("ColumnarCollectTailExec - chained tail") {
    val df = spark.range(0, 10).toDF("id").orderBy("id")
    val expected = (2L to 9L).map(Row(_))
    verifyTailExec(df, expected, tailCount = 8)
  }

  test("ColumnarCollectTailExec - tail after union") {
    val df1 = spark.range(0, 5).toDF("id")
    val df2 = spark.range(5, 10).toDF("id")
    val unionDf = df1.union(df2).orderBy("id")
    val expected = Seq(Row(7L), Row(8L), Row(9L))
    verifyTailExec(unionDf, expected, tailCount = 3)
  }
}
