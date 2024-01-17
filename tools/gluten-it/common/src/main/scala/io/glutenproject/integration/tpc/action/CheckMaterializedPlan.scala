package io.glutenproject.integration.tpc.action

import io.glutenproject.integration.tpc.{TpcRunner, TpcSuite}

import org.apache.spark.sql.{QueryRunner, SparkSessionSwitcher}

import org.apache.commons.lang3.exception.ExceptionUtils

import java.io.{File, PrintWriter}

case class CheckMaterializedPlan(scale: Double, queryIds: Array[String], genGoldenFile: Boolean)
  extends Action {

  override def execute(tpcSuite: TpcSuite): Boolean = {
    println("=== Start check materialized plan ===")
    val runner: TpcRunner = new TpcRunner(
      tpcSuite.queryResource(),
      tpcSuite.dataWritePath(scale),
      tpcSuite.expectPlanResource())

    val allQueries = tpcSuite.allQueryIds()
    val runQueryIds = queryIds match {
      case Array() =>
        allQueries
      case _ =>
        queryIds
    }
    val allQueriesSet = allQueries.toSet
    // fast fail here
    runQueryIds.foreach(
      id =>
        if (!allQueriesSet.contains(id)) {
          throw new IllegalArgumentException(s"Query ID doesn't exist: $id")
        })

    val resultSeq = runQueryIds.map {
      queryId =>
        CheckMaterializedPlan.runTpcQuery(queryId, tpcSuite.sessionSwitcher, runner, genGoldenFile)
    }

    CheckMaterializedPlan.printResult(resultSeq)
    resultSeq.forall(_._2.success)
  }
}

object CheckMaterializedPlan {

  private val skipSqlPathSet: Set[String] = Set.empty

  def runTpcQuery(
      id: String,
      switcher: SparkSessionSwitcher,
      runner: TpcRunner,
      genGoldenFile: Boolean): (String, CheckResult) = {
    try {
      println(s"Running query: $id ---")
      switcher.useSession(token = "test")
      runner.createTables(switcher.spark())
      val path = "%s/%s.sql".format(runner.queryResourceFolder, id)
      if (skipSqlPathSet.contains(path)) {
        (id, CheckResult(success = true, Some("Skipped.")))
      }

      val result = runner.runTpcQuery(switcher.spark(), "", id)

      val goldenFilePath =
        s"${runner.expectResourceFolder}/spark${switcher.sparkMainVersion()}/$id.txt"
      val except = QueryRunner.resourceToString(goldenFilePath)
      val actual = formatMaterializedPlan(result.materializedPlan)

      if (genGoldenFile) {
        outputFormattedMaterializedPlan(goldenFilePath, actual)
      }
      if (except == actual) {
        (id, CheckResult(success = true, None))
      } else {
        (
          id,
          CheckResult(success = false, Some("Except:\n" + except + "\nActual:\n" + actual + "\n")))
      }
    } catch {
      case e: Exception =>
        (id, CheckResult(success = false, Some(ExceptionUtils.getStackTrace(e))))
    }
  }

  private def formatMaterializedPlan(plan: String): String = {
    plan
      .replaceAll("#[0-9]*L*", "#X")
      .replaceAll("plan_id=[0-9]*", "plan_id=X")
      .replaceAll("Statistics[(A-Za-z0-9=. ,+)]*", "Statistics(X)")
      .replaceAll("WholeStageCodegenTransformer[0-9 ()]*", "WholeStageCodegenTransformer (X)")
  }

  private def printResult(seq: Seq[(String, CheckResult)]): Unit = {
    println("=== Result ===")
    seq.foreach {
      case (id, result) =>
        println(s"===Query: $id, ${if (result.success) "SUCCESS" else "FAIL"}===")
        result.msg.foreach(msg => println(msg))
    }
  }

  private def outputFormattedMaterializedPlan(path: String, plan: String): Unit = {
    val file = new File(s"/tmp/$path")
    // create parent folder if not exists
    file.getParentFile.exists() || file.getParentFile.mkdirs()
    file.createNewFile()
    val writer = new PrintWriter(file)
    try {
      writer.write(plan)
    } finally {
      writer.close()
    }
  }
}

case class CheckResult(success: Boolean, msg: Option[String])
