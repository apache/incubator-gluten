package io.glutenproject.integration.tpc.action

import io.glutenproject.integration.tpc.{TpcRunner, TpcSuite}
import org.apache.spark.repl.Main

case class SparkShell(scale: Double) extends Action {
  override def execute(tpcSuite: TpcSuite): Boolean = {
    tpcSuite.sessionSwitcher.useSession("test", "Gluten Spark CLI")
    val runner: TpcRunner = new TpcRunner(tpcSuite.queryResource(), tpcSuite.dataWritePath(scale))
    runner.createTables(tpcSuite.sessionSwitcher.spark())
    Main.sparkSession = tpcSuite.sessionSwitcher.spark()
    Main.sparkContext = tpcSuite.sessionSwitcher.spark().sparkContext
    Main.main(Array("-usejavacp"))
    true
  }
}
