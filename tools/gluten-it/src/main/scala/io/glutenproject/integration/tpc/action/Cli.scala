package io.glutenproject.integration.tpc.action

import io.glutenproject.integration.tpc.{TpcRunner, TpcSuite}

import java.util.Scanner

case class Cli(scale: Double) extends Action {
  override def execute(tpcSuite: TpcSuite): Boolean = {
    tpcSuite.sessionSwitcher.useSession("test", "Gluten Spark CLI")
    val runner: TpcRunner = new TpcRunner(tpcSuite.queryResource(), tpcSuite.dataWritePath(scale))
    runner.createTables(tpcSuite.sessionSwitcher.spark())
    val scanner = new Scanner(System.in)
    while (true) {
      print("cli> ")
      val line = scanner.nextLine()
      if (line == "exit" || line == "quit") {
        return true
      }
      try {
        val df = tpcSuite.sessionSwitcher.spark().sql(line)
        df.show(Int.MaxValue, truncate = false)
      } catch {
        case e: Exception =>
          println(e.getMessage)
      }
    }
    true
  }
}
