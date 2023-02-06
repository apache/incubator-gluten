package io.glutenproject.integration.tpc.action

import io.glutenproject.integration.tpc.TpcSuite

trait Action {
  def execute(tpcSuite: TpcSuite): Boolean
}

object Actions {
  def createActions(
      mode: String,
      skipDataGen: Boolean,
      scale: Double,
      genPartitionedData: Boolean,
      queryIds: Array[String],
      explain: Boolean,
      iterations: Int): Array[Action] = {
    val actions = new java.util.ArrayList[Action]()
    if (!skipDataGen) {
      actions.add(DataGenOnly(scale, genPartitionedData))
    }
    mode match {
      case "data-gen-only" =>
      case "queries-compare" =>
        actions.add(QueriesCompare(scale, queryIds, explain, iterations))
      case "queries" =>
        actions.add(Queries(scale, queryIds, explain, iterations))
      case "cli" =>
        actions.add(Cli(scale))
      case _ =>
        throw new IllegalArgumentException("Unexpected mode: " + mode)
    }
    actions.toArray(Array[Action]())
  }
}
