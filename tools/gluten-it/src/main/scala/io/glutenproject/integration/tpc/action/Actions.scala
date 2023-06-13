package io.glutenproject.integration.tpc.action

import io.glutenproject.integration.tpc.TpcSuite

trait Action {
  def execute(tpcSuite: TpcSuite): Boolean
}

object Actions {
}
