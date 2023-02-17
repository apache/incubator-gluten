package io.glutenproject.integration.tpc.action

import io.glutenproject.integration.tpc.TpcSuite

import java.io.File

case class DataGenOnly(scale: Double, genPartitionedData: Boolean) extends Action {
  override def execute(tpcSuite: TpcSuite): Boolean = {
    tpcSuite.sessionSwitcher.useSession("baseline", "Data Gen")
    val dataGen = tpcSuite.createDataGen(scale, genPartitionedData)
    dataGen.gen()
    true
  }
}
