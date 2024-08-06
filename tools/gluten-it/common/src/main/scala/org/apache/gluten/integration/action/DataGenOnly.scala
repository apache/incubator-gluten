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
package org.apache.gluten.integration.action

import org.apache.gluten.integration.Suite

import java.io.File

case class DataGenOnly(strategy: DataGenOnly.Strategy, scale: Double, genPartitionedData: Boolean)
    extends Action {
  override def execute(suite: Suite): Boolean = {
    strategy match {
      case DataGenOnly.Skip =>
        // Do nothing
      case DataGenOnly.Once =>
        val dataPath = suite.dataWritePath(scale, genPartitionedData)
        val alreadyExists = new File(dataPath).exists()
        if (alreadyExists) {
          println(s"Data already exists at $dataPath, skipping generating it.")
        } else {
          gen(suite)
        }
      case DataGenOnly.Always =>
        gen(suite)
    }
    true
  }

  private def gen(suite: Suite): Unit = {
    suite.sessionSwitcher.useSession("baseline", "Data Gen")
    val dataGen = suite.createDataGen(scale, genPartitionedData)
    dataGen.gen()
  }
}

object DataGenOnly {
  sealed trait Strategy
  case object Skip extends Strategy
  case object Once extends Strategy
  case object Always extends Strategy
}
