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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

case class DataGenOnly(strategy: DataGenOnly.Strategy) extends Action {

  override def execute(suite: Suite): Boolean = {
    strategy match {
      case DataGenOnly.Skip =>
        ()

      case DataGenOnly.Once =>
        val fs = this.fs(suite)
        val dataPath = this.dataPath(suite)
        val markerPath = this.markerPath(suite)

        if (fs.exists(dataPath) && fs.exists(markerPath)) {
          println(s"Test data already generated at $dataPath. Skipping.")
        } else {
          if (fs.exists(dataPath)) {
            println(
              s"Test data exists at $dataPath but no completion marker found. Regenerating."
            )
            fs.delete(dataPath, true)
          }
          if (fs.exists(markerPath)) {
            fs.delete(markerPath, true)
          }
          genAndMark(suite)
        }

      case DataGenOnly.Always =>
        genAndMark(suite)
    }
    true
  }

  private def fs(suite: Suite): FileSystem = {
    dataPath(suite).getFileSystem(new Configuration())
  }

  private def markerPath(suite: Suite): Path =
    new Path(suite.dataWritePath() + ".completed")

  private def dataPath(suite: Suite): Path =
    new Path(suite.dataWritePath())

  private def genAndMark(suite: Suite): Unit = {
    val dataPath = suite.dataWritePath()
    val marker = markerPath(suite)
    val fileSystem = fs(suite)

    println(s"Generating test data to $dataPath...")

    suite.sessionSwitcher.useSession("baseline", "Data Gen")
    val dataGen = suite.createDataGen()
    dataGen.gen()

    // Create marker only after successful generation
    val out = fileSystem.create(marker, false)
    out.close()

    println(s"All test data successfully generated at $dataPath.")
  }
}

object DataGenOnly {
  sealed trait Strategy
  case object Skip extends Strategy
  case object Once extends Strategy
  case object Always extends Strategy
}
