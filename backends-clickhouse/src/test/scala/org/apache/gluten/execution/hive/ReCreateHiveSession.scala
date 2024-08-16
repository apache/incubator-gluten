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
package org.apache.gluten.execution.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession

import org.scalatest.BeforeAndAfterAll

trait ReCreateHiveSession extends SharedSparkSession with BeforeAndAfterAll {

  protected val hiveMetaStoreDB: String

  private var _hiveSpark: SparkSession = _

  override protected def spark: SparkSession = _hiveSpark

  override protected def initializeSession(): Unit = {
    if (_hiveSpark == null) {
      _hiveSpark = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .config(
          "javax.jdo.option.ConnectionURL",
          s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true")
        .getOrCreate()
    }
  }

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_hiveSpark != null) {
          try {
            _hiveSpark.sessionState.catalog.reset()
          } finally {
            _hiveSpark.stop()
            _hiveSpark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  protected def updateHiveSession(newSession: SparkSession): Unit = {
    _hiveSpark = null
    _hiveSpark = newSession
  }
}
