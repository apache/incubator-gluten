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

package org.apache.spark.sql

import org.apache.spark.internal.config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK
import org.apache.spark.sql.GlutenSparkSessionSwitcher.NONE
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.{DebugFilesystem, SparkConf, SparkContext}

class GlutenSparkSessionSwitcher(val cpus: Int, val logLevel: String) extends AutoCloseable {
  private val sessionMap: java.util.Map[SessionToken, SparkConf] =
    new java.util.HashMap[SessionToken, SparkConf]

  private val testDefaults = new SparkConf()
      .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
      .set(UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)

  testDefaults.set(
    StaticSQLConf.WAREHOUSE_PATH,
    testDefaults.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)

  private var _spark: SparkSession = _
  private var _activeSessionDesc: SessionDesc = GlutenSparkSessionSwitcher.NONE

  def defaultConf(): SparkConf = {
    testDefaults
  }

  def registerSession(name: String, conf: SparkConf): SessionToken = synchronized {
    val token = SessionToken(name)
    if (sessionMap.containsKey(token)) {
      throw new IllegalArgumentException(s"Session name already registered: $name")
    }
    sessionMap.put(token, conf)
    return token
  }

  def useSession(token: String, appName: String = "gluten-app"): Unit = synchronized {
    useSession(SessionDesc(SessionToken(token), appName))
  }

  def useSession(desc: SessionDesc): Unit = synchronized {
    if (desc == _activeSessionDesc) {
      return
    }
    if (!sessionMap.containsKey(desc.sessionToken)) {
      throw new IllegalArgumentException(s"Session doesn't exist: $desc")
    }
    println(s"Switching to ${desc} session... ")
    stopActiveSession()
    val conf = new SparkConf()
        .setAll(testDefaults.getAll)
        .setAll(sessionMap.get(desc.sessionToken).getAll)
    activateSession(conf, desc.appName)
    _activeSessionDesc = desc
    println(s"Successfully switched to $desc session. ")
  }

  def spark(): SparkSession = {
    _spark
  }

  private def activateSession(conf: SparkConf, appName: String): Unit = {
    SparkSession.cleanupAnyExistingSession()
    if (hasActiveSession()) {
      stopActiveSession()
    }
    createSession(conf, appName = appName)
    SparkSession.setDefaultSession(_spark)
    SparkSession.setActiveSession(_spark)
  }

  private def stopActiveSession(): Unit = synchronized {
    try {
      if (_spark != null) {
        try {
          _spark.sessionState.catalog.reset()
        } finally {
          _spark.stop()
          _spark = null
          _activeSessionDesc = NONE
        }
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def createSession(conf: SparkConf, appName: String): Unit = {
    if (hasActiveSession()) {
      throw new IllegalStateException()
    }
    _spark = new SparkSession(new SparkContext(s"local[$cpus]", appName, conf))
    _spark.sparkContext.setLogLevel(logLevel)
  }

  private def hasActiveSession(): Boolean = {
    _spark != null
  }

  override def close(): Unit = {
    stopActiveSession()
  }
}

case class SessionToken(name: String)

case class SessionDesc(sessionToken: SessionToken, appName: String)

object GlutenSparkSessionSwitcher {
  val NONE: SessionDesc = SessionDesc(SessionToken("none"), "none")
}
