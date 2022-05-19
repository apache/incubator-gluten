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
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.{DebugFilesystem, SparkConf}

class GlutenSparkSessionSwitcher() {
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

  private var _spark: TestSparkSession = null
  private var _activeSession: SessionToken = GlutenSparkSessionSwitcher.NONE

  def registerSession(name: String, conf: SparkConf): SessionToken = synchronized {
    val token = new SessionToken(name)
    if (sessionMap.containsKey(token)) {
      throw new IllegalArgumentException(s"Session name already registered: $name")
    }
    sessionMap.put(token, conf)
    return token
  }

  def useSession(token: String): Unit = synchronized {
    useSession(new SessionToken(token))
  }

  def useSession(token: SessionToken): Unit = synchronized {
    if (token == _activeSession) {
      return
    }
    if (!sessionMap.containsKey(token)) {
      throw new IllegalArgumentException(s"Session name doesn't exist: ${token.name}")
    }
    // scalastyle:off println
    println(s"Switching to ${token.name} session... ")
    // scalastyle:on println
    stopActiveSession()
    val conf = new SparkConf()
      .setAll(testDefaults.getAll)
      .setAll(sessionMap.get(token).getAll)
    activateSession(conf)
    _activeSession = token
    // scalastyle:off println
    println(s"Successfully switched to ${token.name} session. ")
    // scalastyle:on println
  }

  def stopActiveSession(): Unit = synchronized {
    try {
      if (_spark != null) {
        try {
          _spark.sessionState.catalog.reset()
        } finally {
          _spark.stop()
          _spark = null
          _activeSession = NONE
        }
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  def spark(): SparkSession = {
    _spark
  }

  private def hasActiveSession(): Boolean = {
    _spark != null
  }

  private def createSession(conf: SparkConf): Unit = {
    if (hasActiveSession()) {
      throw new IllegalStateException()
    }
    _spark = new TestSparkSession(conf)
  }

  private def activateSession(conf: SparkConf): Unit = {
    SparkSession.cleanupAnyExistingSession()
    if (hasActiveSession()) {
      stopActiveSession()
    }
    createSession(conf)
  }
}

class SessionToken(val name: String) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[SessionToken]

  override def equals(other: Any): Boolean = other match {
    case that: SessionToken =>
      (that canEqual this) &&
        name == that.name
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(name)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object GlutenSparkSessionSwitcher {
  val NONE: SessionToken = new SessionToken("none")
}
