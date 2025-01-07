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
package org.apache.gluten.integration

import org.apache.gluten.integration.metrics.MetricMapper

import org.apache.spark.sql.{RunResult, SparkQueryRunner, SparkSession}

import com.google.common.base.Preconditions
import org.apache.commons.lang3.exception.ExceptionUtils

import java.io.File

class QueryRunner(val queryResourceFolder: String, val dataPath: String) {
  import QueryRunner._

  Preconditions.checkState(
    new File(dataPath).exists(),
    s"Data not found at $dataPath, try using command `<gluten-it> data-gen-only <options>` to generate it first.",
    Array(): _*)

  def createTables(creator: TableCreator, spark: SparkSession): Unit = {
    creator.create(spark, dataPath)
  }

  def runQuery(
      spark: SparkSession,
      desc: String,
      caseId: String,
      explain: Boolean = false,
      sqlMetricMapper: MetricMapper = MetricMapper.dummy,
      executorMetrics: Seq[String] = Nil,
      randomKillTasks: Boolean = false): QueryResult = {
    val path = "%s/%s.sql".format(queryResourceFolder, caseId)
    try {
      val r =
        SparkQueryRunner.runQuery(
          spark,
          desc,
          path,
          explain,
          sqlMetricMapper,
          executorMetrics,
          randomKillTasks)
      println(s"Successfully ran query $caseId. Returned row count: ${r.rows.length}")
      Success(caseId, r)
    } catch {
      case e: Exception =>
        println(s"Error running query $caseId. Error: ${ExceptionUtils.getStackTrace(e)}")
        Failure(caseId, e)
    }
  }
}

object QueryRunner {
  sealed trait QueryResult {
    def caseId(): String
    def succeeded(): Boolean
  }

  implicit class QueryResultOps(r: QueryResult) {
    def asSuccessOption(): Option[Success] = {
      r match {
        case s: Success => Some(s)
        case _: Failure => None
      }
    }

    def asFailureOption(): Option[Failure] = {
      r match {
        case _: Success => None
        case f: Failure => Some(f)
      }
    }

    def asSuccess(): Success = {
      asSuccessOption().get
    }

    def asFailure(): Failure = {
      asFailureOption().get
    }
  }

  case class Success(override val caseId: String, runResult: RunResult) extends QueryResult {
    override def succeeded(): Boolean = true
  }
  case class Failure(override val caseId: String, error: Exception) extends QueryResult {
    override def succeeded(): Boolean = false
  }
}
