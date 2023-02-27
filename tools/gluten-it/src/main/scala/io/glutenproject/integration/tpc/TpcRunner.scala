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

package io.glutenproject.integration.tpc

import com.google.common.base.Preconditions
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SparkSession}

import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets

class TpcRunner(val queryResourceFolder: String, val dataPath: String) {

  def createTables(spark: SparkSession): Unit = {
    TpcRunner.createTables(spark, dataPath)
  }

  def runTpcQuery(spark: SparkSession, caseId: String, explain: Boolean = false, desc: String): RunResult = {
    spark.sparkContext.setJobDescription(desc)
    val path = "%s/%s.sql".format(queryResourceFolder, caseId);
    println(s"Executing SQL query from resource path $path...")
    val sql = TpcRunner.resourceToString(path)
    val prev = System.nanoTime()
    val df = spark.sql(sql)
    if (explain) {
      df.explain(extended = true)
    }
    val rows = df.collect()
    val millis = (System.nanoTime() - prev) / 1000000L
    RunResult(rows, millis)
  }
}

object TpcRunner {
  def createTables(spark: SparkSession, dataPath: String): Unit = {
    val files = new File(dataPath).listFiles()
    files.foreach(file => {
      if (spark.catalog.tableExists(file.getName)) {
        println("Table exists: " + file.getName)
      } else {
        println("Creating catalog table: " + file.getName)
        spark.catalog.createTable(file.getName, file.getAbsolutePath, "parquet")
        try {
          spark.catalog.recoverPartitions(file.getName)
        } catch {
          case _: Throwable =>
        }
      }
    })
  }

  private def resourceToString(resource: String): String = {
    val inStream = classOf[TpcRunner].getResourceAsStream(resource)
    Preconditions.checkNotNull(inStream)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while (reading) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    new String(outStream.toByteArray, StandardCharsets.UTF_8)
  }

  private def delete(path: String): Unit = {
    FileUtils.forceDelete(new File(path))
  }
}

case class RunResult(rows: Seq[Row], executionTimeMillis: Long)
