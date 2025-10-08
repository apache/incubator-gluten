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
package org.apache.spark.sql.avro

import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution.{BatchScanExecTransformer, FileSourceScanExecTransformer}

import org.apache.spark.sql.{DataFrame, GlutenSQLTestsBaseTrait, QueryTest, Row}
import org.apache.spark.sql.avro.GlutenAvroTestBase._
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf

import java.io.File
import java.nio.file.{Files, Path, StandardCopyOption}

import scala.collection.mutable

trait GlutenAvroTestBase extends QueryTest with GlutenSQLTestsBaseTrait {
  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConfString(VeloxConfig.VELOX_AVRO_SCAN_ENABLED.key, "true")
  }

  import testImplicits._
  override protected def testFile(testResource: String): String = {
    extractedResources.getOrElseUpdate(testResource, copyToDisk(testResource))
  }

  protected def checkAvroScanFallback(df: DataFrame): Unit = {
    val executedPlans = getExecutedPlan(df)

    val hasNativeScan = executedPlans.exists {
      case _: FileSourceScanExecTransformer => true
      case _: BatchScanExecTransformer => true
      case _ => false
    }
    assert(hasNativeScan, s"Expected native scan, but got plan: ${df.queryExecution.executedPlan}")

    val hasFallbackScan = executedPlans.exists {
      case _: FileSourceScanExec => true
      case _: BatchScanExec => true
      case _ => false
    }
    assert(!hasFallbackScan, s"Unexpected fallback plan: ${df.queryExecution.executedPlan}")
  }

  test("native avro scan") {
    withTempPath {
      tempDir =>
        val avroDir = new File(tempDir, "data").getCanonicalPath
        spark.range(3).write.format("avro").save(avroDir)
        val df = spark.read.format("avro").load(avroDir)

        checkAvroScanFallback(df)
        checkAnswer(df, Seq(Row(0), Row(1), Row(2)))
    }
  }

  test("native avro filter") {
    Seq(true, false).foreach {
      filtersPushdown =>
        withSQLConf(SQLConf.AVRO_FILTER_PUSHDOWN_ENABLED.key -> filtersPushdown.toString) {
          withTempPath {
            dir =>
              Seq(("a", 1, 2), ("b", 1, 2), ("c", 2, 1))
                .toDF("value", "p1", "p2")
                .write
                .format("avro")
                .save(dir.getCanonicalPath)
              val df = spark.read
                .format("avro")
                .load(dir.getCanonicalPath)
                .where("value = 'a'")

              checkAvroScanFallback(df)
              checkAnswer(df, Row("a", 1, 2))
          }
        }
    }
  }
}

class GlutenAvroV1Suite extends AvroV1Suite with GlutenAvroTestBase

class GlutenAvroV2Suite extends AvroV2Suite with GlutenAvroTestBase

object GlutenAvroTestBase {
  private val extractedResources = mutable.Map.empty[String, String]
  private val tempDir: Path = {
    val dir = Files.createTempDirectory("gluten-avro-resources")
    dir.toFile.deleteOnExit()
    dir
  }

  private def copyToDisk(resource: String): String = {
    require(resource.nonEmpty, "Resource name cannot be empty")
    val stream = Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(resource))
      .orElse(Option(getClass.getClassLoader.getResourceAsStream(resource)))
      .getOrElse(throw new IllegalArgumentException(s"Resource $resource not found on classpath"))

    val target = tempDir.resolve(resource)
    Option(target.getParent).foreach(Files.createDirectories(_))
    try {
      Files.copy(stream, target, StandardCopyOption.REPLACE_EXISTING)
    } finally {
      stream.close()
    }
    target.toFile.deleteOnExit()
    s"file://${target.toFile.getCanonicalPath}"
  }
}
