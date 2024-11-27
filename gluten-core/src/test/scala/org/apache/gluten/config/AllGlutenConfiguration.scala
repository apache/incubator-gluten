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
package org.apache.gluten.config

import org.apache.gluten.GlutenConfig
import org.apache.gluten.MarkdownBuilder

import org.apache.spark.sql.internal.SQLConfBridge

import org.scalactic.Prettifier
import org.scalactic.source.Position
import org.scalatest.Assertions._
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.collection.JavaConverters._
import scala.collection.Traversable
import scala.io.Source

/**
 * End-to-end test cases for configuration doc file The golden result file is
 * "docs/Configuration.md".
 *
 * To run the entire test suite:
 * {{{
 *   GLUTEN_UPDATE=0 dev/gen/gen_all_config_docs.sh
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   dev/gen/gen_all_config_docs.sh
 * }}}
 */
class AllGlutenConfiguration extends AnyFunSuite {
  private val glutenHome: String =
    AllGlutenConfiguration.getCodeSourceLocation(this.getClass).split("gluten-core")(0)
  private val markdown = Paths.get(glutenHome, "docs", "Configuration.md").toAbsolutePath

  private val veloxBackendIdentify = ".velox."
  private val chBackendIdentify = ".ch."

  private def loadConfigs = Array(GlutenConfig)

  test("Check all gluten configs") {
    loadConfigs
    val builder = MarkdownBuilder(getClass.getName)

    builder ++=
      s"""
         |---
         |layout: page
         |title: Configuration
         |nav_order: 3
         |---
         |
         |"""

    builder ++=
      s"""
         |# Spark Configurations for Gluten Plugin
         |
         |""" +=
      """There are many configurations could impact the Gluten Plugin performance
        | and can be fine-tuned in Spark.""" +=
      """You can add these configurations into spark-defaults.conf to enable or
        | disable the setting."""

    builder ++=
      s"""
         |## Gluten configurations
         |
         | Key | Default | Meaning
         | --- | --- | ---
         |"""

    SQLConfBridge.getConfigEntries.asScala.toStream
      .filter(_.key.contains("gluten"))
      .filterNot(_.key.contains(veloxBackendIdentify))
      .filterNot(_.key.contains(chBackendIdentify))
      .sortBy(_.key)
      .foreach {
        entry =>
          val dft = entry.defaultValueString.replace("<", "&lt;").replace(">", "&gt;")
          builder += Seq(s"${entry.key}", s"$dft", s"${entry.doc}")
            .mkString("|")
      }

    builder ++=
      s"""
         |
         |## Gluten Velox backend configurations
         |
         |The following configurations are related to Velox settings.
         |
         | Key | Default | Meaning
         | --- | --- | ---
         |"""

    SQLConfBridge.getConfigEntries.asScala.toStream
      .filter(_.key.contains(veloxBackendIdentify))
      .sortBy(_.key)
      .foreach {
        entry =>
          val dft = entry.defaultValueString.replace("<", "&lt;").replace(">", "&gt;")
          builder += Seq(s"${entry.key}", s"$dft", s"${entry.doc}")
            .mkString("|")
      }

    builder ++=
      s"""
         |
         |## Gluten CH backend configurations
         |
         |The following configurations are related to CH settings.
         |
         | Key | Default | Meaning
         | --- | --- | ---
         |"""

    SQLConfBridge.getConfigEntries.asScala.toStream
      .filter(_.key.contains(chBackendIdentify))
      .sortBy(_.key)
      .foreach {
        entry =>
          val dft = entry.defaultValueString.replace("<", "&lt;").replace(">", "&gt;")
          builder += Seq(s"${entry.key}", s"$dft", s"${entry.doc}")
            .mkString("|")
      }

    builder ++=
      s"""
         |# Thread level gluten configuration
         |
         |""" +=
      """Additionally, you can control the configurations of gluten at thread level
         | by local property."""

    builder ++=
      """
         |
         |Parameters | Description | Recommend Setting
         | --- | --- | ---
         || gluten.enabledForCurrentThread | Control the usage of gluten at thread level. | true |
         |
         |Below is an example of developing an application using scala to set local properties.
         |
         |```scala
         |// Before executing the query, set local properties.
         |sparkContext.setLocalProperty(key, value)
         |spark.sql("select * from demo_tables").show()
         |```
         |"""

    AllGlutenConfiguration.verifyOrRegenerateGoldenFile(
      markdown,
      builder.toMarkdown,
      "dev/gen/gen_all_config_docs.sh")
  }
}

object AllGlutenConfiguration {
  def isRegenerateGoldenFiles: Boolean = sys.env.get("GLUTEN_UPDATE").contains("1")

  def getCodeSourceLocation[T](clazz: Class[T]): String = {
    clazz.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
  }

  /**
   * Verify the golden file content when GLUTEN_UPDATE env is not equals to 1, or regenerate the
   * golden file content when GLUTEN_UPDATE env is equals to 1.
   *
   * @param path
   *   the path of file
   * @param lines
   *   the expected lines for validation or regeneration
   * @param regenScript
   *   the script for regeneration, used for hints when verification failed
   */
  def verifyOrRegenerateGoldenFile(
      path: Path,
      lines: Iterable[String],
      regenScript: String): Unit = {
    if (isRegenerateGoldenFiles) {
      Files.write(
        path,
        lines.asJava,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING)
    } else {
      assertFileContent(path, lines, regenScript)
    }
  }

  /**
   * Assert the file content is equal to the expected lines. If not, throws assertion error with the
   * given regeneration hint.
   * @param expectedLines
   *   expected lines
   * @param path
   *   source file path
   * @param regenScript
   *   regeneration script
   * @param splitFirstExpectedLine
   *   whether to split the first expected line into multiple lines by EOL
   */
  def assertFileContent(
      path: Path,
      expectedLines: Traversable[String],
      regenScript: String,
      splitFirstExpectedLine: Boolean = false)(implicit
      prettifier: Prettifier,
      pos: Position): Unit = {
    val fileSource = Source.fromFile(path.toUri, StandardCharsets.UTF_8.name())
    try {
      def expectedLinesIter = if (splitFirstExpectedLine) {
        Source.fromString(expectedLines.head).getLines()
      } else {
        expectedLines.toIterator
      }
      val fileLinesIter = fileSource.getLines()
      val regenerationHint = s"The file ($path) is out of date. " + {
        if (regenScript != null && regenScript.nonEmpty) {
          s" Please regenerate it by running `${regenScript.stripMargin}`. "
        } else ""
      }
      var fileLineCount = 0
      fileLinesIter.zipWithIndex
        .zip(expectedLinesIter)
        .foreach {
          case ((lineInFile, lineIndex), expectedLine) =>
            val lineNum = lineIndex + 1
            withClue(s"Line $lineNum is not expected. $regenerationHint") {
              assertResult(expectedLine)(lineInFile)(prettifier, pos)
            }
            fileLineCount = Math.max(lineNum, fileLineCount)
        }
      withClue(s"Line number is not expected. $regenerationHint") {
        assertResult(expectedLinesIter.size)(fileLineCount)(prettifier, pos)
      }
    } finally {
      fileSource.close()
    }
  }
}
