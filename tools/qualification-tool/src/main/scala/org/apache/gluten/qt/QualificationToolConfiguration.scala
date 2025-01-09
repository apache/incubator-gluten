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
package org.apache.gluten.qt

import org.apache.commons.cli.{CommandLine, DefaultParser, Options}

import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.time.temporal._

/** Default Configurations */
case class QualificationToolConfiguration(
    inputPath: String,
    verbose: Boolean = false,
    gcsKeys: String = "",
    outputPath: Path,
    threads: Int,
    dateFilter: Long,
    project: String
)

object QualificationToolConfiguration {
  def parseCommandLineArgs(args: Array[String]): QualificationToolConfiguration = {
    val options = new Options()

    options.addOption("f", "input", true, "File / Folder for qualification tool")
    options.addOption("k", "gcsKey", true, "Provide the path to GCS keys")
    options.addOption("o", "output", true, "Path to write output")
    options.addOption("t", "threads", true, "Processing Threads")
    options.addOption("v", "verbose", false, "non verbose output")
    options.addOption("p", "project", true, "Project Id")
    options.addOption("d", "dateFilter", true, "Do not analyze files created before this date")

    val parser = new DefaultParser()
    val cmd: CommandLine = parser.parse(options, args)

    val inputPath = cmd.getOptionValue("input")
    if (inputPath == null) {
      throw new RuntimeException("Input Path can not be null")
    }

    val outputPath = Option(cmd.getOptionValue("output"))
      .map {
        p =>
          val path = Paths.get(p)
          Files.createDirectories(path)
          path
      }
      .getOrElse {
        Files.createTempDirectory(
          java.util.UUID.randomUUID.toString.replaceAll(":+", "_").replaceAll("/", "_"))
      }

    val verbose = !cmd.hasOption("v")

    val gcsKeys = cmd.getOptionValue("k")

    val threads = Option(cmd.getOptionValue("threads")).map(_.toInt).getOrElse(4)
    val dateFilter = Option(cmd.getOptionValue("dateFilter"))
      .map(df => Instant.parse(df + "T00:00:00.00Z").toEpochMilli)
      .getOrElse {
        // Analyze only for last 90 days
        Instant.now().minus(90, ChronoUnit.DAYS).toEpochMilli
      }

    val project = Option(cmd.getOptionValue("project")).getOrElse("")

    QualificationToolConfiguration(
      inputPath = inputPath,
      verbose = verbose,
      gcsKeys = gcsKeys,
      outputPath = outputPath,
      threads = threads,
      dateFilter = dateFilter,
      project)
  }
}
