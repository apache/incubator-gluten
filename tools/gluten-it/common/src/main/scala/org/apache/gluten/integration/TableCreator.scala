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

import org.apache.spark.sql.{AnalysisException, SparkSession}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.File
import java.net.URI

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait TableCreator {
  def create(spark: SparkSession, source: String, dataPath: String): Unit
}

object TableCreator {
  def discoverSchema(): TableCreator = {
    DiscoverSchema
  }

  private object DiscoverSchema extends TableCreator {
    override def create(spark: SparkSession, source: String, dataPath: String): Unit = {
      val existedTableNames = mutable.ArrayBuffer[String]()
      val createdTableNames = mutable.ArrayBuffer[String]()
      val recoveredPartitionTableNames = mutable.ArrayBuffer[String]()

      // Handle remote filesystems (hdfs://, s3a://) vs local paths
      val (tableNames, tablePaths) = if (dataPath.startsWith("hdfs://") || dataPath.startsWith("s3a://")) {
        val uri = URI.create(dataPath)
        val fs = FileSystem.get(uri, new Configuration())
        val basePath = new Path(dataPath)
        val statuses = fs.listStatus(basePath)
        val tableDirs = statuses.filter(_.isDirectory).map(_.getPath)
        val names = tableDirs.map(_.getName)
        val paths = tableDirs.map(_.toString)
        (names, paths)
      } else {
        val files = new File(dataPath).listFiles()
        val names = files.map(_.getName)
        val paths = files.map(_.getAbsolutePath)
        (names, paths)
      }

      if (tableNames.isEmpty) {
        return
      }

      println("Creating catalog tables: " + tableNames.mkString(", "))

      tableNames.zip(tablePaths).foreach {
        case (tableName, tablePath) =>
          if (spark.catalog.tableExists(tableName)) {
            existedTableNames += tableName
          } else {
            spark.catalog.createTable(tableName, tablePath, source)
            createdTableNames += tableName
            try {
              spark.catalog.recoverPartitions(tableName)
              recoveredPartitionTableNames += tableName
            } catch {
              case _: AnalysisException =>
              // Swallows analysis exceptions.
            }
          }
      }

      if (existedTableNames.nonEmpty) {
        println("Tables already exists: " + existedTableNames.mkString(", "))
      }

      if (createdTableNames.nonEmpty) {
        println("Tables created: " + createdTableNames.mkString(", "))
      }

      if (recoveredPartitionTableNames.nonEmpty) {
        println("Recovered partition tables: " + recoveredPartitionTableNames.mkString(", "))
      }
    }
  }
}
