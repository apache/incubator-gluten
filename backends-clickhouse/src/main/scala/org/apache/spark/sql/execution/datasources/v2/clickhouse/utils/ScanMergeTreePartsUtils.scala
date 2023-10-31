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
package org.apache.spark.sql.execution.datasources.v2.clickhouse.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddFileTags
import org.apache.spark.sql.execution.datasources.v2.clickhouse.table.ClickHouseTableV2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object ScanMergeTreePartsUtils extends Logging {

  def scanMergeTreePartsToAddFile(
      configuration: Configuration,
      clickHouseTableV2: ClickHouseTableV2,
      pathFilter: String,
      isBucketTable: Boolean): Seq[AddFile] = {
    // scan parts dir
    val scanPath = new Path(clickHouseTableV2.path + pathFilter)
    val fs = scanPath.getFileSystem(configuration)
    val fileGlobStatuses = fs.globStatus(scanPath)
    val allDirSummary = fileGlobStatuses
      .filter(_.isDirectory)
      .map(
        p => {
          logInfo(s"scan merge tree parts: ${p.getPath.toString}")
          val sum = fs.getContentSummary(p.getPath)
          val pathName = p.getPath.getName
          val pathNameArr = pathName.split("_")
          val (partitionId, bucketNum, minBlockNum, maxBlockNum, level) =
            if (pathNameArr.length == 4) {
              if (isBucketTable) {
                (
                  pathNameArr(0),
                  p.getPath.getParent.getName,
                  pathNameArr(1).toLong,
                  pathNameArr(2).toLong,
                  pathNameArr(3).toInt)
              } else {
                (
                  pathNameArr(0),
                  "",
                  pathNameArr(1).toLong,
                  pathNameArr(2).toLong,
                  pathNameArr(3).toInt)
              }
            } else {
              ("", "", 0L, 0L, 0)
            }
          (
            pathName,
            partitionId,
            minBlockNum,
            maxBlockNum,
            level,
            sum.getLength,
            p.getModificationTime,
            bucketNum)
        })
      .filter(!_._2.equals(""))

    // generate CommitInfo and AddFile
    val versionFileName = FileNames.deltaFile(clickHouseTableV2.deltaLog.logPath, 1)
    if (fs.exists(versionFileName)) {
      fs.delete(versionFileName, false)
    }
    val finalActions = allDirSummary.map(
      dir => {
        val (filePath, name) = if (isBucketTable) {
          (
            clickHouseTableV2.deltaLog.dataPath.toString + "/" + dir._8 + "/" + dir._1,
            dir._8 + "/" + dir._1)
        } else {
          (clickHouseTableV2.deltaLog.dataPath.toString + "/" + dir._1, dir._1)
        }
        AddFileTags.partsInfoToAddFile(
          clickHouseTableV2.catalogTable.get.identifier.database.get,
          clickHouseTableV2.catalogTable.get.identifier.table,
          clickHouseTableV2.snapshot.metadata.configuration("engine"),
          filePath,
          "",
          name,
          "",
          0L,
          dir._6,
          dir._6,
          dir._6,
          dir._7,
          dir._2,
          dir._3,
          dir._4,
          dir._5,
          dir._3,
          dir._8,
          dir._1,
          dataChange = true
        )
      })
    if (finalActions.nonEmpty) {
      // write transaction log
      logInfo(s"starting to generate commit info, finalActions.length=${finalActions.length} .")
      clickHouseTableV2.deltaLog.withNewTransaction {
        txn =>
          val operation =
            DeltaOperations.Write(SaveMode.Append, Option(Seq.empty[String]), None, None)
          txn.commit(finalActions, operation)
      }
    }
    finalActions
  }

}
