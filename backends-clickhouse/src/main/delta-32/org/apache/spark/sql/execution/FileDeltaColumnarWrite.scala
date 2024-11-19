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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.delta.files.{FileDelayedCommitProtocol, MergeTreeDelayedCommitProtocol2}
import org.apache.spark.sql.execution.datasources.{ExecutedWriteSummary, WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

case class DeltaFileCommitInfo(committer: FileDelayedCommitProtocol)
  extends (NativeFileWriteResult => Unit) {
  val addedFiles: ArrayBuffer[(Map[String, String], String)] =
    new ArrayBuffer[(Map[String, String], String)]
  override def apply(stat: NativeFileWriteResult): Unit = {
    if (stat.partition_id == "__NO_PARTITION_ID__") {
      addedFiles.append((Map.empty[String, String], stat.filename))
    } else {
      val partitionValues = committer.parsePartitions(stat.partition_id)
      addedFiles.append((partitionValues, stat.relativePath))
    }
  }

  def result: Seq[(Map[String, String], String)] = addedFiles.toSeq
}

case class FileDeltaColumnarWrite(
    override val jobTrackerID: String,
    override val description: WriteJobDescription,
    override val committer: FileDelayedCommitProtocol)
  extends CHColumnarWrite[FileDelayedCommitProtocol]
  with Logging {

  override def doSetupNativeTask(): Unit = {
    assert(description.path == committer.outputPath)
    val nameSpec = CreateFileNameSpec(taskAttemptContext, description)
    val writePath = description.path
    val writeFileName = committer.getFileName(taskAttemptContext, nameSpec.suffix, Map.empty)
    logDebug(s"Native staging write path: $writePath and file name: $writeFileName")
    BackendsApiManager.getIteratorApiInstance.injectWriteFilesTempPath(writePath, writeFileName)
  }

  private def doCollectNativeResult(stats: Seq[InternalRow])
      : Option[(Seq[(Map[String, String], String)], ExecutedWriteSummary)] = {

    // Write an empty iterator
    if (stats.isEmpty) {
      None
    } else {
      val x = deltaWriteJobStatsTracker
        .map(
          e => {
            val r = e._2.transform {
              case ae: AggregateExpression
                  if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
                ae.aggregateFunction.asInstanceOf[DeclarativeAggregate].evaluateExpression
            }
            val z = Seq(
              AttributeReference("filename", StringType, nullable = false)(),
              AttributeReference("partition_id", StringType, nullable = false)())
            val s =
              e._2
                .collect {
                  case ae: AggregateExpression
                      if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
                    ae.aggregateFunction.asInstanceOf[DeclarativeAggregate]
                }
                .asInstanceOf[Seq[DeclarativeAggregate]]
                .flatMap(_.aggBufferAttributes)
            UnsafeProjection.create(
              exprs = Seq(r),
              inputSchema = z :++ s
            )
          })
        .orNull
      // stats.map(row => x.apply(row).getString(0)).foreach(println)
      // process stats
      val commitInfo = DeltaFileCommitInfo(committer)
      val basicNativeStat = NativeBasicWriteTaskStatsTracker(description, basicWriteJobStatsTracker)
      val basicNativeStats = Seq(commitInfo, basicNativeStat)
      NativeStatCompute(stats)(basicNativeStats)
      Some(
        (
          commitInfo.result,
          ExecutedWriteSummary(updatedPartitions = Set.empty, stats = Seq(basicNativeStat.result))))
    }
  }

  override def commitTask(writeResults: Seq[InternalRow]): Option[WriteTaskResult] = {
    doCollectNativeResult(writeResults).map {
      case (addedFiles, summary) =>
        require(addedFiles.nonEmpty, "No files to commit")

        committer.updateAddedFiles(addedFiles)

        val (taskCommitMessage, taskCommitTime) = Utils.timeTakenMs {
          committer.commitTask(taskAttemptContext)
        }

        // Just for update task commit time
        description.statsTrackers.foreach {
          stats => stats.newTaskInstance().getFinalStats(taskCommitTime)
        }
        WriteTaskResult(taskCommitMessage, summary)
    }
  }
}

object CHDeltaColumnarWrite {
  def apply(
      jobTrackerID: String,
      description: WriteJobDescription,
      committer: FileCommitProtocol): CHColumnarWrite[FileCommitProtocol] = committer match {
    case c: FileDelayedCommitProtocol =>
      FileDeltaColumnarWrite(jobTrackerID, description, c)
        .asInstanceOf[CHColumnarWrite[FileCommitProtocol]]
    case m: MergeTreeDelayedCommitProtocol2 =>
      MergeTreeDeltaColumnarWrite(jobTrackerID, description, m)
        .asInstanceOf[CHColumnarWrite[FileCommitProtocol]]
    case _ =>
      throw new GlutenNotSupportException(
        s"Unsupported committer type: ${committer.getClass.getSimpleName}")
  }
}
