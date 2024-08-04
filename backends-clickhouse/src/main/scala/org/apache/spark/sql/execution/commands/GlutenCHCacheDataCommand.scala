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
package org.apache.spark.sql.execution.commands

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.substrait.rel.ExtensionTableBuilder

import org.apache.spark.affinity.CHAffinity
import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.rpc.GlutenRpcMessages.{CacheLoadResult, GlutenMergeTreeCacheLoad}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, GreaterThanOrEqual, IsNotNull, Literal}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.commands.GlutenCHCacheDataCommand.toExecutorId
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.types.{BooleanType, StringType}
import org.apache.spark.util.ThreadUtils

import org.apache.hadoop.fs.Path

import java.net.URI
import java.util.{ArrayList => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.Duration

case class GlutenCHCacheDataCommand(
    onlyMetaCache: Boolean,
    asynExecute: Boolean,
    selectedColuman: Option[Seq[String]],
    path: Option[String],
    table: Option[TableIdentifier],
    tsfilter: Option[String],
    partitionColumn: Option[String],
    partitionValue: Option[String],
    tablePropertyOverrides: Map[String, String]
) extends LeafRunnableCommand {

  override def output: Seq[Attribute] = Seq(
    AttributeReference("result", BooleanType, nullable = false)(),
    AttributeReference("reason", StringType, nullable = false)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val pathToCache =
      if (path.nonEmpty) {
        new Path(path.get)
      } else if (table.nonEmpty) {
        DeltaTableIdentifier(sparkSession, table.get) match {
          case Some(id) if id.path.nonEmpty =>
            new Path(id.path.get)
          case _ =>
            new Path(sparkSession.sessionState.catalog.getTableMetadata(table.get).location)
        }
      } else {
        throw DeltaErrors.missingTableIdentifierException("CACHE DATA")
      }

    val baseDeltaPath = DeltaTableUtils.findDeltaTableRoot(sparkSession, pathToCache)
    if (baseDeltaPath.isDefined) {
      if (baseDeltaPath.get != pathToCache) {
        throw DeltaErrors.vacuumBasePathMissingException(baseDeltaPath.get)
      }
    }

    val deltaLog = DeltaLog.forTable(sparkSession, pathToCache)
    if (!deltaLog.tableExists) {
      throw DeltaErrors.notADeltaTableException(
        "CACHE DATA",
        DeltaTableIdentifier(path = Some(pathToCache.toString)))
    }

    val snapshot = deltaLog.update()

    require(
      snapshot.version >= 0,
      "No state defined for this table. Is this really " +
        "a Delta table? Refusing to garbage collect.")

    val allColumns = snapshot.dataSchema.fieldNames.toSeq
    val selectedColumns = if (selectedColuman.nonEmpty) {
      selectedColuman.get
        .filter(allColumns.contains(_))
        .map(ConverterUtils.normalizeColName)
        .toSeq
    } else {
      allColumns.map(ConverterUtils.normalizeColName)
    }

    val selectedAddFiles = if (tsfilter.isDefined) {
      val allParts = DeltaAdapter.snapshotFilesForScan(snapshot, Seq.empty, Seq.empty, false)
      allParts.files.filter(_.modificationTime >= tsfilter.get.toLong).toSeq
    } else if (partitionColumn.isDefined && partitionValue.isDefined) {
      val partitionColumns = snapshot.metadata.partitionSchema.fieldNames
      require(
        partitionColumns.contains(partitionColumn.get),
        s"the partition column ${partitionColumn.get} is invalid.")
      val partitionColumnField = snapshot.metadata.partitionSchema(partitionColumn.get)

      val partitionColumnAttr = AttributeReference(
        ConverterUtils.normalizeColName(partitionColumn.get),
        partitionColumnField.dataType,
        partitionColumnField.nullable)()
      val isNotNullExpr = IsNotNull(partitionColumnAttr)
      val greaterThanOrEqual = GreaterThanOrEqual(partitionColumnAttr, Literal(partitionValue.get))
      DeltaAdapter
        .snapshotFilesForScan(
          snapshot,
          Seq(partitionColumnAttr),
          Seq(isNotNullExpr, greaterThanOrEqual),
          false)
        .files
    } else {
      DeltaAdapter.snapshotFilesForScan(snapshot, Seq.empty, Seq.empty, false).files
    }

    val executorIdsToAddFiles =
      scala.collection.mutable.Map[String, ArrayBuffer[AddMergeTreeParts]]()
    val executorIdsToParts = scala.collection.mutable.Map[String, String]()
    executorIdsToAddFiles.put(
      GlutenCHCacheDataCommand.ALL_EXECUTORS,
      new ArrayBuffer[AddMergeTreeParts]())
    selectedAddFiles.foreach(
      addFile => {
        val mergeTreePart = addFile.asInstanceOf[AddMergeTreeParts]
        val partName = mergeTreePart.name
        val tableUri = URI.create(mergeTreePart.tablePath)
        val relativeTablePath = if (tableUri.getPath.startsWith("/")) {
          tableUri.getPath.substring(1)
        } else tableUri.getPath

        val locations = CHAffinity.getNativeMergeTreePartLocations(partName, relativeTablePath)

        if (locations.isEmpty) {
          // non soft affinity
          executorIdsToAddFiles
            .get(GlutenCHCacheDataCommand.ALL_EXECUTORS)
            .get
            .append(mergeTreePart)
        } else {
          locations.foreach(
            executor => {
              if (!executorIdsToAddFiles.contains(executor)) {
                executorIdsToAddFiles.put(executor, new ArrayBuffer[AddMergeTreeParts]())
              }
              executorIdsToAddFiles.get(executor).get.append(mergeTreePart)
            })
        }
      })

    executorIdsToAddFiles.foreach(
      value => {
        val parts = value._2
        val executorId = value._1
        if (parts.nonEmpty) {
          val onePart = parts(0)
          val partNameList = parts.map(_.name).toSeq
          // starts and lengths is useless for write
          val partRanges = Seq.range(0L, partNameList.length).map(_ => long2Long(0L)).asJava

          val extensionTableNode = ExtensionTableBuilder.makeExtensionTable(
            -1,
            -1,
            onePart.database,
            onePart.table,
            ClickhouseSnapshot.genSnapshotId(snapshot),
            onePart.tablePath,
            pathToCache.toString,
            snapshot.metadata.configuration.getOrElse("orderByKey", ""),
            snapshot.metadata.configuration.getOrElse("lowCardKey", ""),
            snapshot.metadata.configuration.getOrElse("minmaxIndexKey", ""),
            snapshot.metadata.configuration.getOrElse("bloomfilterIndexKey", ""),
            snapshot.metadata.configuration.getOrElse("setIndexKey", ""),
            snapshot.metadata.configuration.getOrElse("primaryKey", ""),
            partNameList.asJava,
            partRanges,
            partRanges,
            ConverterUtils.convertNamedStructJson(snapshot.metadata.schema),
            snapshot.metadata.configuration.asJava,
            new JList[String]()
          )

          executorIdsToParts.put(executorId, extensionTableNode.getExtensionTableStr)
        }
      })

    // send rpc call
    if (executorIdsToParts.contains(GlutenCHCacheDataCommand.ALL_EXECUTORS)) {
      // send all parts to all executors
      val tableMessage = executorIdsToParts.get(GlutenCHCacheDataCommand.ALL_EXECUTORS).get
      if (asynExecute) {
        GlutenDriverEndpoint.executorDataMap.forEach(
          (executorId, executor) => {
            executor.executorEndpointRef.send(
              GlutenMergeTreeCacheLoad(tableMessage, selectedColumns.toSet.asJava))
          })
        Seq(Row(true, ""))
      } else {
        val futureList = ArrayBuffer[Future[CacheLoadResult]]()
        val resultList = ArrayBuffer[CacheLoadResult]()
        GlutenDriverEndpoint.executorDataMap.forEach(
          (executorId, executor) => {
            futureList.append(
              executor.executorEndpointRef.ask[CacheLoadResult](
                GlutenMergeTreeCacheLoad(tableMessage, selectedColumns.toSet.asJava)
              ))
          })
        futureList.foreach(
          f => {
            resultList.append(ThreadUtils.awaitResult(f, Duration.Inf))
          })
        if (resultList.exists(!_.success)) {
          Seq(Row(false, resultList.filter(!_.success).map(_.reason).mkString(";")))
        } else {
          Seq(Row(true, ""))
        }
      }
    } else {
      if (asynExecute) {
        executorIdsToParts.foreach(
          value => {
            val executorData = GlutenDriverEndpoint.executorDataMap.get(toExecutorId(value._1))
            if (executorData != null) {
              executorData.executorEndpointRef.send(
                GlutenMergeTreeCacheLoad(value._2, selectedColumns.toSet.asJava))
            } else {
              throw new GlutenException(
                s"executor ${value._1} not found," +
                  s" all executors are ${GlutenDriverEndpoint.executorDataMap.toString}")
            }
          })
        Seq(Row(true, ""))
      } else {
        val futureList = ArrayBuffer[Future[CacheLoadResult]]()
        val resultList = ArrayBuffer[CacheLoadResult]()
        executorIdsToParts.foreach(
          value => {
            val executorData = GlutenDriverEndpoint.executorDataMap.get(toExecutorId(value._1))
            if (executorData != null) {
              futureList.append(
                executorData.executorEndpointRef.ask[CacheLoadResult](
                  GlutenMergeTreeCacheLoad(value._2, selectedColumns.toSet.asJava)
                ))
            } else {
              throw new GlutenException(
                s"executor ${value._1} not found," +
                  s" all executors are ${GlutenDriverEndpoint.executorDataMap.toString}")
            }
          })
        futureList.foreach(
          f => {
            resultList.append(ThreadUtils.awaitResult(f, Duration.Inf))
          })
        if (resultList.exists(!_.success)) {
          Seq(Row(false, resultList.filter(!_.success).map(_.reason).mkString(";")))
        } else {
          Seq(Row(true, ""))
        }
      }
    }
  }
}

object GlutenCHCacheDataCommand {
  val ALL_EXECUTORS = "allExecutors"

  private def toExecutorId(executorId: String): String =
    executorId.split("_").last
}
