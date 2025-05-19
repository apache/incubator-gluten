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

import org.apache.gluten.expression.ConverterUtils

import org.apache.spark.affinity.CHAffinity
import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.rpc.GlutenRpcMessages.{CacheJobInfo, GlutenMergeTreeCacheLoad}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, GreaterThanOrEqual, IsNotNull, Literal}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.commands.GlutenCacheBase._
import org.apache.spark.sql.execution.datasources.clickhouse.ExtensionTableBuilder
import org.apache.spark.sql.execution.datasources.mergetree.{PartSerializer, StorageMeta}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.types.{BooleanType, StringType}

import org.apache.hadoop.fs.Path

import java.net.URI
import java.util.{ArrayList => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

case class GlutenCHCacheDataCommand(
    onlyMetaCache: Boolean,
    asynExecute: Boolean,
    selectedColumn: Option[Seq[String]],
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
    val selectedColumns = if (selectedColumn.nonEmpty) {
      selectedColumn.get
        .filter(allColumns.contains(_))
        .map(ConverterUtils.normalizeColName)
        .toSeq
    } else {
      allColumns.map(ConverterUtils.normalizeColName)
    }

    val selectedAddFiles = if (tsfilter.isDefined) {
      val allParts =
        DeltaAdapter.snapshotFilesForScan(snapshot, Seq.empty, Seq.empty, keepNumRecords = false)
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
          keepNumRecords = false)
        .files
    } else {
      DeltaAdapter
        .snapshotFilesForScan(snapshot, Seq.empty, Seq.empty, keepNumRecords = false)
        .files
    }

    val executorIdsToAddFiles =
      scala.collection.mutable.Map[String, ArrayBuffer[AddMergeTreeParts]]()
    val executorIdsToParts = scala.collection.mutable.Map[String, String]()
    executorIdsToAddFiles.put(ALL_EXECUTORS, new ArrayBuffer[AddMergeTreeParts]())
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
          executorIdsToAddFiles(ALL_EXECUTORS)
            .append(mergeTreePart)
        } else {
          locations.foreach(
            executor => {
              if (!executorIdsToAddFiles.contains(executor)) {
                executorIdsToAddFiles.put(executor, new ArrayBuffer[AddMergeTreeParts]())
              }
              executorIdsToAddFiles(executor).append(mergeTreePart)
            })
        }
      })

    executorIdsToAddFiles.foreach(
      value => {
        val parts = value._2
        val executorId = value._1
        if (parts.nonEmpty) {
          val onePart = parts(0)
          val extensionTableNode = ExtensionTableBuilder.makeExtensionTable(
            onePart.database,
            onePart.table,
            ClickhouseSnapshot.genSnapshotId(snapshot),
            onePart.tablePath,
            pathToCache.toString,
            snapshot.metadata.configuration
              .getOrElse("orderByKey", StorageMeta.DEFAULT_ORDER_BY_KEY),
            snapshot.metadata.configuration.getOrElse("lowCardKey", ""),
            snapshot.metadata.configuration.getOrElse("minmaxIndexKey", ""),
            snapshot.metadata.configuration.getOrElse("bloomfilterIndexKey", ""),
            snapshot.metadata.configuration.getOrElse("setIndexKey", ""),
            snapshot.metadata.configuration.getOrElse("primaryKey", ""),
            PartSerializer.fromPartNames(parts.map(p => new Path(new URI(p.name)).toString).toSeq),
            snapshot.metadata.schema,
            snapshot.metadata.configuration.asJava,
            new JList[String]()
          )

          executorIdsToParts.put(executorId, extensionTableNode.getExtensionTableStr)
        }
      })
    val futureList = ArrayBuffer[(String, Future[CacheJobInfo])]()
    if (executorIdsToParts.contains(ALL_EXECUTORS)) {
      // send all parts to all executors
      val tableMessage = executorIdsToParts(ALL_EXECUTORS)
      GlutenDriverEndpoint.executorDataMap.forEach(
        (executorId, executor) => {
          futureList.append(
            (
              executorId,
              executor.executorEndpointRef.ask[CacheJobInfo](
                GlutenMergeTreeCacheLoad(tableMessage, selectedColumns.toSet.asJava, onlyMetaCache)
              )))
        })
    } else {
      executorIdsToParts.foreach(
        value => {
          checkExecutorId(value._1)
          val executorData = GlutenDriverEndpoint.executorDataMap.get(toExecutorId(value._1))
          futureList.append(
            (
              value._1,
              executorData.executorEndpointRef.ask[CacheJobInfo](
                GlutenMergeTreeCacheLoad(value._2, selectedColumns.toSet.asJava, onlyMetaCache)
              )))
        })
    }

    getResult(futureList, asynExecute)
  }
}
