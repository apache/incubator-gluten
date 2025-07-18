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
package org.apache.gluten.datasource.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters.mapAsScalaMapConverter

case class ArrowCSVScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
  extends FileScan {

  private lazy val parsedOptions: CSVOptions = new CSVOptions(
    options.asScala.toMap,
    columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord
  )

  override def isSplitable(path: Path): Boolean = {
    false
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap().asScala.toMap
    val hconf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hconf))
    val actualFilters =
      pushedFilters.filterNot(_.references.contains(parsedOptions.columnNameOfCorruptRecord))
    ArrowCSVPartitionReaderFactory(
      sparkSession.sessionState.conf,
      broadcastedConf,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      parsedOptions,
      actualFilters)
  }

  def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
}
