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
package org.apache.spark.sql.execution.datasources

import io.glutenproject.backendsapi.BackendsApiManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.{FileScan, TextBasedFileScan}
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory

class GlutenTextBasedScanWrapper(
    scan: TextBasedFileScan,
    schema: StructType,
    session: SparkSession,
    options: CaseInsensitiveStringMap)
  extends TextBasedFileScan(session, options) {

  lazy val codecFactory: CompressionCodecFactory =
    new CompressionCodecFactory(session.sessionState.newHadoopConf())

  override def sparkSession: SparkSession = scan.sparkSession

  override def fileIndex: PartitioningAwareFileIndex = scan.fileIndex

  override def readDataSchema: StructType = scan.readDataSchema

  override def readPartitionSchema: StructType = scan.readPartitionSchema

  override def partitionFilters: Seq[Expression] = scan.partitionFilters

  override def dataFilters: Seq[Expression] = scan.dataFilters

  override def createReaderFactory(): PartitionReaderFactory = scan.createReaderFactory()

  override def isSplitable(path: Path): Boolean =
    super.isSplitable(path) && compressionSplittable(path)

  def dataSchema: StructType = schema

  def withFilters(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan = {
    scan match {
      case t: TextScan => t.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
      case j: JsonScan => j.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
      case _ => scan
    }
  }

  def compressionSplittable(path: Path): Boolean = {
    val codec = codecFactory.getCodec(path)
    if (codec == null) {
      true
    } else {
      BackendsApiManager.getValidatorApiInstance.doCompressionSplittableValidate(
        codec.getClass.getSimpleName)
    }
  }

  def getScan: TextBasedFileScan = scan
}

object GlutenTextBasedScanWrapper {

  def wrap(scan: TextScan, dataSchema: StructType): GlutenTextBasedScanWrapper = {
    new GlutenTextBasedScanWrapper(scan, dataSchema, scan.sparkSession, scan.options)
  }

  def wrap(scan: JsonScan, dataSchema: StructType): GlutenTextBasedScanWrapper = {
    new GlutenTextBasedScanWrapper(scan, dataSchema, scan.sparkSession, scan.options)
  }
}
