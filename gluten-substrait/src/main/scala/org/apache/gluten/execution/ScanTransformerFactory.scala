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
package org.apache.gluten.execution

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}

import java.util.ServiceLoader
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

object ScanTransformerFactory {

  private val scanTransformerMap = new ConcurrentHashMap[String, Class[_]]()

  def createFileSourceScanTransformer(
      scanExec: FileSourceScanExec): FileSourceScanExecTransformerBase = {
    val fileFormat = scanExec.relation.fileFormat
    lookupDataSourceScanTransformer(fileFormat.getClass.getName) match {
      case Some(clz) =>
        clz
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[DataSourceScanTransformerRegister]
          .createDataSourceTransformer(scanExec)
      case _ =>
        FileSourceScanExecTransformer(
          scanExec.relation,
          scanExec.output,
          scanExec.requiredSchema,
          scanExec.partitionFilters,
          scanExec.optionalBucketSet,
          scanExec.optionalNumCoalescedBuckets,
          scanExec.dataFilters,
          scanExec.tableIdentifier,
          scanExec.disableBucketedScan
        )
    }
  }

  def createBatchScanTransformer(batchScanExec: BatchScanExec): BatchScanExecTransformerBase = {
    val scan = batchScanExec.scan
    lookupDataSourceScanTransformer(scan.getClass.getName) match {
      case Some(clz) =>
        clz
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[DataSourceScanTransformerRegister]
          .createDataSourceV2Transformer(batchScanExec)
      case _ =>
        BatchScanExecTransformer(
          batchScanExec.output,
          batchScanExec.scan,
          batchScanExec.runtimeFilters,
          table = SparkShimLoader.getSparkShims.getBatchScanExecTable(batchScanExec)
        )
    }
  }

  def supportedBatchScan(scan: Scan): Boolean = scan match {
    case _: FileScan => true
    case _ => lookupDataSourceScanTransformer(scan.getClass.getName).nonEmpty
  }

  private def lookupDataSourceScanTransformer(scanClassName: String): Option[Class[_]] = {
    val clz = scanTransformerMap.computeIfAbsent(
      scanClassName,
      _ => {
        val loader = Option(Thread.currentThread().getContextClassLoader)
          .getOrElse(getClass.getClassLoader)
        val serviceLoader = ServiceLoader.load(classOf[DataSourceScanTransformerRegister], loader)
        serviceLoader.asScala
          .filter(service => scanClassName.contains(service.scanClassName))
          .toList match {
          case head :: Nil =>
            // there is exactly one registered alias
            head.getClass
          case _ => null
        }
      }
    )
    Option(clz)
  }
}
