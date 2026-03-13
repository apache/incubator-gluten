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
package org.apache.gluten.utils

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFooterReaderShim, ParquetOptions}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.parquet.crypto.ParquetCryptoRuntimeException
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.ParquetMetadata

object ParquetMetadataUtils extends Logging {

  /**
   * Validates Parquet file metadata for unsupported features. Iterates files once, reads each
   * footer once, and runs all applicable checks against it.
   *
   * Checks always performed (correctness):
   *   - Variant annotation detection (Spark 4.1+)
   *
   * Checks gated by parquetMetadataValidationEnabled:
   *   - Encrypted footer / encrypted file
   *   - Unsupported codec
   *   - Legacy timezone metadata
   */
  def validateMetadata(
      rootPaths: Seq[String],
      hadoopConf: Configuration,
      parquetOptions: ParquetOptions,
      fileLimit: Int
  ): Option[String] = {
    val enabled = GlutenConfig.get.parquetMetadataValidationEnabled
    if (enabled || SparkShimLoader.getSparkShims.needsVariantAnnotationCheck) {
      parquetFooters(rootPaths, hadoopConf, fileLimit)
        .map(isUnsupportedMetadata(_, parquetOptions, enabled))
        .find(_.isDefined)
        .flatten
    } else {
      None
    }
  }

  def validateCodec(footer: ParquetMetadata): Option[String] = {
    val blocks = footer.getBlocks
    if (blocks.isEmpty) {
      return None
    }
    val codec = blocks.get(0).getColumns.get(0).getCodec
    val unsupportedCodec = SparkShimLoader.getSparkShims.unsupportedCodec
    if (unsupportedCodec.contains(codec)) {
      return Some(s"Unsupported codec ${codec.name()}.")
    }
    None
  }

  /**
   * Iterates over Parquet files under rootPaths, reads footer once per file. Returns an iterator of
   * Either[Exception, ParquetMetadata] where Left represents a readFooter failure.
   */
  private def parquetFooters(
      rootPaths: Seq[String],
      hadoopConf: Configuration,
      fileLimit: Int
  ): Iterator[Either[Exception, ParquetMetadata]] = {
    rootPaths.iterator.flatMap {
      rootPath =>
        val fs = new Path(rootPath).getFileSystem(hadoopConf)
        try {
          val filesIterator = fs.listFiles(new Path(rootPath), true)
          new Iterator[LocatedFileStatus] {
            def hasNext: Boolean = filesIterator.hasNext
            def next(): LocatedFileStatus = filesIterator.next()
          }.take(fileLimit)
            .map {
              fileStatus =>
                try {
                  Right(
                    ParquetFooterReaderShim
                      .readFooter(hadoopConf, fileStatus, ParquetMetadataConverter.NO_FILTER))
                } catch {
                  case e: Exception => Left(e)
                }
            }
        } catch {
          case e: Exception =>
            logWarning("Catch exception when validating parquet file metadata", e)
            Iterator.empty
        }
    }
  }

  private def isUnsupportedMetadata(
      footerOrError: Either[Exception, ParquetMetadata],
      parquetOptions: ParquetOptions,
      metadataValidationEnabled: Boolean): Option[String] = {
    footerOrError match {
      case Left(e)
          if metadataValidationEnabled &&
            ExceptionUtils.hasCause(e, classOf[ParquetCryptoRuntimeException]) =>
        Some("Encrypted Parquet footer detected.")
      case Left(_: RuntimeException) =>
        // Ignored as it's could be a "Not a Parquet file" exception.
        None
      case Left(e) =>
        logWarning("Catch exception when validating parquet file metadata", e)
        None
      case Right(footer) =>
        // Always-on check (correctness).
        if (SparkShimLoader.getSparkShims.shouldFallbackForParquetVariantAnnotation(footer)) {
          Some("Variant annotation detected in Parquet file.")
        } else if (metadataValidationEnabled) {
          // Previous Spark3.4 version uses toString to check if the data is encrypted,
          // so place the check to the end
          validateCodec(footer)
            .orElse(isTimezoneFoundInMetadata(footer, parquetOptions))
            .orElse(Option.when(SparkShimLoader.getSparkShims.isParquetFileEncrypted(footer))(
              "Encrypted Parquet file detected."))
        } else {
          None
        }
    }
  }

  private def isTimezoneFoundInMetadata(
      footer: ParquetMetadata,
      parquetOptions: ParquetOptions): Option[String] = {
    val footerFileMetaData = footer.getFileMetaData
    val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
    val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead
    val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get,
      datetimeRebaseModeInRead)
    val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get,
      int96RebaseModeInRead)
    if (datetimeRebaseSpec.originTimeZone.nonEmpty) {
      return Some("Legacy timezone found.")
    }
    if (int96RebaseSpec.originTimeZone.nonEmpty) {
      return Some("Legacy timezone found.")
    }
    None
  }

}
