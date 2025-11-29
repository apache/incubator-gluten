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
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFooterReader, ParquetOptions}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.ParquetMetadata

object ParquetMetadataUtils {

  /**
   * Validates whether Parquet metadata is unsupported for the given paths.
   *
   *   - If the file format is not Parquet, skip this check and return success.
   *   - If there is at least one Parquet file with encryption enabled, fail the validation.
   *
   * @param format
   *   File format, e.g., `ParquetReadFormat`
   * @param rootPaths
   *   List of file paths to scan
   * @param hadoopConf
   *   Hadoop configuration
   * @return
   *   [[Option[String]]] Empty if the Parquet metadata is supported. Fallback reason otherwise.
   */
  def validateMetadata(
      format: ReadFileFormat,
      rootPaths: Seq[String],
      hadoopConf: Configuration,
      parquetOptions: ParquetOptions,
      fileLimit: Int
  ): Option[String] = {
    rootPaths.foreach {
      rootPath =>
        val fs = new Path(rootPath).getFileSystem(hadoopConf)
        try {
          val reason =
            checkForUnexpectedMetadataWithLimit(
              fs,
              new Path(rootPath),
              hadoopConf,
              parquetOptions,
              fileLimit = fileLimit)
          if (reason.nonEmpty) {
            return reason
          }
        } catch {
          case e: Exception =>
        }
    }
    None
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
   * Check any Parquet file under the given path is with unexpected metadata using a recursive
   * iterator. Only the first `fileLimit` files are processed for efficiency.
   *
   * @param fs
   *   FileSystem to use
   * @param path
   *   Root path to check
   * @param conf
   *   Hadoop configuration
   * @param fileLimit
   *   Maximum number of files to inspect
   * @return
   *   True if an encrypted file is detected, false otherwise
   */
  private def checkForUnexpectedMetadataWithLimit(
      fs: FileSystem,
      path: Path,
      conf: Configuration,
      parquetOptions: ParquetOptions,
      fileLimit: Int
  ): Option[String] = {
    val isMetadataValidationEnabled =
      if (!GlutenConfig.get.parquetMetadataValidationEnabled) {
        return None
      }
    val isEncryptionValidationEnabled = GlutenConfig.get.parquetEncryptionValidationEnabled
    val filesIterator: RemoteIterator[LocatedFileStatus] = fs.listFiles(path, true)
    var checkedFileCount = 0
    while (filesIterator.hasNext && checkedFileCount < fileLimit) {
      val fileStatus = filesIterator.next()
      checkedFileCount += 1
      if (
        isEncryptionValidationEnabled && SparkShimLoader.getSparkShims.isParquetFileEncrypted(
          fileStatus,
          conf)
      ) {
        return Some("Encrypted Parquet file detected.")
      }
      // isMetadataValidationEnabled
      val metadataUnsupported = isUnsupportedMetadata(fileStatus, conf, parquetOptions)
      if (metadataUnsupported.isDefined) {
        return metadataUnsupported
      }
    }
    None
  }

  /**
   * Checks whether there are timezones set with Spark key SPARK_TIMEZONE_METADATA_KEY in the
   * Parquet metadata. In this case, the Parquet scan should fall back to vanilla Spark since Velox
   * doesn't yet support Spark legacy datetime.
   */
  private def isUnsupportedMetadata(
      fileStatus: LocatedFileStatus,
      conf: Configuration,
      parquetOptions: ParquetOptions): Option[String] = {
    val footer =
      try {
        ParquetFooterReader.readFooter(conf, fileStatus, ParquetMetadataConverter.NO_FILTER)
      } catch {
        case _: RuntimeException =>
          // Ignored as it's could be a "Not a Parquet file" exception.
          return None
      }
    val validationChecks = Seq(
      validateCodec(footer),
      isTimezoneFoundInMetadata(footer, parquetOptions)
    )

    for (check <- validationChecks) {
      if (check.isDefined) {
        return check
      }
    }
    None
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
