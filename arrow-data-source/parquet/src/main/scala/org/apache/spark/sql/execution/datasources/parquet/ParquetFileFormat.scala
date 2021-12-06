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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.IOException
import java.net.URI

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

import com.intel.oap.spark.sql.execution.datasources.arrow.ArrowFileFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, JobID, OutputCommitter, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{Footer, ParquetFileReader, ParquetInputFormat, ParquetOutputCommitter, ParquetOutputFormat, ParquetRecordReader}
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.schema.MessageType

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, OutputWriter, OutputWriterFactory, PartitionedFile, RecordReaderIterator, SchemaMergeUtils}
import org.apache.spark.sql.execution.datasources.parquet.ParquetSQLConf._
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, StructType, UserDefinedType}
import org.apache.spark.util.{SerializableConfiguration, ThreadUtils}

/**
 * This is expected to overwrite built-in ParquetFileFormat. Read is redirected to ArrowFileFormat's
 * read.
 */
class ParquetFileFormat
  extends ArrowFileFormat
    with DataSourceRegister
    with Logging
    with Serializable {

  override def shortName(): String = "parquet"

  override def toString: String = ParquetFileFormatIndicator.OVERWRITTEN_INDICATOR

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[ParquetFileFormat]

  /**
   * We copy following codes from Spark's built-in ParquetFileFormat. It's not suggested to
   * change any of the logic to make sure we are on the same boat with Spark when it comes
   * to Parquet write.
   */
  override def prepareWrite(sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)

    val conf = ContextUtil.getConfiguration(job)

    val committerClass =
      conf.getClass(
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[OutputCommitter])

    if (conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) == null) {
      logInfo("Using default output committer for Parquet: " +
        classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo("Using user defined output committer for Parquet: " + committerClass.getCanonicalName)
    }

    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      committerClass,
      classOf[OutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here, because we override
    // it in `ParquetOutputWriter` to support appending and dynamic partitioning.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetWriteSupport])

    // This metadata is useful for keeping UDTs like Vector/Matrix.
    ParquetWriteSupport.setSchema(dataSchema, conf)

    // Sets flags for `ParquetWriteSupport`, which converts Catalyst schema to Parquet
    // schema and writes actual rows to Parquet files.
    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sparkSession.sessionState.conf.writeLegacyParquetFormat.toString)

    conf.set(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      sparkSession.sessionState.conf.parquetOutputTimestampType.toString)

    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, parquetOptions.compressionCodecClassName)

    // SPARK-15719: Disables writing Parquet summary files by default.
    if (conf.get(ParquetOutputFormat.JOB_SUMMARY_LEVEL) == null
      && conf.get(ParquetOutputFormat.ENABLE_JOB_SUMMARY) == null) {
      conf.setEnum(ParquetOutputFormat.JOB_SUMMARY_LEVEL, JobSummaryLevel.NONE)
    }

    if (ParquetOutputFormat.getJobSummaryLevel(conf) != JobSummaryLevel.NONE
      && !classOf[ParquetOutputCommitter].isAssignableFrom(committerClass)) {
      // output summary is requested, but the class is not a Parquet Committer
      logWarning(s"Committer $committerClass is not a ParquetOutputCommitter and cannot" +
        s" create job summaries. " +
        s"Set Parquet option ${ParquetOutputFormat.JOB_SUMMARY_LEVEL} to NONE.")
    }

    new OutputWriterFactory {
      // This OutputWriterFactory instance is deserialized when writing Parquet files on the
      // executor side without constructing or deserializing ParquetFileFormat. Therefore, we hold
      // another reference to ParquetLogRedirector.INSTANCE here to ensure the latter class is
      // initialized.
      private val parquetLogRedirector = ParquetLogRedirector.INSTANCE

      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new ParquetOutputWriter(path, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        CodecConfig.from(context).getCodec.getExtension + ".parquet"
      }
    }
  }

  override def inferSchema(sparkSession: SparkSession,
                            parameters: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    val overwrite = sparkSession.sqlContext.conf.overwriteParquetDataSourceRead
    if (overwrite) {
      return super.inferSchema(sparkSession, parameters, files)
    }
    ParquetUtils.inferSchema(sparkSession, parameters, files)
  }

  /**
   * Returns whether the reader will return the rows as batch or not.
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    val overwrite = sparkSession.sqlContext.conf.overwriteParquetDataSourceRead
    if (overwrite) {
      return super.supportBatch(sparkSession, schema)
    }
    conf.parquetVectorizedReaderEnabled && conf.wholeStageEnabled &&
      schema.length <= conf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def vectorTypes(requiredSchema: StructType,
                            partitionSchema: StructType,
                            sqlConf: SQLConf): Option[Seq[String]] = {
    val overwrite = sqlConf.overwriteParquetDataSourceRead
    if (overwrite) {
      return super.vectorTypes(requiredSchema, partitionSchema, sqlConf)
    }
    Option(Seq.fill(requiredSchema.fields.length + partitionSchema.fields.length)(
      if (!sqlConf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
    ))
  }

  override def isSplitable(sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = {
    val overwrite = sparkSession.sqlContext.conf.overwriteParquetDataSourceRead
    if (overwrite) {
      return super.isSplitable(sparkSession, options, path)
    }
    true
  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                               dataSchema: StructType,
                                               partitionSchema: StructType,
                                               requiredSchema: StructType,
                                               filters: Seq[Filter],
                                               options: Map[String, String],
                                               hadoopConf: Configuration):
  (PartitionedFile) => Iterator[InternalRow] = {
    val overwrite = sparkSession.sqlContext.conf.overwriteParquetDataSourceRead
    if (overwrite) {
      return super.buildReaderWithPartitionValues(sparkSession, dataSchema,
        partitionSchema, requiredSchema, filters, options, hadoopConf)
    }
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    ParquetWriteSupport.setSchema(requiredSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    // TODO: if you move this into the closure it reverts to the default values.
    // If true, enable using the custom RecordReader for parquet. This only works for
    // a subset of the types (no complex types).
    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val enableVectorizedReader: Boolean =
      sqlConf.parquetVectorizedReaderEnabled &&
        resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
    val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
    val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
    val capacity = sqlConf.parquetVectorizedReaderBatchSize
    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
    // Whole stage codegen (PhysicalRDD) is able to deal with batches directly
    val returningBatch = supportBatch(sparkSession, resultSchema)
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)

      val filePath = new Path(new URI(file.filePath))
      val split =
        new org.apache.parquet.hadoop.ParquetInputSplit(
          filePath,
          file.start,
          file.start + file.length,
          file.length,
          Array.empty,
          null)

      val sharedConf = broadcastedHadoopConf.value.value

      lazy val footerFileMetaData =
        ParquetFileReader.readFooter(sharedConf, filePath, SKIP_ROW_GROUPS).getFileMetaData
      // Try to push down filters when filter push-down is enabled.
      val pushed = if (enableParquetFilterPushDown) {
        val parquetSchema = footerFileMetaData.getSchema
        val parquetFilters = new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
          pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
        filters
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
          .flatMap(parquetFilters.createFilter(_))
          .reduceOption(FilterApi.and)
      } else {
        None
      }

      // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
      // *only* if the file was created by something other than "parquet-mr", so check the actual
      // writer here for this file.  We have to do this per-file, as each file in the table may
      // have different writers.
      // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
      def isCreatedByParquetMr: Boolean =
        footerFileMetaData.getCreatedBy().startsWith("parquet-mr")

      val convertTz =
        if (timestampConversion && !isCreatedByParquetMr) {
          Some(DateTimeUtils.getZoneId(sharedConf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
        } else {
          None
        }

      val datetimeRebaseMode = DataSourceUtils.datetimeRebaseMode(
        footerFileMetaData.getKeyValueMetaData.get,
        SQLConf.get.getConf(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ))

      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val hadoopAttemptContext =
        new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

      // Try to push down filters when filter push-down is enabled.
      // Notice: This push-down is RowGroups level, not individual records.
      if (pushed.isDefined) {
        ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
      }
      val taskContext = Option(TaskContext.get())
      if (enableVectorizedReader) {
        val vectorizedReader = new VectorizedParquetRecordReader(
          convertTz.orNull,
          datetimeRebaseMode.toString,
          "",
          enableOffHeapColumnVector && taskContext.isDefined,
          capacity)
        val iter = new RecordReaderIterator(vectorizedReader)
        // SPARK-23457 Register a task completion listener before `initialization`.
        taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
        vectorizedReader.initialize(split, hadoopAttemptContext)
        logDebug(s"Appending $partitionSchema ${file.partitionValues}")
        vectorizedReader.initBatch(partitionSchema, file.partitionValues)
        if (returningBatch) {
          vectorizedReader.enableReturningBatches()
        }

        // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
        iter.asInstanceOf[Iterator[InternalRow]]
      } else {
        logDebug(s"Falling back to parquet-mr")
        // ParquetRecordReader returns InternalRow
        val readSupport = new ParquetReadSupport(
          convertTz, enableVectorizedReader = false, datetimeRebaseMode, SQLConf.LegacyBehaviorPolicy.LEGACY)
        val reader = if (pushed.isDefined && enableRecordFilter) {
          val parquetFilter = FilterCompat.get(pushed.get, null)
          new ParquetRecordReader[InternalRow](readSupport, parquetFilter)
        } else {
          new ParquetRecordReader[InternalRow](readSupport)
        }
        val iter = new RecordReaderIterator[InternalRow](reader)
        // SPARK-23457 Register a task completion listener before `initialization`.
        taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
        reader.initialize(split, hadoopAttemptContext)

        val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
        val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

        if (partitionSchema.length == 0) {
          // There is no partition columns
          iter.map(unsafeProjection)
        } else {
          val joinedRow = new JoinedRow()
          iter.map(d => unsafeProjection(joinedRow(d, file.partitionValues)))
        }
      }
    }
  }

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType) }

    case ArrayType(elementType, _) => supportDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType) && supportDataType(valueType)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _ => false
  }
}

object ParquetFileFormat extends Logging {
  /**
   * Reads Spark SQL schema from a Parquet footer.  If a valid serialized Spark SQL schema string
   * can be found in the file metadata, returns the deserialized [[StructType]], otherwise, returns
   * a [[StructType]] converted from the [[MessageType]] stored in this footer.
   */
  def readSchemaFromFooter(footer: Footer, converter: ParquetToSparkSchemaConverter): StructType = {
    val fileMetaData = footer.getParquetMetadata.getFileMetaData
    fileMetaData
      .getKeyValueMetaData
      .asScala.toMap
      .get(ParquetReadSupport.SPARK_METADATA_KEY)
      .flatMap(deserializeSchemaString)
      .getOrElse(converter.convert(fileMetaData.getSchema))
  }

  private def deserializeSchemaString(schemaString: String): Option[StructType] = {
    // Tries to deserialize the schema string as JSON first, then falls back to the case class
    // string parser (data generated by older versions of Spark SQL uses this format).
    Try(DataType.fromJson(schemaString).asInstanceOf[StructType]).recover {
      case _: Throwable =>
        logInfo(
          "Serialized Spark schema in Parquet key-value metadata is not in JSON format, " +
            "falling back to the deprecated DataType.fromCaseClassString parser.")
        LegacyTypeStringParser.parseString(schemaString).asInstanceOf[StructType]
    }.recoverWith {
      case cause: Throwable =>
        logWarning(
          "Failed to parse and ignored serialized Spark schema in " +
            s"Parquet key-value metadata:\n\t$schemaString", cause)
        Failure(cause)
    }.toOption
  }

  def mergeSchemasInParallel(
                              filesToTouch: Seq[FileStatus],
                              sparkSession: SparkSession): Option[StructType] = {
    val assumeBinaryIsString = sparkSession.sessionState.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sparkSession.sessionState.conf.isParquetINT96AsTimestamp

    val reader = (files: Seq[FileStatus], conf: Configuration, ignoreCorruptFiles: Boolean) => {
      // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
      val converter = new ParquetToSparkSchemaConverter(
        assumeBinaryIsString = assumeBinaryIsString,
        assumeInt96IsTimestamp = assumeInt96IsTimestamp)

      readParquetFootersInParallel(conf, files, ignoreCorruptFiles)
        .map(ParquetFileFormat.readSchemaFromFooter(_, converter))
    }

    SchemaMergeUtils.mergeSchemasInParallel(sparkSession, null, filesToTouch, reader)
  }

  private[parquet] def readParquetFootersInParallel(
                                                     conf: Configuration,
                                                     partFiles: Seq[FileStatus],
                                                     ignoreCorruptFiles: Boolean): Seq[Footer] = {
    ThreadUtils.parmap(partFiles, "readingParquetFooters", 8) { currentFile =>
      try {
        // Skips row group information since we only need the schema.
        // ParquetFileReader.readFooter throws RuntimeException, instead of IOException,
        // when it can't read the footer.
        Some(new Footer(currentFile.getPath(),
          ParquetFileReader.readFooter(
            conf, currentFile, SKIP_ROW_GROUPS)))
      } catch { case e: RuntimeException =>
        if (ignoreCorruptFiles) {
          logWarning(s"Skipped the footer in the corrupted file: $currentFile", e)
          None
        } else {
          throw new IOException(s"Could not read footer for file: $currentFile", e)
        }
      }
    }.flatten
  }
}
