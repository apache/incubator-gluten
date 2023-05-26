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
package org.apache.spark.sql.hive.execution

import io.glutenproject.columnarbatch.{ArrowColumnarBatches, GlutenIndicatorVector}
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.spark.sql.execution.datasources.velox.DatasourceJniWrapper
import io.glutenproject.utils.GlutenArrowAbiUtil

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SPECULATION_ENABLED
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{FakeRow, FileFormat, OutputWriter, OutputWriterFactory, VeloxWriteQueue}
import org.apache.spark.sql.hive.{HiveInspectors, HiveTableUtil}
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.util.SerializableJobConf

import org.apache.arrow.c.ArrowSchema
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.{HiveFileFormatUtils, HiveOutputFormat}
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{JobConf, Reporter}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.codec.CodecConfig

import java.io.IOException
import java.net.URI

import scala.collection.JavaConverters._

/**
 * This file is copied from Spark
 *
 * Offload the parquet write of InsertIntoHiveDirCommand to velox backend when enable gluten plugin.
 *
 */
class HiveFileFormat(fileSinkConf: FileSinkDesc)
  extends FileFormat
  with DataSourceRegister
  with Logging {

  def this() = this(null)

  override def shortName(): String = "hive"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    throw QueryExecutionErrors.inferSchemaUnsupportedForHiveError()
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    if (
      fileSinkConf.tableInfo
        .getOutputFileFormatClassName()
        .equals("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
      && sparkSession.conf.contains("spark.plugins") && sparkSession.conf
        .get("spark.plugins")
        .equals("io.glutenproject.GlutenPlugin")
    ) {
      // Only offload parquet write to velox backend.
      new OutputWriterFactory {
        override def getFileExtension(context: TaskAttemptContext): String = {
          CodecConfig.from(context).getCodec.getExtension + ".parquet"
        }

        override def newInstance(
            path: String,
            dataSchema: StructType,
            context: TaskAttemptContext): OutputWriter = {
          val originPath = path

          URI.create(originPath) // validate uri
          val matcher = VeloxWriteQueue.TAILING_FILENAME_REGEX.matcher(originPath)
          if (!matcher.matches()) {
            throw new IllegalArgumentException("illegal out put file uri: " + originPath)
          }
          val fileName = matcher.group(2)

          val arrowSchema =
            SparkArrowUtil.toArrowSchema(dataSchema, SQLConf.get.sessionLocalTimeZone)
          val cSchema = ArrowSchema.allocateNew(ArrowBufferAllocators.contextInstance())
          var instanceId = -1L
          val datasourceJniWrapper = new DatasourceJniWrapper()
          val allocator = ArrowBufferAllocators.contextInstance()
          try {
            GlutenArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
            instanceId =
              datasourceJniWrapper.nativeInitDatasource(originPath, cSchema.memoryAddress())
          } catch {
            case e: IOException =>
              throw new RuntimeException(e)
          } finally {
            cSchema.close()
          }

          val writeQueue =
            new VeloxWriteQueue(
              instanceId,
              arrowSchema,
              allocator,
              datasourceJniWrapper,
              originPath)

          new OutputWriter {
            override def write(row: InternalRow): Unit = {
              val batch = row.asInstanceOf[FakeRow].batch
              if (batch.column(0).isInstanceOf[GlutenIndicatorVector]) {
                val giv = batch.column(0).asInstanceOf[GlutenIndicatorVector]
                giv.retain()
                writeQueue.enqueue(batch)
              } else {
                val offloaded =
                  ArrowColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance, batch)
                writeQueue.enqueue(offloaded)
              }
            }

            override def close(): Unit = {
              writeQueue.close()
              datasourceJniWrapper.close(instanceId)
            }

            // Do NOT add override keyword for compatibility on spark 3.1.
            def path(): String = {
              originPath
            }
          }
        }
      }
    } else {
      val conf = job.getConfiguration
      val tableDesc = fileSinkConf.getTableInfo
      conf.set("mapred.output.format.class", tableDesc.getOutputFileFormatClassName)

      // When speculation is on and output committer class name contains "Direct", we should warn
      // users that they may loss data if they are using a direct output committer.
      val speculationEnabled = sparkSession.sparkContext.conf.get(SPECULATION_ENABLED)
      val outputCommitterClass = conf.get("mapred.output.committer.class", "")
      if (speculationEnabled && outputCommitterClass.contains("Direct")) {
        val warningMessage =
          s"$outputCommitterClass may be an output committer that writes data directly to " +
            "the final location. Because speculation is enabled, this output committer may " +
            "cause data loss (see the case in SPARK-10063). If possible, please use an output " +
            "committer that does not have this behavior (e.g. FileOutputCommitter)."
        logWarning(warningMessage)
      }

      // Add table properties from storage handler to hadoopConf, so any custom storage
      // handler settings can be set to hadoopConf
      HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, conf, false)
      Utilities.copyTableJobPropertiesToConf(tableDesc, conf)

      // Avoid referencing the outer object.
      val fileSinkConfSer = fileSinkConf
      new OutputWriterFactory {
        private val jobConf = new SerializableJobConf(new JobConf(conf))
        @transient private lazy val outputFormat =
          jobConf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

        override def getFileExtension(context: TaskAttemptContext): String = {
          Utilities.getFileExtension(jobConf.value, fileSinkConfSer.getCompressed, outputFormat)
        }

        override def newInstance(
            path: String,
            dataSchema: StructType,
            context: TaskAttemptContext): OutputWriter = {
          new HiveOutputWriter(path, fileSinkConfSer, jobConf.value, dataSchema)
        }
      }
    }
  }
}

class HiveOutputWriter(
    val path: String,
    fileSinkConf: FileSinkDesc,
    jobConf: JobConf,
    dataSchema: StructType)
  extends OutputWriter
  with HiveInspectors {

  private def tableDesc = fileSinkConf.getTableInfo

  private val serializer = {
    val serializer =
      tableDesc.getDeserializerClass.getConstructor().newInstance().asInstanceOf[Serializer]
    serializer.initialize(jobConf, tableDesc.getProperties)
    serializer
  }

  private val hiveWriter = HiveFileFormatUtils.getHiveRecordWriter(
    jobConf,
    tableDesc,
    serializer.getSerializedClass,
    fileSinkConf,
    new Path(path),
    Reporter.NULL)

  /**
   * Since SPARK-30201 ObjectInspectorCopyOption.JAVA change to ObjectInspectorCopyOption.DEFAULT.
   * The reason is DEFAULT option can convert `UTF8String` to `Text` with bytes and we can
   * compatible with non UTF-8 code bytes during write.
   */
  private val standardOI = ObjectInspectorUtils
    .getStandardObjectInspector(
      tableDesc.getDeserializer(jobConf).getObjectInspector,
      ObjectInspectorCopyOption.DEFAULT)
    .asInstanceOf[StructObjectInspector]

  private val fieldOIs =
    standardOI.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
  private val dataTypes = dataSchema.map(_.dataType).toArray
  private val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }
  private val outputData = new Array[Any](fieldOIs.length)

  override def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fieldOIs.length) {
      outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row.get(i, dataTypes(i)))
      i += 1
    }
    hiveWriter.write(serializer.serialize(outputData, standardOI))
  }

  override def close(): Unit = {
    // Seems the boolean value passed into close does not matter.
    hiveWriter.close(false)
  }
}
