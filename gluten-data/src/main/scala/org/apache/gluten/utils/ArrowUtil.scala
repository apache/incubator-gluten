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

import org.apache.gluten.exception.SchemaMismatchException
import org.apache.gluten.vectorized.ArrowWritableColumnVector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.vectorized.ArrowColumnVectorUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.{SparkArrowUtil, SparkSchemaUtil}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.arrow.c.{ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.dataset.file.{FileFormat, FileSystemDatasetFactory}
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.hadoop.fs.FileStatus

import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

object ArrowUtil extends Logging {

  private val defaultTimeZoneId = SparkSchemaUtil.getLocalTimezoneID

  private def getResultType(dataType: DataType): ArrowType = {
    getResultType(dataType, defaultTimeZoneId)
  }

  private def getResultType(dataType: DataType, timeZoneId: String): ArrowType = {
    dataType match {
      case other =>
        SparkArrowUtil.toArrowType(dataType, timeZoneId)
    }
  }

  def toArrowSchema(attributes: Seq[Attribute]): Schema = {
    val fields = attributes.map(
      attr => {
        Field
          .nullable(s"${attr.name}#${attr.exprId.id}", getResultType(attr.dataType))
      })
    new Schema(fields.toList.asJava)
  }

  def toArrowSchema(
      cSchema: ArrowSchema,
      allocator: BufferAllocator,
      provider: CDataDictionaryProvider): Schema = {
    val schema = Data.importSchema(allocator, cSchema, provider)
    val originFields = schema.getFields
    val fields = new util.ArrayList[Field](originFields.size)
    originFields.forEach {
      field =>
        val dt = SparkArrowUtil.fromArrowField(field)
        fields.add(
          SparkArrowUtil.toArrowField(field.getName, dt, true, SparkSchemaUtil.getLocalTimezoneID))
    }
    new Schema(fields)
  }

  def toSchema(batch: ColumnarBatch): Schema = {
    val fields = new java.util.ArrayList[Field](batch.numCols)
    for (i <- 0 until batch.numCols) {
      val col: ColumnVector = batch.column(i)
      fields.add(col match {
        case vector: ArrowWritableColumnVector =>
          vector.getValueVector.getField
        case _ =>
          throw new UnsupportedOperationException(
            s"Unexpected vector type: ${col.getClass.toString}")
      })
    }
    new Schema(fields)
  }

  def getFormat(format: String): FileFormat = {
    format match {
      case "parquet" => FileFormat.PARQUET
      case "orc" => FileFormat.ORC
      case "csv" => FileFormat.CSV
      case _ => throw new IllegalArgumentException("Unrecognizable format")
    }
  }

  def getFormat(format: org.apache.spark.sql.execution.datasources.FileFormat): FileFormat = {
    format match {
      case _: ParquetFileFormat =>
        FileFormat.PARQUET
      case _: CSVFileFormat =>
        FileFormat.CSV
      case _ =>
        throw new IllegalArgumentException("Unrecognizable format")
    }
  }

  private def rewriteUri(encodeUri: String): String = {
    val decodedUri = encodeUri
    val uri = URI.create(decodedUri)
    if (uri.getScheme == "s3" || uri.getScheme == "s3a") {
      val s3Rewritten =
        new URI("s3", uri.getAuthority, uri.getPath, uri.getQuery, uri.getFragment).toString
      return s3Rewritten
    }
    val sch = uri.getScheme match {
      case "hdfs" => "hdfs"
      case "file" => "file"
    }
    val ssp = uri.getScheme match {
      case "hdfs" => uri.getSchemeSpecificPart
      case "file" => "//" + uri.getSchemeSpecificPart
    }
    val rewritten = new URI(sch, ssp, uri.getFragment)
    rewritten.toString
  }

  def makeArrowDiscovery(
      encodedUri: String,
      format: FileFormat,
      allocator: BufferAllocator,
      pool: NativeMemoryPool): FileSystemDatasetFactory = {
    val factory = new FileSystemDatasetFactory(allocator, pool, format, rewriteUri(encodedUri))
    factory
  }

  def readSchema(
      file: FileStatus,
      format: FileFormat,
      allocator: BufferAllocator,
      pool: NativeMemoryPool): Option[StructType] = {
    val factory: FileSystemDatasetFactory =
      makeArrowDiscovery(file.getPath.toString, format, allocator, pool)
    val schema = factory.inspect()
    try {
      Option(SparkSchemaUtil.fromArrowSchema(schema))
    } finally {
      factory.close()
    }
  }

  def readSchema(
      files: Seq[FileStatus],
      format: FileFormat,
      allocator: BufferAllocator,
      pool: NativeMemoryPool): Option[StructType] = {
    if (files.isEmpty) {
      throw new IllegalArgumentException("No input file specified")
    }

    readSchema(files.head, format, allocator, pool)
  }

  def compareStringFunc(caseSensitive: Boolean): (String, String) => Boolean = {
    if (caseSensitive) { (str1: String, str2: String) => str1.equals(str2) }
    else { (str1: String, str2: String) => str1.equalsIgnoreCase(str2) }
  }

  // If user specify schema by .schema(newSchemaDifferentWithFile)
  def checkSchema(
      requiredField: DataType,
      parquetFileFieldType: ArrowType,
      parquetFileFields: mutable.Buffer[Field]): Unit = {
    val requiredFieldType =
      SparkArrowUtil.toArrowType(requiredField, SparkSchemaUtil.getLocalTimezoneID)
    if (!requiredFieldType.equals(parquetFileFieldType)) {
      val arrowFileSchema = parquetFileFields
        .map(f => f.toString)
        .reduceLeft((f1, f2) => f1 + "\n" + f2)
      throw new SchemaMismatchException(
        s"Not support specified schema is different with file schema\n$arrowFileSchema")
    }
  }

  def getRequestedField(
      requiredSchema: StructType,
      parquetFileFields: mutable.Buffer[Field],
      caseSensitive: Boolean): Schema = {
    val compareFunc = compareStringFunc(caseSensitive)
    requiredSchema.foreach {
      readField =>
        // TODO: check schema inside of complex type
        val matchedFields =
          parquetFileFields.filter(field => compareFunc(field.getName, readField.name))
        if (!caseSensitive && matchedFields.size > 1) {
          // Need to fail if there is ambiguity, i.e. more than one field is matched
          val fieldsString = matchedFields.map(_.getName).mkString("[", ", ", "]")
          throw new RuntimeException(
            s"""
               |Found duplicate field(s) "${readField.name}": $fieldsString

               |in case-insensitive mode""".stripMargin.replaceAll("\n", " "))
        }
        if (matchedFields.nonEmpty) {
          checkSchema(
            readField.dataType,
            matchedFields.head.getFieldType.getType,
            parquetFileFields)
        }
    }

    val requestColNames = requiredSchema.map(_.name)
    new Schema(parquetFileFields.filter {
      field => requestColNames.exists(col => compareFunc(col, field.getName))
    }.asJava)
  }

  def loadMissingColumns(
      rowCount: Int,
      missingSchema: StructType): Array[ArrowWritableColumnVector] = {

    val vectors =
      ArrowWritableColumnVector.allocateColumns(rowCount, missingSchema)
    vectors.foreach {
      vector =>
        vector.putNulls(0, rowCount)
        vector.setValueCount(rowCount)
    }

    vectors
  }

  def loadPartitionColumns(
      rowCount: Int,
      partitionSchema: StructType,
      partitionValues: InternalRow): Array[ArrowWritableColumnVector] = {
    val partitionColumns = ArrowWritableColumnVector.allocateColumns(rowCount, partitionSchema)
    (0 until partitionColumns.length).foreach(
      i => {
        ArrowColumnVectorUtils.populate(partitionColumns(i), partitionValues, i)
        partitionColumns(i).setValueCount(rowCount)
        partitionColumns(i).setIsConstant()
      })

    partitionColumns
  }

  def loadBatch(
      allocator: BufferAllocator,
      input: ArrowRecordBatch,
      dataSchema: StructType,
      requiredSchema: StructType,
      partitionVectors: Array[ArrowWritableColumnVector] = Array.empty,
      nullVectors: Array[ArrowWritableColumnVector] = Array.empty): ColumnarBatch = {
    val rowCount: Int = input.getLength

    val vectors =
      try {
        ArrowWritableColumnVector.loadColumns(
          rowCount,
          SparkSchemaUtil.toArrowSchema(dataSchema),
          input,
          allocator)
      } finally {
        input.close()
      }

    val totalVectors = if (nullVectors.nonEmpty) {
      val finalVectors =
        mutable.ArrayBuffer[ArrowWritableColumnVector]()
      val requiredIterator = requiredSchema.iterator
      val compareFunc = compareStringFunc(SQLConf.get.caseSensitiveAnalysis)
      while (requiredIterator.hasNext) {
        val field = requiredIterator.next()
        finalVectors.append(vectors
          .find(vector => compareFunc(vector.getValueVector.getName, field.name))
          .getOrElse {
            // The missing column need to be find in nullVectors
            val nullVector =
              nullVectors.find(vector => compareFunc(vector.getValueVector.getName, field.name)).get
            nullVector.setValueCount(rowCount)
            nullVector.retain()
            nullVector
          })
      }
      finalVectors.toArray
    } else {
      vectors
    }

    val batch = new ColumnarBatch(
      totalVectors.map(_.asInstanceOf[ColumnVector]) ++
        partitionVectors
          .map {
            vector =>
              vector.setValueCount(rowCount)
              vector.asInstanceOf[ColumnVector]
          },
      rowCount
    )
    batch
  }

}
