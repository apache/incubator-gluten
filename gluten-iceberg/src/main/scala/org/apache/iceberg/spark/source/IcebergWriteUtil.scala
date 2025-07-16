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
package org.apache.iceberg.spark.source

import org.apache.spark.sql.connector.write.{BatchWrite, Write, WriterCommitMessage}

import org.apache.iceberg._
import org.apache.iceberg.spark.source.SparkWrite.TaskCommit
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.{ListType, MapType}

object IcebergWriteUtil {
  def isBatchAppend(write: BatchWrite): Boolean = {
    write.getClass.getSimpleName.equals("BatchAppend")
  }

  def isDataWrite(write: Write): Boolean = {
    write.isInstanceOf[SparkWrite]
  }

  def hasUnsupportedDataType(write: Write): Boolean = {
    getWriteSchema(write).columns().stream().anyMatch(d => containsUuidOrFixedType(d.`type`()))
  }

  private def containsUuidOrFixedType(dataType: Type): Boolean = {
    dataType match {
      case l: ListType => containsUuidOrFixedType(l.elementType)
      case m: MapType => containsUuidOrFixedType(m.keyType) || containsUuidOrFixedType(m.valueType)
      case s: org.apache.iceberg.types.Types.StructType =>
        s.fields().stream().anyMatch(f => containsUuidOrFixedType(f.`type`()))
      case t if t.typeId() == TypeID.UUID || t.typeId() == TypeID.FIXED => true
      case _ => false
    }
  }

  private def getWriteSchema(write: Write): Schema = {
    assert(write.isInstanceOf[SparkWrite])
    val field = classOf[SparkWrite].getDeclaredField("writeSchema")
    field.setAccessible(true)
    field.get(write).asInstanceOf[Schema]
  }

  def getWriteProperty(write: Write): java.util.Map[String, String] = {
    val field = classOf[SparkWrite].getDeclaredField("writeProperties")
    field.setAccessible(true)
    field.get(write).asInstanceOf[java.util.Map[String, String]]
  }

  def getTable(write: Write): Table = {
    val field = classOf[SparkWrite].getDeclaredField("table")
    field.setAccessible(true)
    field.get(write).asInstanceOf[Table]
  }

  def getSparkWrite(write: BatchWrite): SparkWrite = {
    // Access the enclosing SparkWrite instance from BatchAppend
    val outerInstanceField = write.getClass.getDeclaredField("this$0")
    outerInstanceField.setAccessible(true)
    outerInstanceField.get(write).asInstanceOf[SparkWrite]
  }

  def getFileFormat(write: Write): FileFormat = {
    val field = classOf[SparkWrite].getDeclaredField("format")
    field.setAccessible(true)
    field.get(write).asInstanceOf[FileFormat]
  }

  def getFileFormat(write: BatchWrite): FileFormat = {
    val sparkWrite = getSparkWrite(write)
    val field = classOf[SparkWrite].getDeclaredField("format")
    field.setAccessible(true)
    field.get(sparkWrite).asInstanceOf[FileFormat]
  }

  def getDirectory(write: Write): String = {
    val field = classOf[SparkWrite].getDeclaredField("table")
    field.setAccessible(true)
    getTable(write).locationProvider().newDataLocation("")
  }

  def getPartitionSpec(write: Write): PartitionSpec = {
    val field = classOf[SparkWrite].getDeclaredField("table")
    field.setAccessible(true)
    getTable(write).spec()
  }

  def getDirectory(write: BatchWrite): String = {
    val sparkWrite = getSparkWrite(write)
    val field = classOf[SparkWrite].getDeclaredField("table")
    field.setAccessible(true)
    getTable(sparkWrite).locationProvider().newDataLocation("")
  }

  // Similar to the UnpartitionedDataWriter#commit
  def commitDataFiles(dataFiles: Array[DataFile]): WriterCommitMessage = {
    val commit = new TaskCommit(dataFiles)
    commit.reportOutputMetrics()
    commit
  }

}
