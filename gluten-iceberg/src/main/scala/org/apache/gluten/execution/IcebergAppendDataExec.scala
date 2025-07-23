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

import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.iceberg.{FileFormat, PartitionField, PartitionSpec, Schema}
import org.apache.iceberg.TableProperties.{ORC_COMPRESSION, ORC_COMPRESSION_DEFAULT, PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT}
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.spark.source.IcebergWriteUtil
import org.apache.iceberg.types.Type.TypeID

import scala.collection.JavaConverters._

trait IcebergAppendDataExec extends ColumnarAppendDataExec {

  protected def getFileFormat(format: FileFormat): Int = {
    format match {
      case FileFormat.PARQUET => 1;
      case FileFormat.ORC => 0;
      case _ => throw new UnsupportedOperationException()
    }
  }

  protected def getCodec: String = {
    val config = IcebergWriteUtil.getWriteProperty(write)
    val codec = IcebergWriteUtil.getFileFormat(write) match {
      case FileFormat.PARQUET =>
        config.getOrDefault(PARQUET_COMPRESSION, PARQUET_COMPRESSION_DEFAULT)
      case FileFormat.ORC => config.getOrDefault(ORC_COMPRESSION, ORC_COMPRESSION_DEFAULT)
      case _ => throw new UnsupportedOperationException()
    }
    if (codec == "UNCOMPRESSED") {
      "none"
    } else codec
  }

  protected def getPartitionSpec: PartitionSpec = {
    IcebergWriteUtil.getPartitionSpec(write)
  }

  private def validatePartitionType(schema: Schema, field: PartitionField): Boolean = {
    val partitionType = schema.findType(field.sourceId())
    val unSupportType = Seq(TypeID.DOUBLE, TypeID.FLOAT, TypeID.BINARY, TypeID.DECIMAL)
    !unSupportType.contains(partitionType.typeId())
  }

  override def doValidateInternal(): ValidationResult = {
    if (!IcebergWriteUtil.isDataWrite(write)) {
      return ValidationResult.failed(s"Not support the write ${write.getClass.getSimpleName}")
    }
    if (IcebergWriteUtil.hasUnsupportedDataType(write)) {
      return ValidationResult.failed("Contains UUID ot FIXED data type")
    }
    if (BackendsApiManager.getValidatorApiInstance.doSchemaValidate(query.schema).isDefined) {
      return ValidationResult.failed("Contains unsupported data type")
    }
    val spec = IcebergWriteUtil.getTable(write).spec()
    if (spec.isPartitioned) {
      return ValidationResult.failed("Not support write partition table")
    }
    if (spec.isPartitioned) {
      val topIds = spec.schema().columns().asScala.map(c => c.fieldId())
      if (
        spec
          .fields()
          .stream()
          .anyMatch(
            f =>
              !f.transform().isIdentity
                || !validatePartitionType(spec.schema(), f) || !topIds.contains(f.sourceId()))
      ) {
        return ValidationResult.failed(
          "Not support write non identity partition table," +
            "or contains unsupported partition type, or is nested partition column")
      }
    }
    if (IcebergWriteUtil.getTable(write).sortOrder().isSorted) {
      return ValidationResult.failed("Not support write table with sort order")
    }
    val format = IcebergWriteUtil.getFileFormat(write)
    if (format != FileFormat.PARQUET) {
      return ValidationResult.failed("Not support this format " + format.name())
    }

    val codec = getCodec
    if (Seq("brotli, lzo").contains(codec)) {
      return ValidationResult.failed("Not support this codec " + codec)
    }
    if (query.output.exists(a => !AvroSchemaUtil.makeCompatibleName(a.name).equals(a.name))) {
      return ValidationResult.failed("Not support the compatible column name")
    }

    ValidationResult.succeeded
  }

}
