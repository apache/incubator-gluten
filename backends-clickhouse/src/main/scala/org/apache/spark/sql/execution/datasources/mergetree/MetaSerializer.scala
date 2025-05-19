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
package org.apache.spark.sql.execution.datasources.mergetree

import org.apache.gluten.execution.MergeTreePartSplit
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.utils.SubstraitPlanPrinterUtil

import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.types.StructType

import com.fasterxml.jackson.databind.ObjectMapper
import io.substrait.proto.{NamedStruct, Type}

import java.util.{Map => jMap}

case class PartSerializer(
    partList: Seq[String],
    starts: Seq[Long],
    lengths: Seq[Long],
    rowIndexFilterTypes: Seq[String],
    rowIndexFilterIdsEncodeded: Seq[String]
) {
  def apply(): StringBuilder = {
    val partPathList = new StringBuilder
    for (i <- partList.indices) {
      val end = starts(i) + lengths(i)
      partPathList
        .append(partList(i))
        .append("\n")
        .append(starts(i))
        .append("\n")
        .append(end)
        .append("\n")
        .append(rowIndexFilterTypes(i))
        .append("\n")
        .append(rowIndexFilterIdsEncodeded(i))
        .append("\n")
    }
    partPathList
  }

  // TODO: remove pathList
  def pathList(absolutePath: String): Seq[String] = {
    partList.map(name => absolutePath + "/" + name)
  }
}

object PartSerializer {
  def fromMergeTreePartSplits(partLists: Seq[MergeTreePartSplit]): PartSerializer = {
    val partList = partLists.map(_.name)
    val starts = partLists.map(_.start)
    val lengths = partLists.map(_.length)
    val rowIndexFilterTypes = partLists.map(_.rowIndexFilterType)
    val rowIndexFilterIdEncodeds = partLists.map(_.rowIndexFilterIdEncoded)
    PartSerializer(partList, starts, lengths, rowIndexFilterTypes, rowIndexFilterIdEncodeds)
  }

  def fromAddMergeTreeParts(parts: Seq[AddMergeTreeParts]): PartSerializer = {
    val partList = parts.map(_.name)
    val starts = parts.map(_ => 0L)
    val lengths = parts.map(_.marks)
    val emptyStrings = Seq.fill[String](parts.length)("")
    PartSerializer(partList, starts, lengths, emptyStrings, emptyStrings)
  }

  def fromPartNames(partNames: Seq[String]): PartSerializer = {
    // starts and lengths is useless for writing
    val partRanges = Seq.range(0L, partNames.length)
    val emptyStrings = Seq.fill[String](partNames.length)("")
    PartSerializer(partNames, partRanges, partRanges, emptyStrings, emptyStrings)
  }
}

object MetaSerializer {
  // scalastyle:off argcount
  def apply(
      database: String,
      tableName: String,
      snapshotId: String,
      relativePath: String,
      absolutePath: String,
      orderByKey: String,
      lowCardKey: String,
      minmaxIndexKey: String,
      bfIndexKey: String,
      setIndexKey: String,
      primaryKey: String,
      partSerializer: PartSerializer,
      tableSchema: StructType,
      clickhouseTableConfigs: jMap[String, String]): String = {
    // scalastyle:on argcount

    // New: MergeTree;{database}\n{table}\n{orderByKey}\n{primaryKey}\n{relative_path}\n
    // {part_path1}\n{part_path2}\n...
    val extensionTableStr = new StringBuilder(StorageMeta.SERIALIZER_HEADER)

    val tableSchemaJson = convertNamedStructJson(tableSchema)

    extensionTableStr
      .append(database)
      .append("\n")
      .append(tableName)
      .append("\n")
      .append(snapshotId)
      .append("\n")
      .append(tableSchemaJson)
      .append("\n")
      .append(orderByKey)
      .append("\n")

    assert(orderByKey.nonEmpty)
    if (orderByKey == StorageMeta.DEFAULT_ORDER_BY_KEY) {
      extensionTableStr.append("").append("\n")
    } else {
      extensionTableStr.append(primaryKey).append("\n")
    }

    extensionTableStr.append(lowCardKey).append("\n")
    extensionTableStr.append(minmaxIndexKey).append("\n")
    extensionTableStr.append(bfIndexKey).append("\n")
    extensionTableStr.append(setIndexKey).append("\n")
    extensionTableStr.append(StorageMeta.normalizeRelativePath(relativePath)).append("\n")
    extensionTableStr.append(absolutePath).append("\n")
    appendConfigs(extensionTableStr, clickhouseTableConfigs)
    extensionTableStr.append(partSerializer())

    extensionTableStr.toString()
  }

  private def appendConfigs(
      extensionTableStr: StringBuilder,
      clickhouseTableConfigs: jMap[String, String]): Unit = {
    if (clickhouseTableConfigs != null && !clickhouseTableConfigs.isEmpty) {
      val objectMapper: ObjectMapper = new ObjectMapper
      try {
        val clickhouseTableConfigsJson: String = objectMapper
          .writeValueAsString(clickhouseTableConfigs)
          .replaceAll("\n", "")
          .replaceAll(" ", "")
        extensionTableStr.append(clickhouseTableConfigsJson).append("\n")
      } catch {
        case e: Exception =>
          extensionTableStr.append("\n")
      }
    } else extensionTableStr.append("\n")
  }

  /** Convert StructType to Json */
  private def convertNamedStructJson(tableSchema: StructType): String = {
    val structBuilder = Type.Struct.newBuilder
    tableSchema.fields
      .map(f => ConverterUtils.getTypeNode(f.dataType, f.nullable))
      .foreach(t => structBuilder.addTypes(t.toProtobuf))

    val nStructBuilder = NamedStruct.newBuilder
    nStructBuilder.setStruct(structBuilder.build)
    tableSchema.fieldNames.foreach(nStructBuilder.addNames)

    SubstraitPlanPrinterUtil
      .substraitNamedStructToJson(nStructBuilder.build())
      .replaceAll("\n", "")
      .replaceAll(" ", "")
  }
}
