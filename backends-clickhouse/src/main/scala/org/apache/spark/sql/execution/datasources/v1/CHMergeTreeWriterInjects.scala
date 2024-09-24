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
package org.apache.spark.sql.execution.datasources.v1

import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.`type`.ColumnTypeNode
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.substrait.rel.RelBuilder
import org.apache.gluten.utils.ConfigUtil

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.{CHDatasourceJniWrapper, OutputWriter}
import org.apache.spark.sql.execution.datasources.clickhouse.ExtensionTableBuilder
import org.apache.spark.sql.execution.datasources.utils.MergeTreeDeltaUtil
import org.apache.spark.sql.execution.datasources.v1.clickhouse.MergeTreeOutputWriter
import org.apache.spark.sql.types.StructType

import com.google.common.collect.Lists
import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.NamedStruct
import org.apache.hadoop.mapreduce.TaskAttemptContext

import java.util.{ArrayList => JList, Map => JMap, UUID}

import scala.collection.JavaConverters._

case class PlanWithSplitInfo(plan: Array[Byte], splitInfo: Array[Byte])

class CHMergeTreeWriterInjects extends CHFormatWriterInjects {

  override def nativeConf(
      options: Map[String, String],
      compressionCodec: String): JMap[String, String] = {
    options.asJava
  }

  override def createOutputWriter(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext,
      nativeConf: JMap[String, String]): OutputWriter = null

  // scalastyle:off argcount
  override def createOutputWriter(
      path: String,
      database: String,
      tableName: String,
      snapshotId: String,
      orderByKeyOption: Option[Seq[String]],
      lowCardKeyOption: Option[Seq[String]],
      minmaxIndexKeyOption: Option[Seq[String]],
      bfIndexKeyOption: Option[Seq[String]],
      setIndexKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      partitionColumns: Seq[String],
      tableSchema: StructType,
      clickhouseTableConfigs: Map[String, String],
      context: TaskAttemptContext,
      nativeConf: JMap[String, String]): OutputWriter = {

    val uuid = UUID.randomUUID.toString

    val planWithSplitInfo = CHMergeTreeWriterInjects.genMergeTreeWriteRel(
      path,
      database,
      tableName,
      snapshotId,
      orderByKeyOption,
      lowCardKeyOption,
      minmaxIndexKeyOption,
      bfIndexKeyOption,
      setIndexKeyOption,
      primaryKeyOption,
      partitionColumns,
      Seq(),
      ConverterUtils.convertNamedStructJson(tableSchema),
      clickhouseTableConfigs,
      // use table schema instead of data schema
      SparkShimLoader.getSparkShims.attributesFromStruct(tableSchema)
    )
    val datasourceJniWrapper = new CHDatasourceJniWrapper()
    val instance =
      datasourceJniWrapper.nativeInitMergeTreeWriterWrapper(
        planWithSplitInfo.plan,
        planWithSplitInfo.splitInfo,
        uuid,
        context.getTaskAttemptID.getTaskID.getId.toString,
        context.getConfiguration.get("mapreduce.task.gluten.mergetree.partition.dir"),
        context.getConfiguration.get("mapreduce.task.gluten.mergetree.bucketid.str"),
        ConfigUtil.serialize(nativeConf)
      )

    new MergeTreeOutputWriter(database, tableName, datasourceJniWrapper, instance, path)
  }
  // scalastyle:on argcount

  override val formatName: String = "mergetree"

}

object CHMergeTreeWriterInjects {

  // scalastyle:off argcount
  def genMergeTreeWriteRel(
      path: String,
      database: String,
      tableName: String,
      snapshotId: String,
      orderByKeyOption: Option[Seq[String]],
      lowCardKeyOption: Option[Seq[String]],
      minmaxIndexKeyOption: Option[Seq[String]],
      bfIndexKeyOption: Option[Seq[String]],
      setIndexKeyOption: Option[Seq[String]],
      primaryKeyOption: Option[Seq[String]],
      partitionColumns: Seq[String],
      partList: Seq[String],
      tableSchemaJson: String,
      clickhouseTableConfigs: Map[String, String],
      output: Seq[Attribute]): PlanWithSplitInfo = {
    // scalastyle:on argcount
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val columnTypeNodes = output.map {
      attr =>
        if (partitionColumns.exists(_.equals(attr.name))) {
          new ColumnTypeNode(NamedStruct.ColumnType.PARTITION_COL)
        } else {
          new ColumnTypeNode(NamedStruct.ColumnType.NORMAL_COL)
        }
    }.asJava

    val (orderByKey, primaryKey) = MergeTreeDeltaUtil.genOrderByAndPrimaryKeyStr(
      orderByKeyOption,
      primaryKeyOption
    )

    val lowCardKey = MergeTreeDeltaUtil.columnsToStr(lowCardKeyOption)
    val minmaxIndexKey = MergeTreeDeltaUtil.columnsToStr(minmaxIndexKeyOption)
    val bfIndexKey = MergeTreeDeltaUtil.columnsToStr(bfIndexKeyOption)
    val setIndexKey = MergeTreeDeltaUtil.columnsToStr(setIndexKeyOption)

    val substraitContext = new SubstraitContext
    val extensionTableNode = ExtensionTableBuilder.makeExtensionTable(
      -1,
      -1,
      database,
      tableName,
      snapshotId,
      path,
      "",
      orderByKey,
      lowCardKey,
      minmaxIndexKey,
      bfIndexKey,
      setIndexKey,
      primaryKey,
      scala.collection.JavaConverters.seqAsJavaList(partList),
      scala.collection.JavaConverters.seqAsJavaList(
        Seq.range(0L, partList.length).map(long2Long)
      ), // starts and lengths is useless for write
      scala.collection.JavaConverters.seqAsJavaList(Seq.range(0L, partList.length).map(long2Long)),
      tableSchemaJson,
      clickhouseTableConfigs.asJava,
      new JList[String]()
    )

    val optimizationContent = "isMergeTree=1\n"
    val optimization = Any.pack(StringValue.newBuilder.setValue(optimizationContent).build)
    val extensionNode = ExtensionBuilder.makeAdvancedExtension(optimization, null)

    val relNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      columnTypeNodes,
      null,
      extensionNode,
      substraitContext,
      substraitContext.nextOperatorId("readRel"))

    val plan =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode), nameList).toProtobuf

    PlanWithSplitInfo(plan.toByteArray, extensionTableNode.toProtobuf.toByteArray)
  }
}
