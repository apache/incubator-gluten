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
package io.glutenproject.backendsapi.clickhouse

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.{BackendsApiManager, TransformerApi}
import io.glutenproject.execution.{CHHashAggregateExecTransformer, TransformSupport, WholeStageTransformerExec}
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter}
import io.glutenproject.substrait.`type`.TypeNode
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.SelectionNode
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.utils.CHInputPartitionsUtil

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.utils.RangePartitionerBoundsGenerator
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Final, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.ColumnarAQEShuffleReadExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.datasources.v1.ClickHouseFileIndex
import org.apache.spark.sql.types.StructField

import java.util

class CHTransformerApi extends TransformerApi with Logging {

  /**
   * Do validate for ColumnarShuffleExchangeExec. For ClickHouse backend, it will return true
   * directly.
   *
   * @return
   */
  override def validateColumnarShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan): Boolean = {
    val outputAttributes = child.output
    // check repartition expression
    val substraitContext = new SubstraitContext
    outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        !(exprs
          .map(
            expr => {
              val node = ExpressionConverter
                .replaceWithExpressionTransformer(expr, outputAttributes)
                .doTransform(substraitContext.registeredFunction)
              if (!node.isInstanceOf[SelectionNode]) {
                // This is should not happen.
                logDebug("Expressions are not supported in HashPartitioning.")
                false
              } else {
                true
              }
            })
          .exists(_ == false)) ||
        BackendsApiManager.getSettings.supportShuffleWithProject(outputPartitioning, child)
      case rangePartitoning: RangePartitioning =>
        GlutenConfig.getConf.enableColumnarSort &&
        RangePartitionerBoundsGenerator.supportedOrderings(rangePartitoning, child)
      case _ => true
    }
  }

  /**
   * Used for table scan validation.
   *
   * @return
   *   true if backend supports reading the file format.
   */
  def supportsReadFileFormat(
      fileFormat: ReadFileFormat,
      fields: Array[StructField],
      partTable: Boolean,
      paths: Seq[String]): Boolean =
    BackendsApiManager.getSettings.supportFileFormatRead(fileFormat, fields, partTable, paths)

  /** Generate Seq[InputPartition] for FileSourceScanExecTransformer. */
  def genInputPartitionSeq(
      relation: HadoopFsRelation,
      selectedPartitions: Array[PartitionDirectory]): Seq[InputPartition] = {
    if (relation.location.isInstanceOf[ClickHouseFileIndex]) {
      // Generate NativeMergeTreePartition for MergeTree
      relation.location.asInstanceOf[ClickHouseFileIndex].partsPartitions
    } else {
      // Generate FilePartition for Parquet
      CHInputPartitionsUtil.genInputPartitionSeq(relation, selectedPartitions)
    }
  }

  override def postProcessNativeConfig(
      nativeConfMap: util.Map[String, String],
      backendPrefix: String): Unit = {
    /// TODO: IMPLEMENT POST PROCESS FOR CLICKHOUSE BACKEND
  }

  override def getCoalesceInputAttributes(
      plan: SparkPlan): (util.ArrayList[TypeNode], util.ArrayList[String]) = {
    logDebug(s"xxx getCoalesceInputAttributes: $plan")
    plan match {
      case shuffleExec: ColumnarShuffleExchangeExec =>
        /*
        val (typeList, nameList) = getCoalesceInputAttributes(shuffleExec.child)
        if (shuffleExec.projectOutputAttributes != null) {
          logDebug(
            s"xxx child.out=${shuffleExec.child.output}\n" +
              s"projectOutputAttributes=${shuffleExec.projectOutputAttributes}")
          val subTypeList = new util.ArrayList[TypeNode]()
          subTypeList.addAll(typeList.subList(0, shuffleExec.projectOutputAttributes.size))
          val subNameList = new util.ArrayList[String]()
          subNameList.addAll(nameList.subList(0, shuffleExec.projectOutputAttributes.size))
          throw new IllegalStateException(s"xxx $subNameList; $nameList")
          // (subTypeList, subNameList)
        } else {
          (typeList, nameList)
        }
         */
        logDebug(
          s"xxx projectOutputAttributes=${shuffleExec.projectOutputAttributes}\n" +
            s"child is : ${shuffleExec.child.getClass}")
        if (shuffleExec.projectOutputAttributes != null) {
          val typeList = new util.ArrayList[TypeNode]
          val nameList = new util.ArrayList[String]
          plan.output.foreach(
            attr => {
              typeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
              nameList.add(ConverterUtils.genColumnNameWithExprId(attr))
            })
          (typeList, nameList)
        } else {
          getCoalesceInputAttributes(shuffleExec.child)
        }
      case aqeShuffleExec: ColumnarAQEShuffleReadExec =>
        getCoalesceInputAttributes(aqeShuffleExec.child)
      case wholeStage: WholeStageTransformerExec =>
        getCoalesceInputAttributes(wholeStage.child)
      case hashAgg: CHHashAggregateExecTransformer =>
        // Hash aggregating intermediate result is special
        val typeList = new util.ArrayList[TypeNode]
        val nameList = new util.ArrayList[String]
        // add group by columns
        hashAgg.groupingExpressions.foreach(
          expr => {
            val attr = ConverterUtils.getAttrFromExpr(expr).toAttribute
            val colName = ConverterUtils.genColumnNameWithExprId(attr)
            nameList.add(colName)
            typeList.add(hashAgg.getTypeNode(colName, attr, hashAgg.aggregateExpressions))
          })
        hashAgg.aggregateExpressions.foreach(
          expr => {
            val attr = expr.resultAttribute
            // If it's a partial aggregate function, make the column name in special
            // format
            val colName = expr.mode match {
              case Partial => hashAgg.getPartialAggregateColumnName(attr)
              case _ => ConverterUtils.genColumnNameWithExprId(attr)
            }
            nameList.add(colName)
            typeList.add(hashAgg.getTypeNode(colName, attr, hashAgg.aggregateExpressions))
          })
        (typeList, nameList)
      case _ =>
        val typeList = new util.ArrayList[TypeNode]
        val nameList = new util.ArrayList[String]
        plan.output.foreach(
          attr => {
            typeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
            nameList.add(ConverterUtils.genColumnNameWithExprId(attr))
          })
        (typeList, nameList)
    }
  }
}
