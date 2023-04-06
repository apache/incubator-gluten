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

package io.glutenproject.execution

import com.google.common.collect.Lists
import com.google.protobuf.{Any, StringValue}
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{ColumnTypeNode, TypeBuilder, TypeNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.utils.BindReferencesUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{AliasAwareOutputPartitioning, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

import java.util

case class DataWritingCommandExecTransformer(
           cmd: DataWritingCommand, child: SparkPlan) extends UnaryExecNode
  with TransformSupport
  with GlutenPlan
  with PredicateHelper
  with AliasAwareOutputPartitioning
  with Logging {

  val sparkConf: SparkConf = sparkContext.getConf

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note
   * Right now we support up to two RDDs
   */
  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }


  def getRelNode(context: SubstraitContext,
                 cmd: DataWritingCommand,
                 originalInputAttributes: Seq[Attribute],
                 operatorId: Long,
                 input: RelNode,
                 validation: Boolean): RelNode = {
    val typeNodes = ConverterUtils.getTypeNodeFromAttributes(originalInputAttributes)
    val nameList = new java.util.ArrayList[String]()
    val columnTypeNodes = new java.util.ArrayList[ColumnTypeNode]()
    nameList.addAll(OperatorsUtils.collectAttributesNamesDFS(originalInputAttributes))



    var writePath = ""
    var isParquet = false
    var isDWRF = false
    cmd match {
      case InsertIntoHadoopFsRelationCommand(
      outputPath, _, _, partitionColumns, _, fileFormat, _, _, _, _, _, _) =>
        fileFormat.getClass.getSimpleName match {
          case "ParquetFileFormat" =>
            isParquet = true
          case "DwrfFileFormat" =>
            isDWRF = true
          case _ =>
        }
        writePath = outputPath.toString

        for (attr <- originalInputAttributes) {
          if (partitionColumns.find(_.equals(attr)).isDefined) {
            columnTypeNodes.add(new ColumnTypeNode(1))
          } else {
            columnTypeNodes.add(new ColumnTypeNode(0))
          }
        }
      case _ =>
    }

    if (!validation) {
      RelBuilder.makeWriteRel(
        input, typeNodes, nameList, columnTypeNodes, writePath,
        OperatorsUtils.createExtensionNode(
          genFormatParametersBuilder(isParquet, isDWRF), originalInputAttributes),
        context, operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = new java.util.ArrayList[TypeNode]()
      for (attr <- originalInputAttributes) {
        inputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeWriteRel(
        input, typeNodes, nameList, columnTypeNodes, writePath,
        extensionNode, context, operatorId)
    }
  }

  def genFormatParametersBuilder(isPARQUET: Boolean,
                                 isDWRF: Boolean = false): Any.Builder = {
    // Start with "FormatParameters:"
    val formatParametersStr = new StringBuffer("WriteFormatParameters:")
    // isParquet: 0 for DWRF, 1 for Parquet
    formatParametersStr.append("isPARQUET=").append(if (isPARQUET) 1 else 0).append("\n")
      .append("isDWRF=").append(if (isDWRF) 1 else 0).append("\n")

    val message = StringValue
      .newBuilder()
      .setValue(formatParametersStr.toString)
      .build()
    Any.newBuilder
      .setValue(message.toByteString)
      .setTypeUrl("/google.protobuf.StringValue")
  }

  override def doValidateInternal(): Boolean = {
    // format only support parquet and dwrf

    if (!BackendsApiManager.getSettings.supportWriteExec() ||
      !BackendsApiManager.getSettings.supportedFileFormatWrite(cmd)) {
      return false
    }
     val substraitContext = new SubstraitContext
     val operatorId = substraitContext.nextOperatorId
     val relNode = try {
       getRelNode(
         substraitContext, cmd, child.output, operatorId, null, validation = true)
     } catch {
       case e: Throwable =>
         logValidateFailure(
           s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}", e)
         return false
     }
     // Then, validate the generated plan in native engine.
     if (relNode != null && GlutenConfig.getConf.enableNativeValidation) {
       val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
       BackendsApiManager.getValidatorApiInstance.doValidate(planNode)
     } else {
       true
     }
   }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport =>
        c.doTransform(context)
      case _ =>
        null
    }
    val operatorId = context.nextOperatorId

    val (currRel, inputAttributes) = if (childCtx != null) {
      (getRelNode(
        context, cmd, child.output, operatorId, childCtx.root, validation = false),
        childCtx.outputAttributes)
    } else {
      // This means the input is just an iterator, so an ReadRel will be created as child.
      // Prepare the input schema.
      val attrList = new util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)
      (getRelNode(
        context, cmd, child.output, operatorId, readRel, validation = false),
        child.output)
    }
    assert(currRel != null, "Write Rel should be valid")

    val outputAttrs = BindReferencesUtil.bindReferencesWithNullable(output, inputAttributes)
    TransformContext(inputAttributes, outputAttrs, currRel)
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = child match {
    case c: TransformSupport =>
      c.getBuildPlans
    case _ =>
      Seq()
  }

  override def getStreamedLeafPlan: SparkPlan = child match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def metricsUpdater(): MetricsUpdater = {
    null
  }


  override protected def outputExpressions: Seq[NamedExpression] = output

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def output: Seq[Attribute] = cmd.output


  override protected def withNewChildInternal(
        newChild: SparkPlan): DataWritingCommandExecTransformer = {
    copy(child = newChild)
  }
}
