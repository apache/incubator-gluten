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

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter}
import io.glutenproject.extension.ValidationResult
import io.glutenproject.substrait.`type`.ColumnTypeNode
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{ReadRelNode, RelBuilder, SplitInfo}
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.collect.Lists

import scala.collection.JavaConverters._

trait BasicScanExecTransformer extends LeafTransformSupport with BaseDataSource {

  /** Returns the filters that can be pushed down to native file scan */
  def filterExprs(): Seq[Expression]

  def outputAttributes(): Seq[Attribute]

  /** This can be used to report FileFormat for a file based scan operator. */
  val fileFormat: ReadFileFormat

  // TODO: Remove this expensive call when CH support scan custom partition location.
  def getInputFilePaths: Seq[String] = {
    // This is a heavy operation, and only the required backend executes the corresponding logic.
    if (BackendsApiManager.getSettings.requiredInputFilePaths()) {
      getInputFilePathsInternal
    } else {
      Seq.empty
    }
  }

  /** Returns the split infos that will be processed by the underlying native engine. */
  def getSplitInfos: Seq[SplitInfo] = {
    getPartitions.map(
      BackendsApiManager.getIteratorApiInstance
        .genSplitInfo(_, getPartitionSchema, fileFormat))
  }

  def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("outputRows")
    val numOutputVectors = longMetric("outputVectors")
    val scanTime = longMetric("scanTime")
    val substraitContext = new SubstraitContext
    val transformContext = doTransform(substraitContext)
    val outNames = outputAttributes().map(ConverterUtils.genColumnNameWithExprId).asJava
    val planNode =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(transformContext.root), outNames)

    BackendsApiManager.getIteratorApiInstance.genNativeFileScanRDD(
      sparkContext,
      WholeStageTransformContext(planNode, substraitContext),
      getSplitInfos,
      numOutputRows,
      numOutputVectors,
      scanTime
    )
  }

  override protected def doValidateInternal(): ValidationResult = {
    val validationResult = BackendsApiManager.getSettings
      .supportFileFormatRead(
        fileFormat,
        schema.fields,
        getPartitionSchema.nonEmpty,
        getInputFilePaths)
    if (!validationResult.isValid) {
      return validationResult
    }

    val substraitContext = new SubstraitContext
    val relNode = doTransform(substraitContext).root

    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val output = outputAttributes()
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithoutExprId(output)
    val columnTypeNodes = output.map {
      attr =>
        if (getPartitionSchema.exists(_.name.equals(attr.name))) {
          new ColumnTypeNode(1)
        } else {
          new ColumnTypeNode(0)
        }
    }.asJava
    // Will put all filter expressions into an AND expression
    val transformer = filterExprs()
      .map {
        case ar: AttributeReference if ar.dataType == BooleanType =>
          EqualNullSafe(ar, Literal.TrueLiteral)
        case e => e
      }
      .reduceLeftOption(And)
      .map(ExpressionConverter.replaceWithExpressionTransformer(_, output))
    val filterNodes = transformer.map(_.doTransform(context.registeredFunction))
    val exprNode = filterNodes.orNull

    val readNode = RelBuilder.makeReadRel(
      typeNodes,
      nameList,
      columnTypeNodes,
      exprNode,
      context,
      context.nextOperatorId(this.nodeName))
    readNode.asInstanceOf[ReadRelNode].setDataSchema(getDataSchema)
    TransformContext(output, output, readNode)
  }
}
