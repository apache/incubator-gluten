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
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.{MetricsUpdater, NoopMetricsUpdater}
import io.glutenproject.substrait.`type`.{ColumnTypeNode, TypeBuilder}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.{Any, StringValue}

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

case class WriteFilesExecTransformer(
    child: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec)
  extends UnaryTransformSupport {
  override def metricsUpdater(): MetricsUpdater = NoopMetricsUpdater

  override def output: Seq[Attribute] = Seq.empty

  def genWriteParameters(): Any = {
    val compressionCodec = if (options.get("parquet.compression").nonEmpty) {
      options.get("parquet.compression").get.toLowerCase().capitalize
    } else SQLConf.get.parquetCompressionCodec.toLowerCase().capitalize

    val writeParametersStr = new StringBuffer("WriteParameters:")
    writeParametersStr.append("is").append(compressionCodec).append("=1").append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(writeParametersStr.toString)
      .build()
    BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
  }

  def createEnhancement(output: Seq[Attribute]): com.google.protobuf.Any = {
    val inputTypeNodes = output.map {
      attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable)
    }

    BackendsApiManager.getTransformerApiInstance.packPBMessage(
      TypeBuilder.makeStruct(false, inputTypeNodes.asJava).toProtobuf)
  }

  def getRelNode(
      context: SubstraitContext,
      originalInputAttributes: Seq[Attribute],
      writePath: String,
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val typeNodes = ConverterUtils.collectAttributeTypeNodes(originalInputAttributes)

    val columnTypeNodes = new java.util.ArrayList[ColumnTypeNode]()
    val inputAttributes = new java.util.ArrayList[Attribute]()
    val childSize = this.child.output.size
    val childOutput = this.child.output
    for (i <- 0 until childSize) {
      val partitionCol = partitionColumns.find(_.exprId == childOutput(i).exprId)
      if (partitionCol.nonEmpty) {
        columnTypeNodes.add(new ColumnTypeNode(1))
        // "aggregate with partition group by can be pushed down"
        // test partitionKey("p") is different with
        // data columns("P").
        inputAttributes.add(partitionCol.get)
      } else {
        columnTypeNodes.add(new ColumnTypeNode(0))
        inputAttributes.add(originalInputAttributes(i))
      }
    }

    val nameList =
      ConverterUtils.collectAttributeNames(inputAttributes.toSeq)

    if (!validation) {
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        genWriteParameters(),
        createEnhancement(originalInputAttributes))
      RelBuilder.makeWriteRel(
        input,
        typeNodes,
        nameList,
        columnTypeNodes,
        writePath,
        extensionNode,
        context,
        operatorId)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val extensionNode =
        ExtensionBuilder.makeAdvancedExtension(createEnhancement(originalInputAttributes))
      RelBuilder.makeWriteRel(
        input,
        typeNodes,
        nameList,
        columnTypeNodes,
        writePath,
        extensionNode,
        context,
        operatorId)
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    val supportedWrite =
      BackendsApiManager.getSettings.supportWriteFilesExec(
        fileFormat,
        child.output.toStructType.fields)
    if (supportedWrite.nonEmpty) {
      return ValidationResult.notOk("Unsupported native write: " + supportedWrite.get)
    }

    if (bucketSpec.nonEmpty) {
      return ValidationResult.notOk("Unsupported native write: bucket write is not supported.")
    }

    val substraitContext = new SubstraitContext
    val operatorId = substraitContext.nextOperatorId(this.nodeName)

    val relNode =
      getRelNode(substraitContext, child.output, "", operatorId, null, validation = true)

    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val writePath = WriteFilesExecTransformer.getWriteFilePath
    val childCtx = child.asInstanceOf[TransformSupport].doTransform(context)

    val operatorId = context.nextOperatorId(this.nodeName)

    val currRel =
      getRelNode(context, child.output, writePath, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Write Rel should be valid")
    TransformContext(childCtx.outputAttributes, output, currRel)
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WriteFilesExecTransformer =
    copy(child = newChild)
}

object WriteFilesExecTransformer {
  private val writeFilePathThreadLocal = new ThreadLocal[String]

  def withWriteFilePath[T](path: String)(f: => T): T = {
    val origin = writeFilePathThreadLocal.get()
    writeFilePathThreadLocal.set(path)
    try {
      f
    } finally {
      writeFilePathThreadLocal.set(origin)
    }
  }

  def getWriteFilePath: String = {
    val writeFilePath = writeFilePathThreadLocal.get()
    assert(writeFilePath != null)
    writeFilePath
  }
}
