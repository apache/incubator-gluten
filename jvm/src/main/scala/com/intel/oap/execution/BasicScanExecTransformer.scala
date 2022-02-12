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

package com.intel.oap.execution

import com.intel.oap.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import com.intel.oap.substrait.rel.RelBuilder
import com.intel.oap.substrait.SubstraitContext

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.read.InputPartition

trait BasicScanExecTransformer extends TransformSupport {

  def filterExprs(): Seq[Expression]

  def outputAttributes(): Seq[Attribute]

  def getPartitions: Seq[InputPartition]

  override def doTransform(context: SubstraitContext): TransformContext = {
    val output = outputAttributes
    val typeNodes = ConverterUtils.getTypeNodeFromAttributes(output)
    val nameList = new java.util.ArrayList[String]()
    for (attr <- output) {
      nameList.add(attr.name)
    }
    // Will put all filter expressions into an AND expression
    // val functionId = context.registerFunction("AND")
    val transformer = filterExprs.reduceLeftOption(And).map(
      ExpressionConverter.replaceWithExpressionTransformer(_, output)
    )
    val filterNodes = transformer.map(
      _.asInstanceOf[ExpressionTransformer].doTransform(context.registeredFunction))

    // val partNode = LocalFilesBuilder.makeLocalFiles(index, paths, starts, lengths)
    val relNode = RelBuilder.makeReadRel(typeNodes, nameList, filterNodes.getOrElse(null), context)
    TransformContext(output, output, relNode)
  }
}
