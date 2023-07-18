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
package org.apache.spark.sql.expression

import io.glutenproject.backendsapi.velox.BackendSettings
import io.glutenproject.expression.{ConverterUtils, ExpressionTransformer, Transformable}
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.{ExpressionType, TypeConverter}
import io.glutenproject.substrait.expression.ExpressionBuilder
import io.glutenproject.udf.UdfJniWrapper

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.types.DataType

import com.google.common.collect.Lists

import scala.collection.mutable

case class UDFExpression(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    children: Seq[Expression])
  extends Transformable {
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = {
    this.copy(children = newChildren)
  }

  override def getTransformer(
      childrenTransformers: Seq[ExpressionTransformer]): ExpressionTransformer = {
    if (childrenTransformers.size != children.size) {
      throw new IllegalStateException(
        this.getClass.getSimpleName +
          ": getTransformer called before children transformer initialized.")
    }
    (args: Object) => {
      val transformers = childrenTransformers.map(_.doTransform(args))
      val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
      val functionId = ExpressionBuilder.newScalarFunction(
        functionMap,
        ConverterUtils.makeFuncName(name, children.map(_.dataType), FunctionConfig.REQ))

      val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
      ExpressionBuilder.makeScalarFunction(
        functionId,
        Lists.newArrayList(transformers: _*),
        typeNode)
    }
  }
}

object UDFResolver extends Logging {
  val UDFMap: mutable.Map[String, ExpressionType] = mutable.Map()

  def registerUDF(name: String, bytes: Array[Byte]): Unit = {
    registerUDF(name, TypeConverter.from(bytes))
  }

  def registerUDF(name: String, t: ExpressionType): Unit = {
    UDFMap.update(name, t)
    logInfo(s"Registered UDF: $name -> $t")
  }

  def loadAndGetFunctionDescriptions: Seq[(FunctionIdentifier, ExpressionInfo, FunctionBuilder)] = {
    SparkContext.getActive.get.conf.getOption(BackendSettings.GLUTEN_VELOX_UDF_LIB_PATHS).foreach {
      libPaths => new UdfJniWrapper().nativeLoadUdfLibraries(libPaths)
    }

    UDFMap.map {
      case (name, t) =>
        (
          new FunctionIdentifier(name),
          new ExpressionInfo(classOf[UDFExpression].getName, name),
          (e: Seq[Expression]) => UDFExpression(name, t.dataType, t.nullable, e))
    }.toSeq
  }
}
