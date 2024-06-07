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
package org.apache.gluten.extension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, InputFileName, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.StringType

object InputFileNameReplaceRule {
  val replacedInputFileName = "$_input_file_name_$"
}

case class InputFileNameReplaceRule(spark: SparkSession) extends Rule[SparkPlan] {
  import InputFileNameReplaceRule._

  def isInputFileName(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName => true
      case _ => false
    }
  }
  override def apply(plan: SparkPlan): SparkPlan = {
    val inputFileNameCol = AttributeReference(replacedInputFileName, StringType, true)()

    def replaceInputFileName(expr: Expression): Expression = {
      expr match {
        case e if isInputFileName(e) => inputFileNameCol
        case other =>
          other.withNewChildren(other.children.map(child => replaceInputFileName(child)))
      }
    }

    def applyInputFileNameCol(plan: SparkPlan): SparkPlan = {
      plan match {
        case _ @ProjectExec(projectList, child) =>
          val newProjectList = projectList.map {
            expr => replaceInputFileName(expr).asInstanceOf[NamedExpression]
          }
          val newChild = applyInputFileNameCol(child)
          ProjectExec(newProjectList, newChild)
        case f: FileSourceScanExec
            if !f.output.exists(attr => attr.exprId == inputFileNameCol.exprId) =>
          f.copy(output = f.output :+ inputFileNameCol.toAttribute)
        case b: BatchScanExec if !b.output.exists(attr => attr.exprId == inputFileNameCol.exprId) =>
          b.copy(output = b.output :+ inputFileNameCol)
        case other =>
          val newChildren = other.children.map(applyInputFileNameCol)
          other.withNewChildren(newChildren)
      }
    }
    applyInputFileNameCol(plan)
  }
}
