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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.types.{LongType, StringType}

object InputFileNameReplaceRule {
  val replacedInputFileName = "$input_file_name$"
  val replacedInputFileBlockStart = "$input_file_block_start$"
  val replacedInputFileBlockLength = "$input_file_block_length$"
}

case class InputFileNameReplaceRule(spark: SparkSession) extends Rule[SparkPlan] {
  import InputFileNameReplaceRule._

  private def isInputFileName(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName => true
      case _ => false
    }
  }

  private def isInputFileBlockStart(expr: Expression): Boolean = {
    expr match {
      case _: InputFileBlockStart => true
      case _ => false
    }
  }

  private def isInputFileBlockLength(expr: Expression): Boolean = {
    expr match {
      case _: InputFileBlockLength => true
      case _ => false
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    val replacedExprs = scala.collection.mutable.Map[String, AttributeReference]()

    def hasParquetScan(plan: SparkPlan): Boolean = {
      plan match {
        case fileScan: FileSourceScanExec
            if fileScan.relation.fileFormat.isInstanceOf[ParquetFileFormat] =>
          true
        case batchScan: BatchScanExec =>
          batchScan.scan match {
            case _: ParquetScan => true
            case _ => false
          }
        case _ => plan.children.exists(hasParquetScan)
      }
    }

    def mayNeedConvert(expr: Expression): Boolean = {
      expr match {
        case e if isInputFileName(e) => true
        case s if isInputFileBlockStart(s) => true
        case l if isInputFileBlockLength(l) => true
        case other => other.children.exists(mayNeedConvert)
      }
    }

    def doConvert(expr: Expression): Expression = {
      expr match {
        case e if isInputFileName(e) =>
          replacedExprs.getOrElseUpdate(
            replacedInputFileName,
            AttributeReference(replacedInputFileName, StringType, true)())
        case s if isInputFileBlockStart(s) =>
          replacedExprs.getOrElseUpdate(
            replacedInputFileBlockStart,
            AttributeReference(replacedInputFileBlockStart, LongType, true)()
          )
        case l if isInputFileBlockLength(l) =>
          replacedExprs.getOrElseUpdate(
            replacedInputFileBlockLength,
            AttributeReference(replacedInputFileBlockLength, LongType, true)()
          )
        case other =>
          other.withNewChildren(other.children.map(child => doConvert(child)))
      }
    }

    def ensureChildOutputHasNewAttrs(plan: SparkPlan): SparkPlan = {
      plan match {
        case _ @ProjectExec(projectList, child) =>
          var newProjectList = projectList
          for ((_, newAttr) <- replacedExprs) {
            if (!newProjectList.exists(attr => attr.exprId == newAttr.exprId)) {
              newProjectList = newProjectList :+ newAttr.toAttribute
            }
          }
          val newChild = ensureChildOutputHasNewAttrs(child)
          ProjectExec(newProjectList, newChild)
        case f: FileSourceScanExec =>
          var newOutput = f.output
          for ((_, newAttr) <- replacedExprs) {
            if (!newOutput.exists(attr => attr.exprId == newAttr.exprId)) {
              newOutput = newOutput :+ newAttr.toAttribute
            }
          }
          f.copy(output = newOutput)

        case b: BatchScanExec =>
          var newOutput = b.output
          for ((_, newAttr) <- replacedExprs) {
            if (!newOutput.exists(attr => attr.exprId == newAttr.exprId)) {
              newOutput = newOutput :+ newAttr
            }
          }
          b.copy(output = newOutput)
        case other =>
          val newChildren = other.children.map(ensureChildOutputHasNewAttrs)
          other.withNewChildren(newChildren)
      }
    }

    def replaceInputFileNameInProject(plan: SparkPlan): SparkPlan = {
      plan match {
        case _ @ProjectExec(projectList, child)
            if projectList.exists(mayNeedConvert) && hasParquetScan(plan) =>
          val newProjectList = projectList.map {
            expr => doConvert(expr).asInstanceOf[NamedExpression]
          }
          val newChild = replaceInputFileNameInProject(ensureChildOutputHasNewAttrs(child))
          ProjectExec(newProjectList, newChild)
        case other =>
          val newChildren = other.children.map(replaceInputFileNameInProject)
          other.withNewChildren(newChildren)
      }
    }
    replaceInputFileNameInProject(plan)
  }
}
