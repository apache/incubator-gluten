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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, InputFileBlockLength, InputFileBlockStart, InputFileName, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarToRowExec, FileSourceScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/**
 * Velox doesn't have input_file_name function support yet and it's also hard to add one, checking
 * this discussion - https://github.com/facebookincubator/velox/issues/9957
 *
 * For workaround, Gluten currently use `InputFileNameReplaceRule` to convert `project` with
 * `input_file_name` to a new project with a new projected metadata column named `$input_file_name$`
 * which store the file name.
 *
 * While if scan is fallback, the replacement would fail the job due to the metadata column
 * `$input_file_name$` not available.
 *
 * This final rule use to address the scan fallback cases. If scan is fallback and has replaced
 * attrs, the rule would inject a project after the scan to convert the replaced attrs back to
 * `input_file_name`.
 *
 * For example: + ProjectTransformer [$input_file_name$, name] +
 * InputIteratorTransformer[$input_file_name$, name] + RowToVeloxColumnar + ColumnarToRow + FileScan
 * [$input_file_name$, name]
 *
 * would be convert to
 *
 * + ProjectTransformer [$input_file_name$, name] + InputIteratorTransformer[$input_file_name$,
 * name] + project [input_file_name() as $input_file_name$, name] + RowToVeloxColumnar +
 * ColumnarToRow + FileScan [name]
 */
case class InputFileNameReplaceFallbackRule(spark: SparkSession) extends Rule[SparkPlan] {
  import InputFileNameReplaceRule._

  private def replacedAttrs(output: Seq[Attribute]): Seq[Attribute] = {
    output.filter(
      p =>
        p.name == replacedInputFileName ||
          p.name == replacedInputFileBlockStart ||
          p.name == replacedInputFileBlockLength)
  }

  private def projectList(
      output: Seq[Attribute],
      replacedAttrs: Seq[Attribute]): Seq[NamedExpression] = {
    output ++ replacedAttrs.map {
      v =>
        v.name match {
          case `replacedInputFileName` =>
            Alias(InputFileName(), replacedInputFileName)(v.exprId)
          case `replacedInputFileBlockStart` =>
            Alias(InputFileBlockStart(), replacedInputFileBlockStart)(v.exprId)
          case `replacedInputFileBlockLength` =>
            Alias(InputFileBlockLength(), replacedInputFileBlockLength)(v.exprId)
        }
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    def insertProjectAfterScanIfFallback(plan: SparkPlan): SparkPlan = {
      plan match {
        case c: ColumnarToRowExec =>
          c.child match {
            case f: FileSourceScanExec =>
              val fallbackAttrs = replacedAttrs(f.output)
              if (fallbackAttrs.nonEmpty) {
                val newScanOutput = f.output.filterNot(fallbackAttrs.contains)
                val scan = f.copy(output = newScanOutput)
                val newProjectList = projectList(scan.output, fallbackAttrs)
                ProjectExec(newProjectList, c.copy(child = scan))
              } else c
            case b: BatchScanExec =>
              val fallbackAttrs = replacedAttrs(b.output)
              if (fallbackAttrs.nonEmpty) {
                val newScanOutput = b.output.filterNot(fallbackAttrs.contains)
                val scan = b.copy(output = newScanOutput)
                val newProjectList = projectList(scan.output, fallbackAttrs)
                ProjectExec(newProjectList, c.copy(child = scan))
              } else c
            case _ =>
              val newChildren = c.children.map(insertProjectAfterScanIfFallback)
              c.withNewChildren(newChildren)
          }
        case other =>
          val newChildren = other.children.map(insertProjectAfterScanIfFallback)
          other.withNewChildren(newChildren)
      }
    }

    insertProjectAfterScanIfFallback(plan)
  }
}
