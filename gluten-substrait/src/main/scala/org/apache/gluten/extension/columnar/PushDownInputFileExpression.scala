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
package org.apache.gluten.extension.columnar

import org.apache.gluten.execution.{BatchScanExecTransformer, FileSourceScanExecTransformer}

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{DeserializeToObjectExec, LeafExecNode, ProjectExec, SerializeFromObjectExec, SparkPlan, UnionExec}

import scala.collection.mutable

/**
 * The Spark implementations of input_file_name/input_file_block_start/input_file_block_length uses
 * a thread local to stash the file name and retrieve it from the function. If there is a
 * transformer node between project input_file_function and scan, the result of input_file_name is
 * an empty string. So we should push down input_file_function to transformer scan or add fallback
 * project of input_file_function before fallback scan.
 *
 * Two rules are involved:
 *   - Before offload, add new project before leaf node and push down input file expression to the
 *     new project
 *   - After offload, if scan be offloaded, push down input file expression into scan and remove
 *     project
 */
object PushDownInputFileExpression {
  def containsInputFileRelatedExpr(expr: Expression): Boolean = {
    expr match {
      case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
      case _ => expr.children.exists(containsInputFileRelatedExpr)
    }
  }

  object PreOffload extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
      case ProjectExec(projectList, child) if projectList.exists(containsInputFileRelatedExpr) =>
        val replacedExprs = mutable.Map[String, Alias]()
        val newProjectList = projectList.map {
          expr => rewriteExpr(expr, replacedExprs).asInstanceOf[NamedExpression]
        }
        val newChild = addMetadataCol(child, replacedExprs)
        ProjectExec(newProjectList, newChild)
    }

    private def rewriteExpr(
        expr: Expression,
        replacedExprs: mutable.Map[String, Alias]): Expression =
      expr match {
        case _: InputFileName =>
          replacedExprs
            .getOrElseUpdate(expr.prettyName, Alias(InputFileName(), expr.prettyName)())
            .toAttribute
        case _: InputFileBlockStart =>
          replacedExprs
            .getOrElseUpdate(expr.prettyName, Alias(InputFileBlockStart(), expr.prettyName)())
            .toAttribute
        case _: InputFileBlockLength =>
          replacedExprs
            .getOrElseUpdate(expr.prettyName, Alias(InputFileBlockLength(), expr.prettyName)())
            .toAttribute
        case other =>
          other.withNewChildren(other.children.map(child => rewriteExpr(child, replacedExprs)))
      }

    private def addMetadataCol(
        plan: SparkPlan,
        replacedExprs: mutable.Map[String, Alias]): SparkPlan =
      plan match {
        case p: LeafExecNode =>
          ProjectExec(p.output ++ replacedExprs.values, p)
        // Output of SerializeFromObjectExec's child and output of DeserializeToObjectExec must be
        // a single-field row.
        case p @ (_: SerializeFromObjectExec | _: DeserializeToObjectExec) =>
          ProjectExec(p.output ++ replacedExprs.values, p)
        case p: ProjectExec =>
          p.copy(
            projectList = p.projectList ++ replacedExprs.values.toSeq.map(_.toAttribute),
            child = addMetadataCol(p.child, replacedExprs))
        case u @ UnionExec(children) =>
          val newFirstChild = addMetadataCol(children.head, replacedExprs)
          val newOtherChildren = children.tail.map {
            child =>
              // Make sure exprId is unique in each child of Union.
              val newReplacedExprs = replacedExprs.map {
                expr => (expr._1, Alias(expr._2.child, expr._2.name)())
              }
              addMetadataCol(child, newReplacedExprs)
          }
          u.copy(children = newFirstChild +: newOtherChildren)
        case p => p.withNewChildren(p.children.map(child => addMetadataCol(child, replacedExprs)))
      }
  }

  object PostOffload extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
      case p @ ProjectExec(projectList, child: FileSourceScanExecTransformer)
          if projectList.exists(containsInputFileRelatedExpr) =>
        child.copy(output = p.output)
      case p @ ProjectExec(projectList, child: BatchScanExecTransformer)
          if projectList.exists(containsInputFileRelatedExpr) =>
        child.copy(output = p.output.asInstanceOf[Seq[AttributeReference]])
    }
  }
}
