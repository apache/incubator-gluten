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
package org.apache.gluten.extension.columnar.enumerated

import org.apache.gluten.execution.{FilterHandler, TransformSupport}
import org.apache.gluten.extension.columnar.validator.Validator
import org.apache.gluten.ras.path.Pattern._
import org.apache.gluten.ras.path.Pattern.Matchers._
import org.apache.gluten.ras.rule.{RasRule, Shape}
import org.apache.gluten.ras.rule.Shapes._

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

// TODO: Match on Vanilla filter + Gluten scan.
class PushFilterToScan(validator: Validator) extends RasRule[SparkPlan] {
  override def shift(node: SparkPlan): Iterable[SparkPlan] = node match {
    case FilterAndScan(filter, scan) =>
      validator.validate(scan) match {
        case Validator.Failed(reason) =>
          List.empty
        case Validator.Passed =>
          val newScan =
            FilterHandler.pushFilterToScan(filter.condition, scan)
          newScan match {
            case ts: TransformSupport if ts.doValidate().isValid =>
              List(filter.withNewChildren(List(ts)))
            case _ =>
              List.empty
          }
      }
    case _ =>
      List.empty
  }

  override def shape(): Shape[SparkPlan] =
    anyOf(
      pattern(
        branch[SparkPlan](
          clazz(classOf[FilterExec]),
          leaf(
            or(clazz(classOf[FileSourceScanExec]), clazz(classOf[BatchScanExec]))
          )
        ).build()),
      pattern(
        branch[SparkPlan](
          clazz(classOf[FilterExec]),
          branch(
            clazz(classOf[ColumnarToRowTransition]),
            leaf(
              or(clazz(classOf[FileSourceScanExec]), clazz(classOf[BatchScanExec]))
            )
          )
        ).build())
    )

  private object FilterAndScan {
    def unapply(node: SparkPlan): Option[(FilterExec, SparkPlan)] = node match {
      case f @ FilterExec(cond, ColumnarToRowExec(scan)) =>
        ensureScan(scan)
        Some(f, scan)
      case f @ FilterExec(cond, scan) =>
        ensureScan(scan)
        Some(f, scan)
      case _ =>
        None
    }

    private def ensureScan(node: SparkPlan): Unit = {
      assert(node.isInstanceOf[FileSourceScanExec] || node.isInstanceOf[BatchScanExec])
    }
  }
}
