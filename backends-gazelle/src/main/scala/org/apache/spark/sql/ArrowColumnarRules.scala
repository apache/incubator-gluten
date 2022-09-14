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

package org.apache.spark.sql

import io.glutenproject.execution.VeloxRowToArrowColumnarExec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.VeloxColumnarRules.ColumnarToFakeRowAdaptor
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasource.arrow.ArrowFileFormat

object ArrowColumnarRules {
  case class ArrowWritePostRule(session: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan match {
      case rc@DataWritingCommandExec(cmd, ColumnarToRowExec(child)) =>
        cmd match {
          case command: InsertIntoHadoopFsRelationCommand =>
            if (command.fileFormat.isInstanceOf[ParquetFileFormat]) {
              val new_rc = rc.withNewChildren(Array(ColumnarToFakeRowAdaptor(child)))
              val new_cmd = command.copy(fileFormat = new ArrowFileFormat())
              new_rc.asInstanceOf[DataWritingCommandExec].copy(cmd = new_cmd)
            } else {
              plan.withNewChildren(plan.children.map(apply))
            }
          case _ => plan.withNewChildren(plan.children.map(apply))
        }
      case rc@DataWritingCommandExec(cmd, child) =>
        cmd match {
          case command: InsertIntoHadoopFsRelationCommand =>
            if (command.fileFormat.isInstanceOf[ParquetFileFormat]) {
              val new_rc = child match {
                case c: AdaptiveSparkPlanExec =>
                  rc.withNewChildren(
                    Array(
                      AdaptiveSparkPlanExec(
                        ColumnarToFakeRowAdaptor(c.inputPlan),
                        c.context,
                        c.preprocessingRules,
                        c.isSubquery)))
                case other =>
                  rc.withNewChildren(
                    Array(ColumnarToFakeRowAdaptor(new VeloxRowToArrowColumnarExec(child))))
              }
              val new_cmd = command.copy(fileFormat = new ArrowFileFormat())
              new_rc.asInstanceOf[DataWritingCommandExec].copy(cmd = new_cmd)
            } else {
              plan.withNewChildren(plan.children.map(apply))
            }
          case _ => plan.withNewChildren(plan.children.map(apply))
        }
    }
  }
}
