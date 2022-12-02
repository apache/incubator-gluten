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

import io.glutenproject.execution.{ColumnarToFakeRowAdaptor, GlutenDataRowToArrowColumnarExec}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.velox.DwrfFileFormat
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object VeloxColumnarRules {

  case class OtherWritePostRule(session: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan match {
      case rc@DataWritingCommandExec(cmd, ColumnarToRowExec(child)) =>
        cmd match {
          case command: InsertIntoHadoopFsRelationCommand =>
            if (command.fileFormat.isInstanceOf[DwrfFileFormat]) {
              rc.withNewChildren(Array(ColumnarToFakeRowAdaptor(child)))
            } else {
              plan.withNewChildren(plan.children.map(apply))
            }
          case _ => plan.withNewChildren(plan.children.map(apply))
        }
      case rc@DataWritingCommandExec(cmd, child) =>
        cmd match {
          case command: InsertIntoHadoopFsRelationCommand =>
            if (command.fileFormat.isInstanceOf[DwrfFileFormat]) {
              child match {
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
                    Array(ColumnarToFakeRowAdaptor(GlutenDataRowToArrowColumnarExec(child))))
              }
            } else {
              plan.withNewChildren(plan.children.map(apply))
            }
          case _ => plan.withNewChildren(plan.children.map(apply))
        }
      case plan: SparkPlan => plan.withNewChildren(plan.children.map(apply))
    }
  }
}
