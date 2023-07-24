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
package org.apache.spark.sql.execution.datasources

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.ColumnarToRowExecBase
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.utils.LogicalPlanSelector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommand, DataWritingCommandExec}
import org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand

case class ColumnarToFakeRowStrategy(session: SparkSession) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] =
    LogicalPlanSelector.maybeNil(session, plan) {
      plan match {
        case FakeRowLogicAdaptor(child: LogicalPlan) =>
          Seq(FakeRowAdaptor(planLater(child)))
        case other =>
          Nil
      }
    }
}

private case class FakeRowLogicAdaptor(child: LogicalPlan) extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output

  // For spark 3.2.
  protected def withNewChildInternal(newChild: LogicalPlan): FakeRowLogicAdaptor =
    copy(child = newChild)
}

/**
 * Whether the child is columnar or not, this operator will convert the columnar output to FakeRow,
 * which is consumable by native parquet/orc writer
 *
 * This is usually used in data writing since Spark doesn't expose APIs to write columnar data as of
 * now.
 */
case class FakeRowAdaptor(child: SparkPlan) extends UnaryExecNode with GlutenPlan {
  if (child.logicalLink.isDefined) {
    setLogicalLink(FakeRowLogicAdaptor(child.logicalLink.get))
  }

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    if (child.supportsColumnar) {
      child.executeColumnar().map(cb => new FakeRow(cb))
    } else {
      val r2c = BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(child)
      r2c.executeColumnar().map(cb => new FakeRow(cb))
    }
  }

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): FakeRowAdaptor =
    copy(child = newChild)
}

object GlutenColumnarRules {

  // TODO: support Insert clause

  // TODO: support ctas in Spark3.4, see https://github.com/apache/spark/pull/39220
  // TODO: support dynamic partition and bucket write
  //  1. pull out `Empty2Null` and required ordering to `WriteFilesExec`, see Spark3.4 `V1Writes`
  //  2. support detect partition value, partition path, bucket value, bucket path at native side,
  //     see `BaseDynamicPartitionDataWriter`
  def isGlutenInsertInto(cmd: DataWritingCommand): Boolean = {
    cmd match {
      case command: CreateDataSourceTableAsSelectCommand =>
        if (command.table.provider.contains("velox")) {
          throw new UnsupportedOperationException(
            "Velox file format does not support create table as select.")
        }
        false
      case command: InsertIntoHadoopFsRelationCommand
          if command.fileFormat.isInstanceOf[GlutenParquetFileFormat] =>
        if (command.partitionColumns.nonEmpty || command.bucketSpec.nonEmpty) {
          throw new UnsupportedOperationException(
            "Velox file format does not support dynamic partition write and bucket write.")
        }
        true
      case command: InsertIntoHiveDirCommand =>
        command.storage.outputFormat.get
          .equals("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
      case _ => false
    }
  }

  case class NativeWritePostRule(session: SparkSession) extends Rule[SparkPlan] {
    override def apply(p: SparkPlan): SparkPlan = p match {
      case rc @ DataWritingCommandExec(cmd, child) if isGlutenInsertInto(cmd) =>
        child match {
          // if the child is columnar, we can just wrap&transfer the columnar data
          case c2r: ColumnarToRowExecBase =>
            rc.withNewChildren(Array(FakeRowAdaptor(c2r.child)))
          // If the child is aqe, we make aqe "support columnar",
          // then aqe itself will guarantee to generate columnar outputs.
          // So FakeRowAdaptor will always consumes columnar data,
          // thus taking no risk to downgrade performance
          case aqe: AdaptiveSparkPlanExec =>
            rc.withNewChildren(
              Array(
                FakeRowAdaptor(
                  AdaptiveSparkPlanExec(
                    aqe.inputPlan,
                    aqe.context,
                    aqe.preprocessingRules,
                    aqe.isSubquery,
                    supportsColumnar = true
                  ))))
          case other => rc.withNewChildren(Array(FakeRowAdaptor(other)))
        }
      case plan: SparkPlan => plan.withNewChildren(plan.children.map(apply))
    }
  }
}
