/*
 * Copyright 2020 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.sql.shims.spark32

import io.glutenproject.sql.shims.{ShimDescriptor, SparkShims}
import io.glutenproject.{BackendLib, GlutenConfig}
import io.glutenproject.extension.JoinSelectionOverrideShim

import org.apache.spark.sql.catalyst.plans.physical.{Distribution, HashClusteredDistribution, Partitioning}
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SHUFFLE_MERGE}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.{DataSourcePartitioning, DataSourceRDD}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.{joins, SparkPlan}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

class Spark32Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def getKeyPartition(newPartitions: Seq[InputPartition],
                      originalPartition: Partitioning): Seq[Seq[InputPartition]] = {
    originalPartition match {
      case p: DataSourcePartitioning if p.numPartitions != newPartitions.size =>
        throw new SparkException(
          "Data source must have preserved the original partitioning during runtime filtering; " +
            s"reported num partitions: ${p.numPartitions}, " +
            s"num partitions after runtime filtering: ${newPartitions.size}")
      case _ =>
      // no validation is needed as the data source did not report any specific partitioning
    }
    newPartitions.map(Seq(_))
  }

  override def newDatasourceRDD(sc: SparkContext, inputPartitions: Seq[Seq[InputPartition]],
                                 partitionReaderFactory: PartitionReaderFactory,
                                 columnarReads: Boolean,
                                 customMetrics: Map[String, SQLMetric]): RDD[InternalRow] = {
    new DataSourceRDD(sc, inputPartitions.flatten, partitionReaderFactory,
      columnarReads, customMetrics)
  }

  override def getDistribution(leftKeys: Seq[Expression], rightKeys: Seq[Expression])
    : Seq[Distribution] = {
    HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
  }

  override def applyPlan(plan: LogicalPlan,
                forceShuffledHashJoin: Boolean,
                backendLib: BackendLib): Seq[SparkPlan] = {
    plan match {
      // If the build side of BHJ is already decided by AQE, we need to keep the build side.
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right, hint) =>
        new JoinSelectionOverrideShim().extractEqualJoinKeyCondition(joinType, leftKeys, rightKeys,
          condition, left, right, hint,
          forceShuffledHashJoin, backendLib)
      case _ => Nil
    }
  }
}
