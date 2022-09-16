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

package io.glutenproject.sql.shims.spark33

import io.glutenproject.sql.shims.{ShimDescriptor, SparkShims}

import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, PartitionReaderFactory}
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.datasources.v2.Spark33Scan
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.{KeyGroupedPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.util.InternalRowSet
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD
import org.apache.spark.sql.execution.metric.SQLMetric

class Spark33Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  def getKeyPartition(newPartitions: Seq[InputPartition],
                      originalPartitioning: Partitioning): Seq[Seq[InputPartition]] = {
    originalPartitioning match {
      case p: KeyGroupedPartitioning =>
        if (newPartitions.exists(!_.isInstanceOf[HasPartitionKey])) {
          throw new SparkException("Data source must have preserved the original partitioning " +
            "during runtime filtering: not all partitions implement HasPartitionKey after " +
            "filtering")
        }

        val newRows = new InternalRowSet(p.expressions.map(_.dataType))
        newRows ++= newPartitions.map(_.asInstanceOf[HasPartitionKey].partitionKey())
        val oldRows = p.partitionValuesOpt.get

        if (oldRows.size != newRows.size) {
          throw new SparkException("Data source must have preserved the original partitioning " +
            "during runtime filtering: the number of unique partition values obtained " +
            s"through HasPartitionKey changed: before ${oldRows.size}, after ${newRows.size}")
        }

        if (!oldRows.forall(newRows.contains)) {
          throw new SparkException("Data source must have preserved the original partitioning " +
            "during runtime filtering: the number of unique partition values obtained " +
            s"through HasPartitionKey remain the same but do not exactly match")
        }

        new Spark33Scan().groupPartitions(newPartitions).get.map(_._2)
    }
  }

  override def newDatasourceRDD(sc: SparkContext, inputPartitions: Seq[Seq[InputPartition]],
                                partitionReaderFactory: PartitionReaderFactory,
                                columnarReads: Boolean,
                                customMetrics: Map[String, SQLMetric]): RDD[InternalRow] = {
    new DataSourceRDD(sc, inputPartitions, partitionReaderFactory, columnarReads, customMetrics)
  }
}
