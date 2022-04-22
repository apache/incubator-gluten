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

package io.glutenproject.execution

import scala.reflect.ClassTag

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

private[glutenproject] class ZippedPartitionsPartition(
    idx: Int,
    @transient private val rdds: Seq[RDD[_]])
    extends Partition {

  override val index: Int = idx
  var partitionValues = rdds.map(rdd => rdd.partitions(idx))

  def partitions: Seq[Partition] = partitionValues
}

class WholeStageZippedPartitionsRDD[V: ClassTag](
    @transient private val sc: SparkContext,
    var rdds: Seq[RDD[V]],
    val func: Seq[Iterator[V]] => Iterator[V])
    extends RDD[V](sc, rdds.map(x => new OneToOneDependency(x))) {

  override def compute(split: Partition, context: TaskContext): Iterator[V] = {

    val partitions = split.asInstanceOf[ZippedPartitionsPartition].partitions
    val inputIterators = (rdds zip partitions).map {
      case (rdd, partition) => rdd.iterator(partition, context)
    }
    func(inputIterators)
  }

  override def getPartitions: Array[Partition] = {
    val numParts = rdds.head.partitions.length
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
      throw new IllegalArgumentException(
        s"Can't zip RDDs with unequal numbers of partitions: ${rdds.map(_.partitions.length)}")
    }
    Array.tabulate[Partition](numParts) { i => new ZippedPartitionsPartition(i, rdds) }
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}
