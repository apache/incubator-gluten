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

package org.apache.spark.rdd

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * This file is copied from Spark
 *
 * Changed it from a 0-partition RDD to a 1-partition RDD, where the single partition returns empty
 * iterator
 *
 * This can stop mapper stage to be skipped, solving
 * https://github.com/Kyligence/ClickHouse/issues/161
 */
private[spark] class EmptyRDD[T: ClassTag](sc: SparkContext) extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    if (BackendsApiManager.chBackend) {
      Array(new MyEmptyRDDPartition(0))
    } else {
      Array.empty
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    Iterator.empty
  }
}

private[spark] class MyEmptyRDDPartition(idx: Int) extends Partition {
  val index = idx
}
