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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan}

class Spark33Scan extends DataSourceV2ScanExecBase {
  override def scan: Scan = ???

  override def readerFactory: PartitionReaderFactory = ???

  override def keyGroupedPartitioning: Option[Seq[Expression]] = ???

  override protected def inputPartitions: Seq[InputPartition] = ???

  override def inputRDD: RDD[InternalRow] = ???

  override def output: Seq[Attribute] = ???

  override def productElement(n: Int): Any = ???

  override def productArity: Int = ???

  override def canEqual(that: Any): Boolean = ???


}
