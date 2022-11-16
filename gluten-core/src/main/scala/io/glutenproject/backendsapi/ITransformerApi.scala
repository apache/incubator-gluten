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

package io.glutenproject.backendsapi

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, PartitionDirectory}

trait ITransformerApi extends IBackendsApi {

  /**
   * Validate expression for specific backend, including input type.
   * If the expression isn't implemented by the backend or
   * it returns mismatched results with Vanilla Spark,
   * it will fall back to Vanilla Spark.
   *
   * @return true by default
   */
  def doValidate(expr: Expression): Boolean = true

  /**
   * Do validate for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  def validateColumnarShuffleExchangeExec(outputPartitioning: Partitioning,
                                          outputAttributes: Seq[Attribute]): Boolean

  /**
   * Used for table scan validation.
   *
   * @return true if backend supports reading the file format.
   */
  def supportsReadFileFormat(fileFormat: FileFormat): Boolean

  /**
   * Generate Seq[InputPartition] for FileSourceScanExecTransformer.
   */
  def genInputPartitionSeq(relation: HadoopFsRelation,
                           selectedPartitions: Array[PartitionDirectory]): Seq[InputPartition]

}
