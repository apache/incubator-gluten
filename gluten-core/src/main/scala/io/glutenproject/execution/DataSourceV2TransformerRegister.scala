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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

/**
 * Data sources v2 transformer should implement this trait so that they can register an alias to
 * their data source v2 transformer. This allows users to give the data source v2 transformer alias
 * as the format type over the fully qualified class name.
 */
trait DataSourceV2TransformerRegister {

  /**
   * The scan class name that this data source v2 transformer provider adapts. This is overridden by
   * children to provide a alias for the data source v2 transformer. For example:
   *
   * {{{
   *   override def scanClassName(): String = "org.apache.iceberg.spark.source.SparkBatchQueryScan"
   * }}}
   */
  def scanClassName(): String

  def createDataSourceV2Transformer(
      batchScan: BatchScanExec,
      partitionFilters: Seq[Expression]): BatchScanExecTransformer
}
