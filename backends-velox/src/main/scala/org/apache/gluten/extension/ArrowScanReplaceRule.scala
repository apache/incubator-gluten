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
package org.apache.gluten.extension

import org.apache.gluten.datasource.ArrowCSVFileFormat
import org.apache.gluten.datasource.v2.ArrowCSVScan
import org.apache.gluten.execution.datasource.v2.ArrowBatchScanExec

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ArrowFileSourceScanExec, FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

case class ArrowScanReplaceRule(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case plan: FileSourceScanExec if plan.relation.fileFormat.isInstanceOf[ArrowCSVFileFormat] =>
        ArrowFileSourceScanExec(plan)
      case plan: BatchScanExec if plan.scan.isInstanceOf[ArrowCSVScan] =>
        ArrowBatchScanExec(plan)
      case plan: BatchScanExec => plan
      case p => p
    }
  }
}
