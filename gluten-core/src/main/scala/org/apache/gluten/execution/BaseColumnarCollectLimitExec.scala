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
package org.apache.gluten.execution

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.extension.GlutenPlan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.UnaryExecNode

abstract class BaseColumnarCollectLimitExec extends UnaryExecNode with GlutenPlan {
  def limit: Int
  def offset: Int

  override def output: Seq[Attribute] = child.output
  override def supportsColumnar: Boolean = true
  override def supportExecuteCollect: Boolean = true
  override def outputPartitioning: Partitioning = SinglePartition
  override protected def doExecute(): RDD[InternalRow] = {
    throw new GlutenNotSupportException()
  }
}
