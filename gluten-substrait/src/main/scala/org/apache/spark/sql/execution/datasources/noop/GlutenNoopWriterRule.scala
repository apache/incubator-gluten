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
package org.apache.spark.sql.execution.datasources.noop

import org.apache.gluten.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.GlutenWriterColumnarRules.injectFakeRowAdaptor
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec}

case class GlutenNoopWriterRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(p: SparkPlan): SparkPlan = p match {
    case rc @ AppendDataExec(_, _, NoopWrite) if GlutenConfig.get.enableNoopWriter =>
      injectFakeRowAdaptor(rc, rc.child)
    case rc @ OverwriteByExpressionExec(_, _, NoopWrite) if GlutenConfig.get.enableNoopWriter =>
      injectFakeRowAdaptor(rc, rc.child)
    case _ => p
  }
}
