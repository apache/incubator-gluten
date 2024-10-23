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
package org.apache.gluten.extension.injector

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/** Injector used to inject query planner rules into Spark. */
class SparkInjector private[injector] (control: InjectorControl, extensions: SparkSessionExtensions) {
  def injectQueryStagePrepRule(builder: SparkSession => Rule[SparkPlan]): Unit = {
    extensions.injectQueryStagePrepRule(session => control.physicalRuleWithDisabler(session, builder(session)))
  }

  def injectResolutionRule(builder: SparkSession => Rule[LogicalPlan]): Unit = {
    extensions.injectResolutionRule(builder)
  }

  def injectPostHocResolutionRule(builder: SparkSession => Rule[LogicalPlan]): Unit = {
    extensions.injectPostHocResolutionRule(builder)
  }

  def injectOptimizerRule(builder: SparkSession => Rule[LogicalPlan]): Unit = {
    extensions.injectOptimizerRule(builder)
  }

  def injectPlannerStrategy(builder: SparkSession => Strategy): Unit = {
    extensions.injectPlannerStrategy(builder)
  }

  def injectParser(builder: (SparkSession, ParserInterface) => ParserInterface): Unit = {
    extensions.injectParser(builder)
  }

  def injectFunction(
      functionDescription: (FunctionIdentifier, ExpressionInfo, FunctionBuilder)): Unit = {
    extensions.injectFunction(functionDescription)
  }
}
