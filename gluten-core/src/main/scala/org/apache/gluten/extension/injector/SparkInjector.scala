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

import org.apache.spark.sql.SparkSessionExtensions

/** Injector used to inject query planner rules into Spark. */
class SparkInjector private[injector] (
    control: InjectorControl,
    extensions: SparkSessionExtensions) {
  def injectQueryStagePrepRule(builder: QueryStagePrepRuleBuilder): Unit = {
    extensions.injectQueryStagePrepRule(control.disabler().wrapRule(builder))
  }

  def injectResolutionRule(builder: RuleBuilder): Unit = {
    extensions.injectResolutionRule(control.disabler().wrapRule(builder))
  }

  def injectPostHocResolutionRule(builder: RuleBuilder): Unit = {
    extensions.injectPostHocResolutionRule(control.disabler().wrapRule(builder))
  }

  def injectOptimizerRule(builder: RuleBuilder): Unit = {
    extensions.injectOptimizerRule(control.disabler().wrapRule(builder))
  }

  def injectPlannerStrategy(builder: StrategyBuilder): Unit = {
    extensions.injectPlannerStrategy(control.disabler().wrapStrategy(builder))
  }

  def injectParser(builder: ParserBuilder): Unit = {
    extensions.injectParser(control.disabler().wrapParser(builder))
  }

  def injectFunction(functionDescription: FunctionDescription): Unit = {
    extensions.injectFunction(control.disabler().wrapFunction(functionDescription))
  }

  def injectPreCBORule(builder: RuleBuilder): Unit = {
    extensions.injectPreCBORule(control.disabler().wrapRule(builder))
  }
}
