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
package org.apache.gluten.extension.columnar.cost

import org.apache.spark.sql.execution.SparkPlan

/**
 * Costs one single Spark plan node. The coster returns none if the input plan node is not
 * recognizable.
 *
 * Used by the composite cost model [[LongCosterChain]].
 */
trait LongCoster {

  /** The coster will be registered as part of the cost model associated with this kind. */
  def kind(): LongCostModel.Kind

  /**
   * Calculates the long integer cost of the input query plan node. Note, this calculation should
   * omit children's costs.
   */
  def selfCostOf(node: SparkPlan): Option[Long]
}
