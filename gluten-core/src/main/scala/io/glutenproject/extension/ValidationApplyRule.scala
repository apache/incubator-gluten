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
package io.glutenproject.extension

import org.apache.spark.sql.execution.SparkPlan

/**
 * Spark Rule that needs to be applied before validation should extends this trait, and implements
 * the applyForValidation method. This methods should apply the rule locally(There is no need to
 * recursively traverse the entire plan tree) to the input SparkPlan, and return the transformed
 * SparkPlan.
 */
trait ValidationApplyRule {
  def applyForValidation[T <: SparkPlan](plan: T): T
}
