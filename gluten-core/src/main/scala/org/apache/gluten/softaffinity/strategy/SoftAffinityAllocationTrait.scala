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
package org.apache.gluten.softaffinity.strategy

import org.apache.gluten.GlutenConfig

import org.apache.spark.SparkEnv

import scala.collection.mutable.ListBuffer

trait SoftAffinityAllocationTrait {

  lazy val softAffinityReplicationNum = SparkEnv.get.conf.getInt(
    GlutenConfig.GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM,
    GlutenConfig.GLUTEN_SOFT_AFFINITY_REPLICATIONS_NUM_DEFAULT_VALUE
  )

  /** allocate target executors for file */
  def allocateExecs(
      file: String,
      candidates: ListBuffer[Option[(String, String)]]): Array[(String, String)]
}
