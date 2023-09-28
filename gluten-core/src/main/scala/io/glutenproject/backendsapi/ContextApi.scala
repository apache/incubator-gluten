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

import org.apache.spark.SparkConf
import org.apache.spark.util.TaskResource

import java.util

trait ContextApi {
  def initialize(conf: SparkConf, isDriver: Boolean = true): Unit = {}

  def shutdown(): Unit = {}

  def taskResourceFactories(): Seq[() => TaskResource] = Seq()

  /**
   * Should call by driver. Collect Broadcast Hash Table Ids.
   *
   * @param executionId
   *   execution id
   * @param buildHashTableId
   *   build hashtable id
   */
  def collectExecutionBroadcastHashTableId(executionId: String, buildHashTableId: String): Unit = {}

  /**
   * Should call by executor. On execution end. Clean executor broadcast build hashtable.
   *
   * @param executionId
   *   execution id
   * @param broadcastHashIds
   *   broadcast hashtable ids
   */
  def cleanExecutionBroadcastHashtable(
      executionId: String,
      broadcastHashIds: util.Set[String]): Unit = {}
}
