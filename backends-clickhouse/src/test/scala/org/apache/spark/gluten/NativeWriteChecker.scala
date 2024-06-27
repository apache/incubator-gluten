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
package org.apache.spark.gluten

import org.apache.gluten.execution.GlutenClickHouseWholeStageTransformerSuite

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.FakeRowAdaptor
import org.apache.spark.sql.util.QueryExecutionListener

trait NativeWriteChecker extends GlutenClickHouseWholeStageTransformerSuite {

  def checkNativeWrite(sqlStr: String, checkNative: Boolean): Unit = {
    var nativeUsed = false

    val queryListener = new QueryExecutionListener {
      override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}
      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        if (!nativeUsed) {
          nativeUsed = if (isSparkVersionGE("3.4")) {
            false
          } else {
            qe.executedPlan.find(_.isInstanceOf[FakeRowAdaptor]).isDefined
          }
        }
      }
    }

    try {
      spark.listenerManager.register(queryListener)
      spark.sql(sqlStr)
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assertResult(checkNative)(nativeUsed)
    } finally {
      spark.listenerManager.unregister(queryListener)
    }
  }
}
