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
package io.glutenproject.exec

import org.apache.spark.util.TaskResources

object ExecutionCtxs {
  private val EXECUTION_CTX_NAME = "ExecutionCtx"

  /** Get or create the execution ctx which bound with Spark TaskContext. */
  def contextInstance(): ExecutionCtx = {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.")
    }

    TaskResources.addResourceIfNotRegistered(EXECUTION_CTX_NAME, () => create())
  }

  /** Create a temporary execution ctx, caller must invoke ExecutionCtx#release manually. */
  def tmpInstance(): ExecutionCtx = {
    create()
  }

  private def create(): ExecutionCtx = {
    new ExecutionCtx
  }
}
