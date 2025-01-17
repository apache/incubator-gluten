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
package org.apache.gluten.extension.caller

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.columnar.InMemoryRelation

/**
 * Helper API that stores information about the call site of the columnar rule. Specific columnar
 * rules could call the API to check whether this time of rule call was initiated for certain
 * purpose. For example, a rule call could be for AQE optimization, or for cached plan optimization,
 * or for regular executed plan optimization.
 */
trait CallerInfo {
  def isAqe(): Boolean
  def isCache(): Boolean
}

object CallerInfo {
  private val localStorage: ThreadLocal[Option[CallerInfo]] =
    new ThreadLocal[Option[CallerInfo]]() {
      override def initialValue(): Option[CallerInfo] = None
    }

  private class Impl(override val isAqe: Boolean, override val isCache: Boolean) extends CallerInfo

  /*
   * Find the information about the caller that initiated the rule call.
   */
  def create(): CallerInfo = {
    if (localStorage.get().nonEmpty) {
      return localStorage.get().get
    }
    val stack = Thread.currentThread.getStackTrace
    new Impl(isAqe = inAqeCall(stack), isCache = inCacheCall(stack))
  }

  private def inAqeCall(stack: Seq[StackTraceElement]): Boolean = {
    stack.exists(_.getClassName.equals(AdaptiveSparkPlanExec.getClass.getName))
  }

  private def inCacheCall(stack: Seq[StackTraceElement]): Boolean = {
    stack.exists(_.getClassName.equals(InMemoryRelation.getClass.getName))
  }

  /** For testing only. */
  def withLocalValue[T](isAqe: Boolean, isCache: Boolean)(body: => T): T = {
    val prevValue = localStorage.get()
    val newValue = new Impl(isAqe, isCache)
    localStorage.set(Some(newValue))
    try {
      body
    } finally {
      localStorage.set(prevValue)
    }
  }
}
