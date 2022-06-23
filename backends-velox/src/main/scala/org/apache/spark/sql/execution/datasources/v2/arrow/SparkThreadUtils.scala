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

package org.apache.spark.sql.execution.datasources.v2.arrow

import io.glutenproject.vectorized.NativeThreadJniWrapper
import org.apache.spark.TaskContext
import org.apache.spark.util.Utils

import java.lang.reflect.{Field, Method}
import java.util.regex.Pattern
import scala.collection.JavaConversions._

/**
 * Use thread name to recognize the parent task. This might be a sub-optimal solution for
 * backends that manage threading by themselves since one backend thread per task is more
 * reasonable for Spark application.
 */
object SparkThreadUtils {

  private val _m_ThreadLocal_setInitialValue: Method =
    classOf[ThreadLocal[TaskContext]].getDeclaredMethod("setInitialValue")
  private val _f_Thread_threadLocals: Field =
    classOf[Thread].getDeclaredField("threadLocals")
  private val _f_TaskContext_taskContext: Field =
    Utils.classForName("org.apache.spark.TaskContext$")
      .getDeclaredField("taskContext")
  private val _f_TaskContext_taskContext_MODULE$: Field =
    Utils.classForName("org.apache.spark.TaskContext$")
      .getDeclaredField("MODULE$")
  private val _m_ThreadLocal_ThreadLocalMap_getEntry: Method =
    Utils.classForName("java.lang.ThreadLocal$ThreadLocalMap")
      .getDeclaredMethod("getEntry", classOf[ThreadLocal[TaskContext]])
  private val _f_ThreadLocal_ThreadLocalMap_Entry_value: Field =
    Utils.classForName("java.lang.ThreadLocal$ThreadLocalMap$Entry")
      .getDeclaredField("value")

  _m_ThreadLocal_setInitialValue.setAccessible(true)
  _f_Thread_threadLocals.setAccessible(true)
  _f_TaskContext_taskContext.setAccessible(true)
  _f_TaskContext_taskContext_MODULE$.setAccessible(true)
  _m_ThreadLocal_ThreadLocalMap_getEntry.setAccessible(true)
  _f_ThreadLocal_ThreadLocalMap_Entry_value.setAccessible(true)

  private val SPARK_TASK_CHILD_THREAD_NAME_PATTERN: Pattern = Pattern.compile(
    "G-(?<JPARENTTHREAID>\\d+)-(?<NTHREADID>\\d+)")

  private val parentThreadCache: ThreadLocal[Option[Thread]] =
    ThreadLocal.withInitial(() => getParentThread())

  private def getParentThread(): Option[Thread] = {
    if (TaskContext.get() != null) {
      return Some(Thread.currentThread())
    }
    val matcher = SPARK_TASK_CHILD_THREAD_NAME_PATTERN
      .matcher(NativeThreadJniWrapper.getNativeThreadName)
    if (!matcher.matches()) {
      return None
    }
    val javaParentThreadId = matcher.group(1)
    val parentThread = getThreadById(javaParentThreadId.toLong)
    Some(parentThread)
  }

  private def getThreadById(threadId: Long): Thread = {
    for (t <- Thread.getAllStackTraces.keySet) {
      if (t.getId == threadId) return t
    }
    throw new IllegalStateException(s"No thread ID $threadId")
  }

  private def getTaskContextForThread(parentThread: Thread): Option[TaskContext] = {
    val taskContext = _f_TaskContext_taskContext.get(
      _f_TaskContext_taskContext_MODULE$.get(null))
    val threadLocalMap = _f_Thread_threadLocals.get(parentThread)
    if (threadLocalMap == null) {
      return None
    }
    val e = _m_ThreadLocal_ThreadLocalMap_getEntry.invoke(threadLocalMap, taskContext)
    if (e == null) {
      return None
    }
    val value = _f_ThreadLocal_ThreadLocalMap_Entry_value.get(e)
    Some(value.asInstanceOf[TaskContext])
  }

  private def getTaskContext(): Option[TaskContext] = {
    parentThreadCache.get().flatMap(t => getTaskContextForThread(t))
  }

  def inSparkTask(): Boolean = {
    getTaskContext().isDefined
  }

  def getAssociatedTaskContext: TaskContext = {
    getTaskContext().getOrElse(throw new IllegalStateException())
  }
}
