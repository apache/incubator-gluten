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
package org.apache.spark.sql.execution.datasources.v1.clickhouse

import scala.collection.mutable.ArrayBuffer

case class TaskWriteInfo(id: String, addFiles: ArrayBuffer[String] = new ArrayBuffer[String]())

object MergeTreeCommiterHelper {

  private val currentTaskWriteInfo = new ThreadLocal[TaskWriteInfo]()

  private def checkAndGet(id: String): TaskWriteInfo = {
    val localInfo = currentTaskWriteInfo.get()
    require(localInfo != null, "currentTaskWriteInfo is null")
    require(localInfo.id == id, s"jobTaskAttemptID not match, ${localInfo.id} != $id")
    localInfo
  }

  /** called at C++ */
  def setCurrentTaskWriteInfo(jobTaskAttemptID: String, resultJson: String): Unit = {
    val localInfo = checkAndGet(jobTaskAttemptID)
    require(resultJson != null && resultJson.nonEmpty)
    localInfo.addFiles.append(resultJson)
  }

  def getAndResetCurrentTaskWriteInfo(jobID: String, taskAttemptID: String): Seq[String] = {
    val localInfo = checkAndGet(s"$jobID/$taskAttemptID")
    currentTaskWriteInfo.remove()
    localInfo.addFiles.toSeq
  }

  def prepareTaskWriteInfo(jobID: String, taskAttemptID: String): Unit = {
    require(currentTaskWriteInfo.get() == null, "currentTaskWriteInfo is not null")
    currentTaskWriteInfo.set(TaskWriteInfo(s"$jobID/$taskAttemptID"))
  }

  def resetCurrentTaskWriteInfo(): Unit = {
    currentTaskWriteInfo.remove()
  }
}
