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
package org.apache.spark.sql.execution.commands

import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.CacheResult
import org.apache.gluten.execution.CacheResult.Status

import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.rpc.GlutenRpcMessages.{CacheJobInfo, GlutenCacheLoadStatus}
import org.apache.spark.sql.Row
import org.apache.spark.util.ThreadUtils

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object GlutenCacheBase {
  def ALL_EXECUTORS: String = "allExecutors"

  def toExecutorId(executorId: String): String =
    executorId.split("_").last

  protected def waitRpcResults
      : ArrayBuffer[(String, Future[CacheJobInfo])] => ArrayBuffer[(String, CacheJobInfo)] =
    (futureList: ArrayBuffer[(String, Future[CacheJobInfo])]) => {
      val resultList = ArrayBuffer[(String, CacheJobInfo)]()
      futureList.foreach(
        f => {
          resultList.append((f._1, ThreadUtils.awaitResult(f._2, Duration.Inf)))
        })
      resultList
    }

  def checkExecutorId(executorId: String): Unit = {
    if (!GlutenDriverEndpoint.executorDataMap.containsKey(toExecutorId(executorId))) {
      throw new GlutenException(
        s"executor $executorId not found," +
          s" all executors are ${GlutenDriverEndpoint.executorDataMap.toString}")
    }
  }

  def waitAllJobFinish(
      jobs: ArrayBuffer[(String, CacheJobInfo)],
      ask: (String, String) => Future[CacheResult]): (Boolean, String) = {
    val res = collectJobTriggerResult(jobs)
    var status = res._1
    val messages = res._2
    jobs.foreach(
      job => {
        if (status) {
          var complete = false
          while (!complete) {
            Thread.sleep(5000)
            val future_result = ask(job._1, job._2.jobId)
            val result = ThreadUtils.awaitResult(future_result, Duration.Inf)
            result.getStatus match {
              case Status.ERROR =>
                status = false
                messages.append(
                  s"executor : ${job._1}, failed with message: ${result.getMessage};"
                )
                complete = true
              case Status.SUCCESS =>
                complete = true
              case _ =>
              // still running
            }
          }
        }
      })
    (status, messages.mkString(";"))
  }

  def collectJobTriggerResult(
      jobs: ArrayBuffer[(String, CacheJobInfo)]): (Boolean, ArrayBuffer[String]) = {
    var status = true
    val messages = ArrayBuffer[String]()
    jobs.foreach(
      job => {
        if (!job._2.status) {
          messages.append(job._2.reason)
          status = false
        }
      })
    (status, messages)
  }

  def getResult(
      futureList: ArrayBuffer[(String, Future[CacheJobInfo])],
      async: Boolean): Seq[Row] = {
    val resultList = waitRpcResults(futureList)
    if (async) {
      val res = collectJobTriggerResult(resultList)
      Seq(Row(res._1, res._2.mkString(";")))
    } else {
      val fetchStatus: (String, String) => Future[CacheResult] =
        (executorId: String, jobId: String) => {
          val data = GlutenDriverEndpoint.executorDataMap.get(toExecutorId(executorId))
          if (data == null) {
            Future.successful(
              new CacheResult(
                CacheResult.Status.ERROR,
                s"The executor($executorId) status has changed, please try again later"))
          } else {
            data.executorEndpointRef
              .ask[CacheResult](GlutenCacheLoadStatus(jobId))
          }
        }
      val res = waitAllJobFinish(resultList, fetchStatus)
      Seq(Row(res._1, res._2))
    }
  }
}
