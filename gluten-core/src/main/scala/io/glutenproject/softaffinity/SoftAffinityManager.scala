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

package io.glutenproject.softaffinity

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable

import io.glutenproject.GlutenConfig
import io.glutenproject.softaffinity.strategy.SoftAffinityStrategy
import io.glutenproject.utils.LogLevelUtil

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

object SoftAffinityManager extends LogLevelUtil with Logging {

  val resourceRWLock = new ReentrantReadWriteLock(true)

  val softAffinityAllocation = new SoftAffinityStrategy

  lazy val minOnTargetHosts = SparkEnv.get.conf.getInt(
    GlutenConfig.GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS,
    GlutenConfig.GLUTEN_SOFT_AFFINITY_MIN_TARGET_HOSTS_DEFAULT_VALUE
  )

  // (execId, host) list
  val fixedIdForExecutors = new mutable.ListBuffer[Option[(String, String)]]()
  // host list
  val nodesExecutorsMap = new mutable.HashMap[String, mutable.HashSet[String]]()

  protected val totalRegisteredExecutors = new AtomicInteger(0)

  lazy val usingSoftAffinity = SparkEnv.get.conf.getBoolean(
    GlutenConfig.GLUTEN_SOFT_AFFINITY_ENABLED,
    GlutenConfig.GLUTEN_SOFT_AFFINITY_ENABLED_DEFAULT_VALUE
  )

  private val softAffinityLogLevel = GlutenConfig.getConf.softAffinityLogLevel

  def totalExecutors(): Int = totalRegisteredExecutors.intValue()

  def handleExecutorAdded(execHostId: (String, String)): Unit = {
    resourceRWLock.writeLock().lock()
    try {
      // first, check whether the execId exists
      if (!fixedIdForExecutors.exists( exec => {
        exec.isDefined && exec.get._1.equals(execHostId._1)
      })) {
        val executorsSet = nodesExecutorsMap.getOrElseUpdate(execHostId._2,
          new mutable.HashSet[String]())
        executorsSet.add(execHostId._1)
        if (fixedIdForExecutors.exists(_.isEmpty)) {
          // replace the executor which was removed
          val replaceIdx = fixedIdForExecutors.indexWhere(_.isEmpty)
          fixedIdForExecutors(replaceIdx) = Option(execHostId)
        } else {
          fixedIdForExecutors += Option(execHostId)
        }
        totalRegisteredExecutors.addAndGet(1)
      }
      logOnLevel(softAffinityLogLevel,
        s"After adding executor ${execHostId._1} on host ${execHostId._2}, " +
          s"fixedIdForExecutors is ${fixedIdForExecutors.mkString(",")}, " +
          s"nodesExecutorsMap is ${nodesExecutorsMap.keySet.mkString(",")}, " +
          s"actual executors count is ${totalRegisteredExecutors.intValue()}."
      )
    } finally {
      resourceRWLock.writeLock().unlock()
    }
  }

  def handleExecutorRemoved(execId: String): Unit = {
    resourceRWLock.writeLock().lock()
    try {
      val execIdx = fixedIdForExecutors.indexWhere( execHost => {
        if (execHost.isDefined) {
          execHost.get._1.equals(execId)
        } else {
          false
        }
      })
      if (execIdx != -1) {
        val findedExecId = fixedIdForExecutors(execIdx)
        fixedIdForExecutors(execIdx) = None
        val nodeExecs = nodesExecutorsMap.get(findedExecId.get._2).get
        nodeExecs -= findedExecId.get._1
        if (nodeExecs.isEmpty) {
          // there is no executor on this host, remove
          nodesExecutorsMap.remove(findedExecId.get._2)
        }
        totalRegisteredExecutors.addAndGet(-1)
      }
      logOnLevel(softAffinityLogLevel, s"After removing executor ${execId}, " +
        s"fixedIdForExecutors is ${fixedIdForExecutors.mkString(",")}, " +
        s"nodesExecutorsMap is ${nodesExecutorsMap.keySet.mkString(",")}, " +
        s"actual executors count is ${totalRegisteredExecutors.intValue()}."
      )
    } finally {
      resourceRWLock.writeLock().unlock()
    }
  }

  def checkTargetHosts(hosts: Array[String]): Boolean = {
    resourceRWLock.readLock().lock()
    try {
      if (hosts.length < 1) {
        // there is no host locality
        false
      } else if (nodesExecutorsMap.size < 1) {
        true
      } else {
        // when the replication num of hdfs is less than 'minOnTargetHosts'
        val minHostsNum = Math.min(minOnTargetHosts, hosts.length)
        // there are how many the same hosts
        nodesExecutorsMap.map(_._1).toArray.intersect(hosts).size >= minHostsNum
      }
    } finally {
      resourceRWLock.readLock().unlock()
    }
  }

  def askExecutors(file: String): Array[(String, String)] = {
    resourceRWLock.readLock().lock()
    try {
      if (nodesExecutorsMap.size < 1) {
        Array.empty
      } else {
        softAffinityAllocation.allocateExecs(file, fixedIdForExecutors)
      }
    } finally {
      resourceRWLock.readLock().unlock()
    }
  }
}
