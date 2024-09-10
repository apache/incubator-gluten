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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec, SparkPlan}

import java.util.ServiceLoader
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

object ProjectTransformerFactory {

  private val projectTransformerMap = new ConcurrentHashMap[String, Class[_]]()

  def createProjectTransformer(projectExec: ProjectExec): ProjectExecTransformerBase = {
    val dataLakeClass = getDataLakeClass(projectExec)
    lookupProjectTransformer(dataLakeClass) match {
      case Some(clz) =>
        clz
          .getDeclaredConstructor()
          .newInstance()
          .asInstanceOf[ProjectTransformerRegister]
          .createProjectTransformer(projectExec)
      case _ =>
        BackendsApiManager.getSparkPlanExecApiInstance
          .genProjectExecTransformer(projectExec.projectList, projectExec.child)
    }
  }

  private def lookupProjectTransformer(dataLakeClass: String): Option[Class[_]] = {
    val clz = projectTransformerMap.computeIfAbsent(
      dataLakeClass,
      _ => {
        val loader = Option(Thread.currentThread().getContextClassLoader)
          .getOrElse(getClass.getClassLoader)
        val serviceLoader = ServiceLoader.load(classOf[ProjectTransformerRegister], loader)
        serviceLoader.asScala
          .filter(service => dataLakeClass.contains(service.dataLakeClass))
          .toList match {
          case head :: Nil =>
            // there is exactly one registered alias
            head.getClass
          case _ => null
        }
      }
    )
    Option(clz)
  }

  private def getDataLakeClass(plan: SparkPlan): String = {
    if (supportedDelta(plan)) {
      "delta"
    } else {
      "vanilla"
    }
  }

  private def supportedDelta(plan: SparkPlan): Boolean = {
    plan.find {
      p =>
        p.isInstanceOf[FileSourceScanExec] &&
        p.asInstanceOf[FileSourceScanExec].relation.fileFormat.getClass.getName.contains("Delta")
    }.isDefined
  }
}
