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
package org.apache.spark.sql.execution.ui

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.ui.{UIUtils, WebUIPage}

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

/** 同步获取 C++ 栈的页面：/gluten/stackSync?executorId=xxxx */
private[ui] class GlutenStackSyncPage(parent: GlutenSQLTab)
  extends WebUIPage("stackSync")
  with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val execIdOpt = Option(request.getParameter("executorId")).filter(_.nonEmpty)
    execIdOpt match {
      case Some(executorId) =>
        try {
          val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv
          val driverRef: RpcEndpointRef = parent.glutenDriverEndpointRef(rpcEnv)
          val msgClass =
            Class.forName("org.apache.spark.rpc.GlutenRpcMessages$GlutenQueryNativeStackSync")
          val msg = msgClass.getConstructors.head.newInstance(executorId).asInstanceOf[AnyRef]
          val resultStr = driverRef.askSync[String](msg)
          val content = Seq(
            <div>
              <div style="margin-top:10px;"><b>Executor:</b> {executorId}</div>
              <div class="alert alert-info" style="margin-top:10px;">
                Sync fetch complete (blocking).
              </div>
              <pre class="table table-bordered" style="white-space:pre-wrap;">
                {resultStr}
              </pre>
            </div>
          )
          UIUtils.headerSparkPage(request, "Gluten C++ Stack (Sync)", content, parent)
        } catch {
          case t: Throwable =>
            val content = Seq(
              <div class="alert alert-danger">
                <b>Sync fetch failed:</b>
                <pre>{Option(t.getMessage).getOrElse(t.toString)}</pre>
              </div>
            )
            UIUtils.headerSparkPage(request, "Gluten C++ Stack (Sync)", content, parent)
        }
      case None =>
        Seq(<div class="alert alert-warning">Missing parameter: executorId</div>)
    }
  }
}
