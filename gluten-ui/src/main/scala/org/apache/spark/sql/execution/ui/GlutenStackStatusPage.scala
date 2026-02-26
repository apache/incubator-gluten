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
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.ui.{UIUtils, WebUIPage}

import org.json4s.JValue
import org.json4s.jackson.JsonMethods

import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, Text}

/** Status API for async C++ stack collection. Path: /gluten/stackStatus?requestId=xxxx */
private[ui] class GlutenStackStatusPage(parent: GlutenSQLTab)
  extends WebUIPage("stackStatus")
  with Logging {

  override def renderJson(request: HttpServletRequest): JValue = {
    val reqIdOpt = Option(request.getParameter("requestId")).filter(_.nonEmpty)
    reqIdOpt match {
      case Some(id) =>
        try {
          val rpcEnv: RpcEnv = SparkEnv.get.rpcEnv
          val driverRef: RpcEndpointRef = parent.glutenDriverEndpointRef(rpcEnv)
          val msgClass =
            Class.forName("org.apache.spark.rpc.GlutenRpcMessages$GlutenQueryNativeStackStatus")
          val msg = msgClass.getConstructors.head.newInstance(id).asInstanceOf[AnyRef]
          val resultStr = driverRef.askSync[String](msg)
          parseJsonString(resultStr)
        } catch {
          case t: Throwable =>
            val resultStr = s"""{"status": "error", "message":"query failed: ${t.getMessage}"}"""
            parseJsonString(resultStr)
        }
    }
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val reqIdOpt = Option(request.getParameter("requestId")).filter(_.nonEmpty)
    reqIdOpt match {
      case Some(id) =>
        val base = UIUtils.prependBaseUri(request, parent.basePath)
        // Status API endpoint returning JSON text: `${base}/stackStatus?api=1`.
        val statusAPI = s"$base/gluten/stackStatus/json?requestId=$id"
        val content = Seq(
          <div>
            <div style="margin-top:10px;">
              <b>Request ID:</b> {id}
            </div>
            <div id="stack-status" class="alert alert-info" style="margin-top:10px;">
              Collecting...
            </div>
            <pre id="stack-result" class="table table-bordered" style="white-space:pre-wrap;"></pre>
            <script>
            {
            scala.xml.Unparsed(
              """
                |window.glutenStackStatusInit = function(statusAPI) {
                |  var statusEl = document.getElementById('stack-status');
                |  var resultEl = document.getElementById('stack-result');
                |  var stopped = false;
                |  var controller = new AbortController();
                |  function poll() {
                |    fetch(statusAPI, { signal: controller.signal, cache: 'no-store' })
                |      .then(function(r) { return r.json(); })
                |      .then(function(j) {
                |        try {
                |          // Always update result content if present
                |          if (j.message) { resultEl.textContent = String(j.message || ''); }
                |          if (j.status === 'done') {
                |            statusEl.className = 'alert alert-success';
                |            statusEl.textContent = 'Completed';
                |            stopped = true;
                |          } else if (j.status === 'error') {
                |            statusEl.className = 'alert alert-danger';
                |            statusEl.textContent = 'Failed';
                |            stopped = true;
                |          } else {
                |            statusEl.className = 'alert alert-info';
                |            statusEl.textContent = 'Collecting...';
                |          }
                |        } catch (e) {
                |          statusEl.className = 'alert alert-warning';
                |          statusEl.textContent = 'Status parse failed';
                |        }
                |      })
                |      .catch(function() {
                |        statusEl.className = 'alert alert-warning';
                |        statusEl.textContent = 'Status request failed';
                |      });
                |    if (!stopped) setTimeout(poll, 2000);
                |  }
                |  window.addEventListener('beforeunload', function() {
                |    stopped = true; controller.abort();
                |  });
                |  poll();
                |};
                |""".stripMargin
            )
          }
          </script>
          <script>
          {
            scala.xml.Unparsed("window.glutenStackStatusInit('" + statusAPI + "');")
          }
          </script>
          </div>
        )
        UIUtils.headerSparkPage(request, "Gluten C++ Stack", content, parent)
      case None =>
        Seq(<div class="alert alert-warning">Missing parameter: requestId</div>)
    }
  }

  private def parseJsonString(str: String): JValue = {
    JsonMethods.parse(str)
  }
}
