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
package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServer.{createSecurityManager, initSecurity}
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.internal.config.History
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.ShutdownHookManager

import java.net.ServerSocket

object HistoryServerHelper {
  def startHistoryServer(conf: SparkConf): Unit = {
    initSecurity()
    val securityManager = createSecurityManager(conf)
    val providerName = conf
      .get(History.PROVIDER)
      .getOrElse(classOf[FsHistoryProvider].getName())
    val provider = org.apache.spark.util.Utils
      .classForName[ApplicationHistoryProvider](providerName)
      .getConstructor(classOf[SparkConf])
      .newInstance(conf)
    val port = conf.get(History.HISTORY_SERVER_UI_PORT)
    val server = new HistoryServer(conf, provider, securityManager, port)
    server.bind()
    provider.start()
    ShutdownHookManager.addShutdownHook(() => server.stop())
  }

  private def findFreePort(): Int = {
    val port = org.apache.spark.util.Utils
      .tryWithResource(new ServerSocket(0)) {
        socket =>
          socket.setReuseAddress(true)
          socket.getLocalPort
      }
    if (port > 0) {
      return port
    }
    throw new RuntimeException("Could not find a free port")
  }

  def startLogServer(conf: SparkConf): LogServerRpcEnvs = {
    val localHostname = org.apache.spark.util.Utils.localHostName()

    // fake master
    val (rpcEnv, webUiPort, _) = Master.startRpcEnvAndEndpoint(localHostname, 0, 0, conf)
    val masterUrl = "spark://" + org.apache.spark.util.Utils.localHostNameForURI() + ":" +
      rpcEnv.address.port
    val masters = Array(masterUrl)

    // fake worker
    val workerWebUiPort = findFreePort()
    val workerRpcEnv = Worker.startRpcEnvAndEndpoint(
      localHostname,
      0,
      workerWebUiPort,
      0,
      0,
      masters,
      null,
      Some(1),
      conf,
      conf.get(org.apache.spark.internal.config.Worker.SPARK_WORKER_RESOURCE_FILE))

    ShutdownHookManager.addShutdownHook(
      () => {
        workerRpcEnv.shutdown()
        rpcEnv.shutdown()
      })
    LogServerRpcEnvs(rpcEnv, workerRpcEnv, webUiPort, workerWebUiPort)
  }

  case class LogServerRpcEnvs(
      master: RpcEnv,
      worker: RpcEnv,
      masterWebUiPort: Int,
      workerWebUiPort: Int)
}
