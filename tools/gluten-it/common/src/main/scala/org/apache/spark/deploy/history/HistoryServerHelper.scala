package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServer.{createSecurityManager, initSecurity}
import org.apache.spark.internal.config.History
import org.apache.spark.util.ShutdownHookManager

object HistoryServerHelper {
  def startHistoryServer(conf: SparkConf): Unit = {
    initSecurity()
    val securityManager = createSecurityManager(conf)
    val providerName = conf.get(History.PROVIDER)
        .getOrElse(classOf[FsHistoryProvider].getName())
    val provider = org.apache.spark.util.Utils.classForName[ApplicationHistoryProvider](providerName)
        .getConstructor(classOf[SparkConf])
        .newInstance(conf)
    val port = conf.get(History.HISTORY_SERVER_UI_PORT)
    val server = new HistoryServer(conf, provider, securityManager, port)
    server.bind()
    provider.start()
    ShutdownHookManager.addShutdownHook { () => server.stop() }
  }
}
