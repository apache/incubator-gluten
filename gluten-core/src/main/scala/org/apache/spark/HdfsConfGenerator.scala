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
package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import org.apache.hadoop.conf.Configuration

import java.io.{File, FileOutputStream}

import scala.collection.JavaConverters._

object HdfsConfGenerator extends Logging {
  private val addFileMethod = {
    val m = classOf[SparkContext].getDeclaredMethod(
      "addFile",
      classOf[String],
      classOf[Boolean],
      classOf[Boolean],
      classOf[Boolean])
    m.setAccessible(true)
    m
  }

  private def addFileOnSubmit(sc: SparkContext, path: String): Unit = {
    addFileMethod.invoke(sc, path, Boolean.box(false), Boolean.box(true), Boolean.box(false))
    // Overwrite the spark internal config `spark.app.initial.file.urls`,
    // so that the file can be available before initializing executor plugin.
    assert(sc.listFiles.nonEmpty)
    sc.conf.set("spark.app.initial.file.urls", sc.listFiles().mkString(","))
  }

  private def ignoreKey(key: String): Boolean = {
    key.startsWith("yarn.") || key.startsWith("mapreduce.") || key.startsWith("hive.") || key
      .startsWith("io.") || key.startsWith("ipc.")
  }

  private def hasLibHdfsConf(sc: SparkContext): Boolean = {
    sc.conf.getExecutorEnv.toMap.contains("LIBHDFS3_CONF") ||
    sc.listFiles().exists(_.contains("hdfs-client.xml")) ||
    sys.env
      .get("SPARK_CONF_DIR")
      .orElse(sys.env.get("SPARK_HOME").map(t => s"$t${File.separator}conf"))
      .exists(conf => new File(s"$conf${File.separator}hdfs-client.xml").isFile)
  }

  def addHdfsClientToSparkWorkDirectory(sc: SparkContext): Unit = {
    // Only generate hdfs-client.xml in hdfs env
    if (sc.hadoopConfiguration.get("dfs.nameservices") == null) {
      return
    }

    // Do nothing if people have set config
    if (hasLibHdfsConf(sc)) {
      return
    }

    val tmp = Utils.createTempDir()
    // scalastyle:off
    // See https://github.com/apache/hawq/blob/e9d43144f7e947e071bba48871af9da354d177d0/src/backend/utils/misc/etc/hdfs-client.xml
    // scalastyle:on
    val hdfsClientConfFile = new File(tmp, "hdfs-client.xml")
    val output = new FileOutputStream(hdfsClientConfFile)
    try {
      val config = new Configuration(false)
      sc.hadoopConfiguration
        .iterator()
        .asScala
        .filterNot(x => ignoreKey(x.getKey))
        .foreach(x => config.set(x.getKey, x.getValue))
      config.writeXml(output)
      val tmpPath = hdfsClientConfFile.getAbsolutePath
      addFileOnSubmit(sc, tmpPath)
      logInfo(s"Added hdfs-client.xml for libhdfs, tmp path: $tmpPath.")
    } finally {
      output.close()
    }
  }
}
