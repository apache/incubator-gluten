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
package org.apache.gluten.integration.clickbench

import org.apache.commons.io.FileUtils
import org.apache.gluten.integration.DataGen
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.language.postfixOps
import scala.sys.process._

class ClickBenchDataGen(val spark: SparkSession, dir: String) extends DataGen {
  import ClickBenchDataGen._
  override def gen(): Unit = {
    println(s"Start to download ClickBench Parquet dataset from URL: $DATA_URL... ")
    // Directly download from official URL.
    val target = new File(dir + File.separator + FILE_NAME)
    FileUtils.forceMkdirParent(target)
    val cmd =
      s"wget --no-verbose --show-progress --progress=bar:force:noscroll -O $target $DATA_URL"
    println(s"Executing command: $cmd")
    val code = Process(cmd) !;
    if (code != 0) {
      throw new RuntimeException("Download failed")
    }
    println(s"ClickBench Parquet dataset successfully downloaded to $target.")
  }
}

object ClickBenchDataGen {
  private val DATA_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"
  private[clickbench] val FILE_NAME = "hits.parquet"
}
