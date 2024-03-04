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
package org.apache.spark.sql.execution.datasources.v1

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable

class CHTextWriterInjects extends CHFormatWriterInjects {
  override def nativeConf(
      options: Map[String, String],
      compressionCodec: String): java.util.Map[String, String] = {
    val sparkOptions = new mutable.HashMap[String, String]()
    // TODO: implement it
    sparkOptions.asJava
  }

  override def getFormatName(): String = {
    "text"
  }
}
