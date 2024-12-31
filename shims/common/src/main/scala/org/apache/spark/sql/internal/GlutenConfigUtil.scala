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
package org.apache.spark.sql.internal

import org.apache.spark.internal.config.ConfigReader

import scala.collection.JavaConverters._

object GlutenConfigUtil {
  private def getConfString(reader: ConfigReader, key: String, value: String): String = {
    Option(SQLConf.getConfigEntry(key))
      .map {
        _.readFrom(reader) match {
          case o: Option[_] => o.map(_.toString).getOrElse(value)
          case null => value
          case v => v.toString
        }
      }
      .getOrElse(value)
  }

  def parseConfig(conf: Map[String, String]): Map[String, String] = {
    val reader = new ConfigReader(conf.filter(_._1.startsWith("spark.gluten.")).asJava)
    conf.map {
      case (k, v) =>
        if (k.startsWith("spark.gluten.")) {
          (k, getConfString(reader, k, v))
        } else {
          (k, v)
        }
    }.toMap
  }
}
