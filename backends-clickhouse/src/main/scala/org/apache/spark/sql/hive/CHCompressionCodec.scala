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
package org.apache.spark.sql.hive

import io.glutenproject.backendsapi.BackendsApiManager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{BZip2Codec, CompressionCodecFactory, DefaultCodec}
import org.slf4j.LoggerFactory

case class CHCompressionCodec(name: String, supported: Boolean) {}

object CHCompressionCodec {
  val log = LoggerFactory.getLogger(classOf[CHCompressionCodec])
  val bzip2 = new CHCompressionCodec("BZip2", true)
  val zlib = new CHCompressionCodec("ZLib", true)
  val none = new CHCompressionCodec("None", true)
  val codecFactory: CompressionCodecFactory = createCodecFactory

  private def createCodecFactory: CompressionCodecFactory = {
    val conf = new Configuration()
    new CompressionCodecFactory(conf)
  }

  def getCompressionCodec(path: String): String = {
    val codec = codecFactory.getCodec(new Path(path))
    codec match {
      case d: DefaultCodec => CHCompressionCodec.zlib.name
      case b: BZip2Codec => CHCompressionCodec.bzip2.name
      case _ => CHCompressionCodec.none.name
    }
  }

  def compressionSplittable(path: String): Boolean = {
    val codec = getCompressionCodec(path)
    codec match {
      case "None" => true
      case _ => BackendsApiManager.getValidatorApiInstance.doCompressionSplittableValidate(codec)
    }
  }

}
