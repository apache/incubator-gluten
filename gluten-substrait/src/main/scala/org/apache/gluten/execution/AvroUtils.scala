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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

import scala.io.Source

object AvroUtils {
  val AVRO_SCHEMA_OPTION = "avroSchema"
  val AVRO_SCHEMA_URL_OPTION = "avroSchemaUrl"
  val AVRO_SCHEMA_TABLE_PROPERTY = "avro.schema.literal"
  val AVRO_SCHEMA_URL_TABLE_PROPERTY = "avro.schema.url"

  val AVRO_POSITIONAL_FIELD_MATCHING_OPTION = "positionalFieldMatching"

  def createAvroOptionFromOptions(options: Map[String, String]): Map[String, String] = {
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    val builder = Map.newBuilder[String, String]
    getAvroJsonSchema(
      caseInsensitiveOptions.get(AVRO_SCHEMA_OPTION),
      caseInsensitiveOptions.get(AVRO_SCHEMA_URL_OPTION))
      .foreach(s => builder += (AVRO_SCHEMA_OPTION -> s))
    builder += (AVRO_POSITIONAL_FIELD_MATCHING_OPTION -> caseInsensitiveOptions.getOrElse(
      AVRO_POSITIONAL_FIELD_MATCHING_OPTION,
      false.toString))
    builder.result()
  }

  def createAvroOptionFromTableProperties(properties: Map[String, String]): Map[String, String] = {
    val caseInsensitiveProperties = CaseInsensitiveMap(properties)
    val builder = Map.newBuilder[String, String]
    getAvroJsonSchema(
      caseInsensitiveProperties.get(AVRO_SCHEMA_TABLE_PROPERTY),
      caseInsensitiveProperties.get(AVRO_SCHEMA_URL_TABLE_PROPERTY))
      .foreach(s => builder += (AVRO_SCHEMA_OPTION -> s))
    builder.result()
  }

  def getJsonSchema(options: java.util.Map[String, String]): Option[String] = {
    if (!options.containsKey(AVRO_SCHEMA_OPTION)) {
      return None
    }
    Some(options.get(AVRO_SCHEMA_OPTION))
  }

  private def getAvroJsonSchema(
      schema: Option[String],
      schemaUrl: Option[String]): Option[String] = {
    schema.orElse(schemaUrl.map {
      url =>
        val hadoopConf = SparkSession.getActiveSession.get.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(new URI(url), hadoopConf)
        val in = fs.open(new Path(url))
        try {
          Source.fromInputStream(in).mkString
        } finally {
          in.close()
        }
    })
  }
}
