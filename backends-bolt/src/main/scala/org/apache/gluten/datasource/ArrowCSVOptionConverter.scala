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
package org.apache.gluten.datasource

import org.apache.gluten.utils.ArrowAbiUtil

import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkSchemaUtil

import com.google.common.collect.ImmutableMap
import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.dataset.scanner.csv.{CsvConvertOptions, CsvFragmentScanOptions}
import org.apache.arrow.memory.BufferAllocator

import java.util

object ArrowCSVOptionConverter {
  def convert(option: CSVOptions): CsvFragmentScanOptions = {
    val parseMap = new util.HashMap[String, String]()
    val default = new CSVOptions(
      CaseInsensitiveMap(Map()),
      option.columnPruning,
      SparkSchemaUtil.getLocalTimezoneID)
    parseMap.put("strings_can_be_null", "true")
    if (option.delimiter != default.delimiter) {
      parseMap.put("delimiter", option.delimiter)
    }
    if (option.escapeQuotes != default.escapeQuotes) {
      parseMap.put("quoting", (!option.escapeQuotes).toString)
    }

    val convertOptions = new CsvConvertOptions(ImmutableMap.of())
    new CsvFragmentScanOptions(convertOptions, ImmutableMap.of(), parseMap)
  }

  def schema(
      requiredSchema: StructType,
      cSchema: ArrowSchema,
      allocator: BufferAllocator,
      option: CsvFragmentScanOptions): Unit = {
    val schema = SparkSchemaUtil.toArrowSchema(requiredSchema)
    ArrowAbiUtil.exportSchema(allocator, schema, cSchema)
    option.getConvertOptions.setArrowSchema(cSchema)
  }

}
