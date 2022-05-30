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

package io.glutenproject.e2e.tpc.h

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType}

import java.sql.Date

package object velox {
  private val MAX_DIRECT_MEMORY = "6g"

  val VELOX_BACKEND_CONF: SparkConf = new SparkConf()
    .set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.plugins", "io.glutenproject.GlutenPlugin")
    .set("spark.gluten.sql.columnar.backend.lib", "velox")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set("spark.sql.codegen.wholeStage", "true")
    .set("spark.sql.sources.useV1SourceList", "")
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
    .set("spark.executor.heartbeatInterval", "3600000")
    .set("spark.network.timeout", "3601s")
    .set("spark.unsafe.exceptionOnMemoryLeak", "false")
    .set("spark.network.io.preferDirectBufs", "false")

  val GAZELLE_CPP_BACKEND_CONF: SparkConf = new SparkConf()
    .set("spark.memory.offHeap.size", String.valueOf(MAX_DIRECT_MEMORY))
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.plugins", "io.glutenproject.GlutenPlugin")
    .set("spark.gluten.sql.columnar.backend.lib", "gazelle_cpp")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set("spark.sql.codegen.wholeStage", "true")
    .set("spark.sql.sources.useV1SourceList", "")
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
    .set("spark.executor.heartbeatInterval", "3600000")
    .set("spark.network.timeout", "3601s")
    .set("spark.unsafe.exceptionOnMemoryLeak", "false")
    .set("spark.network.io.preferDirectBufs", "false")

  val TYPE_MODIFIER_DATE_AS_DOUBLE: TypeModifier = new TypeModifier(DateType, DoubleType) {
    override def modValue(from: Any): Any = {
      from match {
        case v: Date => v.getTime.asInstanceOf[Double] / 86400.0D / 1000.0D
      }
    }
  }

  val TYPE_MODIFIER_INTEGER_AS_DOUBLE: TypeModifier = new TypeModifier(IntegerType, DoubleType) {
    override def modValue(from: Any): Any = {
      from match {
        case v: Int => v.asInstanceOf[Double]
      }
    }
  }

  val TYPE_MODIFIER_LONG_AS_DOUBLE: TypeModifier = new TypeModifier(LongType, DoubleType) {
    override def modValue(from: Any): Any = {
      from match {
        case v: Long => v.asInstanceOf[Double]
      }
    }
  }
}
