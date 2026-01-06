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
package org.apache.gluten.integration.hdfs

import org.apache.gluten.integration.{DataGen, ShimUtils, TypeModifier}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DecimalType, IntegerType, LongType, StringType, StructField, StructType}

import scala.tools.nsc.io.File
import scala.util.Random

class HdfsDataGen(spark: SparkSession, partitions: Int, source: String, dir: String)
  extends Serializable
  with DataGen {

  override def gen(): Unit = {
    val schema = StructType(
      Seq(
        StructField("value1", IntegerType),
        StructField("value2", LongType),
        StructField("value3", StringType),
        StructField("value4", BooleanType),
        StructField("value5", DecimalType(7, 2))
      ))

    spark
      .range(0, partitions, 1L, partitions)
      .mapPartitions {
        itr =>
          val itrArray = itr.toArray
          if (itrArray.length != 1) {
            throw new IllegalStateException()
          }
          val id = itrArray(0)
          val rnd = new Random(id)
          val rows: Iterator[Row] =
            (1 to 10).iterator.map {
              i =>
                val intVal = i
                val longVal = (id.toLong << 32) + i.toLong
                val strVal = s"p=$id row=$i"
                val boolVal = rnd.nextBoolean()
                val decVal =
                  BigDecimal(rnd.nextDouble() * 99999).setScale(2, BigDecimal.RoundingMode.HALF_UP)

                Row(intVal, longVal, strVal, boolVal, decVal)
            }
          rows
      }(ShimUtils.getExpressionEncoder(schema))
      .write
      .format(source)
      .mode(SaveMode.Overwrite)
      .option("path", dir + File.separator + "test")
      .saveAsTable("test")
  }
}
