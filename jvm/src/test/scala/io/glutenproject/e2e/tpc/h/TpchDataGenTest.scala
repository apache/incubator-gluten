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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, LongType}

import java.sql.Date

class TpchDataGenTest extends QueryTest with SharedSparkSession {

  test("gen tpch data") {
    val dataGen = new TpchDataGen(spark, 0.1D, TpchDataGenTest.TPCH_WRITE_PATH)
    dataGen.gen()
  }

  test("gen tpch data with type modifiers 1") {
    val modifiers = Array(new TypeModifier(LongType, DoubleType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: Long => v.asInstanceOf[Double]
        }
      }
    })
    val dataGen = new TpchDataGen(spark, 0.1D, TpchDataGenTest.TPCH_WRITE_PATH, modifiers)
    dataGen.gen()
  }

  test("gen tpch data with type modifiers 2") {
    val modifiers = Array(new TypeModifier(DateType, DoubleType) {
      override def modValue(from: Any): Any = {
        from match {
          case v: Date => v.getTime.asInstanceOf[Double] / 86400.0D / 1000.0D
        }
      }
    })
    val dataGen = new TpchDataGen(spark, 0.1D, TpchDataGenTest.TPCH_WRITE_PATH, modifiers)
    dataGen.gen()
  }
}

object TpchDataGenTest {
  private val TPCH_WRITE_PATH = "/tmp/tpch-generated-test"
}
