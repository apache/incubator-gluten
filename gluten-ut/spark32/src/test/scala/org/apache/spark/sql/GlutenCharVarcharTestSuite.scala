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
package org.apache.spark.sql

import org.apache.spark.SparkException

class GlutenFileSourceCharVarcharTestSuite
  extends FileSourceCharVarcharTestSuite
  with GlutenSQLTestsTrait {

  private def testTableWrite(f: String => Unit): Unit = {
    withTable("t")(f("char"))
    withTable("t")(f("varchar"))
  }

  private val VELOX_ERROR_MESSAGE = "Exceeds allowed length limitation: 5"

  testGluten("length check for input string values: nested in struct") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c STRUCT<c: $typeName(5)>) USING $format")
        sql("INSERT INTO t SELECT struct(null)")
        checkAnswer(spark.table("t"), Row(Row(null)))
        val e = intercept[SparkException] {
          sql("INSERT INTO t SELECT struct('123456')")
        }
        assert(e.getCause.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in struct of array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c STRUCT<c: ARRAY<$typeName(5)>>) USING $format")
        sql("INSERT INTO t SELECT struct(array(null))")
        checkAnswer(spark.table("t"), Row(Row(Seq(null))))
        val e = intercept[SparkException](sql("INSERT INTO t SELECT struct(array('123456'))"))
        assert(e.getCause.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }
}

class GlutenDSV2CharVarcharTestSuite extends DSV2CharVarcharTestSuite with GlutenSQLTestsTrait {
  private def testTableWrite(f: String => Unit): Unit = {
    withTable("t")(f("char"))
    withTable("t")(f("varchar"))
  }

  private val VELOX_ERROR_MESSAGE = "Exceeds allowed length limitation: 5"

  testGluten("length check for input string values: nested in struct") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c STRUCT<c: $typeName(5)>) USING $format")
        sql("INSERT INTO t SELECT struct(null)")
        checkAnswer(spark.table("t"), Row(Row(null)))
        val e = intercept[SparkException] {
          sql("INSERT INTO t SELECT struct('123456')")
        }
        assert(e.getCause.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in struct of array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c STRUCT<c: ARRAY<$typeName(5)>>) USING $format")
        sql("INSERT INTO t SELECT struct(array(null))")
        checkAnswer(spark.table("t"), Row(Row(Seq(null))))
        val e = intercept[SparkException](sql("INSERT INTO t SELECT struct(array('123456'))"))
        assert(e.getCause.getMessage.contains(VELOX_ERROR_MESSAGE))
    }
  }
}
