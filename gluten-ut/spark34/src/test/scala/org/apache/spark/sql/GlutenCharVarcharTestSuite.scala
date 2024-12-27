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

  private val ERROR_MESSAGE =
    "Exceeds char/varchar type length limitation: 5"

  testGluten("length check for input string values: nested in struct") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c STRUCT<c: $typeName(5)>) USING $format")
        sql("INSERT INTO t SELECT struct(null)")
        checkAnswer(spark.table("t"), Row(Row(null)))
        val e = intercept[RuntimeException] {
          sql("INSERT INTO t SELECT struct('123456')")
        }
        assert(e.getMessage.contains(ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c ARRAY<$typeName(5)>) USING $format")
        sql("INSERT INTO t VALUES (array(null))")
        checkAnswer(spark.table("t"), Row(Seq(null)))
        val e = intercept[SparkException] {
          sql("INSERT INTO t VALUES (array('a', '123456'))")
        }
        assert(e.getMessage.contains(ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in map key") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c MAP<$typeName(5), STRING>) USING $format")
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (map('123456', 'a'))"))
        assert(e.getMessage.contains(ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in map value") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c MAP<STRING, $typeName(5)>) USING $format")
        sql("INSERT INTO t VALUES (map('a', null))")
        checkAnswer(spark.table("t"), Row(Map("a" -> null)))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (map('a', '123456'))"))
        assert(e.getMessage.contains(ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in both map key and value") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c MAP<$typeName(5), $typeName(5)>) USING $format")
        val e1 = intercept[SparkException](sql("INSERT INTO t VALUES (map('123456', 'a'))"))
        assert(e1.getMessage.contains(ERROR_MESSAGE))
        val e2 = intercept[SparkException](sql("INSERT INTO t VALUES (map('a', '123456'))"))
        assert(e2.getMessage.contains(ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in struct of array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c STRUCT<c: ARRAY<$typeName(5)>>) USING $format")
        sql("INSERT INTO t SELECT struct(array(null))")
        checkAnswer(spark.table("t"), Row(Row(Seq(null))))
        val e = intercept[SparkException](sql("INSERT INTO t SELECT struct(array('123456'))"))
        assert(e.getMessage.contains(ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in array of struct") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c ARRAY<STRUCT<c: $typeName(5)>>) USING $format")
        sql("INSERT INTO t VALUES (array(struct(null)))")
        checkAnswer(spark.table("t"), Row(Seq(Row(null))))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (array(struct('123456')))"))
        assert(e.getMessage.contains(ERROR_MESSAGE))
    }
  }

  testGluten("length check for input string values: nested in array of array") {
    testTableWrite {
      typeName =>
        sql(s"CREATE TABLE t(c ARRAY<ARRAY<$typeName(5)>>) USING $format")
        sql("INSERT INTO t VALUES (array(array(null)))")
        checkAnswer(spark.table("t"), Row(Seq(Seq(null))))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (array(array('123456')))"))
        assert(e.getMessage.contains(ERROR_MESSAGE))
    }
  }
}

class GlutenDSV2CharVarcharTestSuite extends DSV2CharVarcharTestSuite with GlutenSQLTestsTrait {}
