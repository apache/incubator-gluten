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
package org.apache.gluten.table.planner.expressions.utils

import org.apache.gluten.table.runtime.stream.common.GlutenStreamingTestBase

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.planner.expressions.utils.ExpressionTestBase
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical._
import org.junit.jupiter.api.Assertions.{assertThrows, assertTrue}

import scala.collection.JavaConverters._
import scala.collection.mutable

trait GlutenExpressionTestBase {
  self: ExpressionTestBase =>

  private lazy val initOnce: Unit = {
    try {
      import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment
      Velox4jEnvironment.initializeOnce()
    } catch {
      case e: Exception =>
        throw e
    }
  }

  protected object GlutenHelper extends GlutenStreamingTestBase {
    override def before(): Unit = {
      initOnce
      super.before()
    }
  }

  protected val tableName = "testTable"
  protected val glutenTestCases = mutable.ArrayBuffer[(String, String)]()
  protected val glutenExceptionTests =
    mutable.ArrayBuffer[(String, String, Class[_ <: Throwable])]()

  abstract override def evaluateExprs(): Unit = {
    evaluateGlutenTestCases()
    evaluateGlutenExceptionTests()
  }

  abstract override def testAllApis(expr: Expression, sqlExpr: String, expected: String): Unit = {
    addGlutenSqlTest(sqlExpr, expected)
  }

  abstract override def testSqlApi(sqlExpr: String, expected: String): Unit = {
    addGlutenSqlTest(sqlExpr, expected)
  }

  abstract override def testSqlApi(sqlExpr: String): Unit = {
    addGlutenSqlTest(sqlExpr, null)
  }

  abstract override def testTableApi(expr: Expression, expected: String): Unit = {
    // Skip TableAPI tests
  }

  abstract override def testExpectedSqlException(
      sqlExpr: String,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException]): Unit = {
    glutenExceptionTests += ((sqlExpr, keywords, clazz))
  }

  abstract override def testExpectedTableApiException(
      expr: Expression,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException]): Unit = {
    // Skip TableAPI exception tests
  }

  abstract override def testExpectedAllApisException(
      expr: Expression,
      sqlExpr: String,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException]): Unit = {
    glutenExceptionTests += ((sqlExpr, keywords, clazz))
  }

  protected def addGlutenSqlTest(sqlExpr: String, expected: String): Unit = {
    glutenTestCases += ((sqlExpr, expected))
  }

  protected def evaluateGlutenTestCases(): Unit = {
    glutenTestCases.foreach {
      case (sqlExpr, expected) =>
        val query = s"SELECT $sqlExpr FROM $tableName "

        if (expected != null) {
          val expectedResult = if (expected == "NULL") "+I[null]" else s"+I[$expected]"
          GlutenHelper.runAndCheck(query, java.util.Arrays.asList(expectedResult))
        } else {
          GlutenHelper.tEnv.executeSql(query).collect()
        }
    }
  }

  protected def evaluateGlutenExceptionTests(): Unit = {
    glutenExceptionTests.foreach {
      case (sqlExpr, keywords, clazz) =>
        val query = s"SELECT $sqlExpr FROM $tableName"
        val exception = assertThrows(
          clazz,
          () => {
            GlutenHelper.tEnv.executeSql(query).collect()
          })

        if (keywords != null) {
          assertTrue(
            exception.getMessage.contains(keywords),
            s"Exception message should contain '$keywords', but was: ${exception.getMessage}")
        }
    }
  }

  protected def setupGlutenTestTable(): Unit = {
    val data = testData
    val dataType = testDataType.asInstanceOf[DataType]

    val rows = java.util.Arrays.asList(data)
    val schema = convertDataTypeToSchema(dataType)
    GlutenHelper.createSimpleBoundedValuesTable(tableName, schema, rows)
  }

  protected def convertDataTypeToSchema(dataType: DataType): String = {
    val rowType = dataType.getLogicalType.asInstanceOf[RowType]
    val fields = rowType.getFields.asScala

    fields
      .map {
        field =>
          val fieldName = field.getName
          val fieldType = convertLogicalTypeToSql(field.getType)
          s"$fieldName $fieldType"
      }
      .mkString(", ")
  }

  protected def convertLogicalTypeToSql(logicalType: LogicalType): String = {
    logicalType match {
      case d: DecimalType => s"decimal(${d.getPrecision}, ${d.getScale})"
      case _: IntType => "int"
      case _: BigIntType => "bigint"
      case _: DoubleType => "double"
      case _: VarCharType => "string"
      case _: BooleanType => "boolean"
      case _: FloatType => "float"
      case _: TinyIntType => "tinyint"
      case _: SmallIntType => "smallint"
      case _: DateType => "date"
      case _: TimeType => "time"
      case _: TimestampType => "timestamp"
      case _ => "string"
    }
  }
}
