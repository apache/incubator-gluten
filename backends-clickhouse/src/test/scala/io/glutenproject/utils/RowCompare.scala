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
package io.glutenproject.utils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.GlutenQueryTest.sameRows

class RowCompare extends GlutenQueryTest {

  test("x") {
    val row1 = Seq(
      Row(null, "xx2"),
      Row(null, "xx2")
    )
    val row2 = Seq(
      Row(null, "xx2"),
      Row(null, "xx")
    )
    checkPartiallySortedRows(row1, row2, Set(0))
  }

  def checkPartiallySortedRows(
      row1: Seq[Row],
      row2: Seq[Row],
      sortedIndex: Set[Int]): Unit = {

    val newRow1 = row1.map(extractSorted(_, sortedIndex))
    val newRow2 = row2.map(extractSorted(_, sortedIndex))

    sameRows(newRow1, newRow2, isSorted = true).map {
      results =>
        s"""
           |Results do not match for sorted ow:
           |== Results ==
           |$results
         """.stripMargin
    } match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  def extractSorted(row: Row, sortedIndex: Set[Int]): Row = {
    Row.fromSeq(row.toSeq.zipWithIndex
      .filter { case (_, index) => sortedIndex.contains(index) }
      .map(_._1))
  }
  override protected def spark: SparkSession = null
}
