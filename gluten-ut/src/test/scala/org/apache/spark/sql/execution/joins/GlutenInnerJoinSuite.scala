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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.{DataFrame, GlutenSQLTestsBaseTrait, Row}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.{joins, FilterExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.internal.SQLConf

class GlutenInnerJoinSuite extends InnerJoinSuite with GlutenSQLTestsBaseTrait {
  private val EnsureRequirements = new EnsureRequirements()

  // Note: the input dataframes and expression must be evaluated lazily because
  // the SQLContext should be used only within a test to keep SQL tests stable
  private def testInnerJoin(
                             testName: String,
                             leftRows: => DataFrame,
                             rightRows: => DataFrame,
                             condition: () => Expression,
                             expectedAnswer: Seq[Product]): Unit = {

    def extractJoinParts(): Option[ExtractEquiJoinKeys.ReturnType] = {
      val join = Join(leftRows.logicalPlan, rightRows.logicalPlan,
        Inner, Some(condition()), JoinHint.NONE)
      ExtractEquiJoinKeys.unapply(join)
    }

    def makeBroadcastHashJoin(
                               leftKeys: Seq[Expression],
                               rightKeys: Seq[Expression],
                               boundCondition: Option[Expression],
                               leftPlan: SparkPlan,
                               rightPlan: SparkPlan,
                               side: BuildSide) = {
      val broadcastJoin = joins.BroadcastHashJoinExec(
        leftKeys,
        rightKeys,
        Inner,
        side,
        boundCondition,
        leftPlan,
        rightPlan)
      EnsureRequirements.apply(broadcastJoin)
    }

    def makeShuffledHashJoin(
                              leftKeys: Seq[Expression],
                              rightKeys: Seq[Expression],
                              boundCondition: Option[Expression],
                              leftPlan: SparkPlan,
                              rightPlan: SparkPlan,
                              side: BuildSide) = {
      val shuffledHashJoin = joins.ShuffledHashJoinExec(leftKeys, rightKeys, Inner,
        side, None, leftPlan, rightPlan)
      val filteredJoin =
        boundCondition.map(FilterExec(_, shuffledHashJoin)).getOrElse(shuffledHashJoin)
      EnsureRequirements.apply(filteredJoin)
    }

    def makeSortMergeJoin(
                           leftKeys: Seq[Expression],
                           rightKeys: Seq[Expression],
                           boundCondition: Option[Expression],
                           leftPlan: SparkPlan,
                           rightPlan: SparkPlan) = {
      val sortMergeJoin = joins.SortMergeJoinExec(leftKeys, rightKeys, Inner, boundCondition,
        leftPlan, rightPlan)
      EnsureRequirements.apply(sortMergeJoin)
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using BroadcastHashJoin (build=left)") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeBroadcastHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, BuildLeft),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using BroadcastHashJoin (build=right)") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeBroadcastHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, BuildRight),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using ShuffledHashJoin (build=left)") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeShuffledHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, BuildLeft),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using ShuffledHashJoin (build=right)") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeShuffledHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, BuildRight),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using SortMergeJoin") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeSortMergeJoin(leftKeys, rightKeys, boundCondition, leftPlan, rightPlan),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

  }

  {
    def df: DataFrame = spark.range(3).selectExpr(
      "struct(id as id1, id as id2) as key", "id as value")
    lazy val left = df.selectExpr("key", "concat('L', value) as value").alias("left")
    lazy val right = df.selectExpr("key", "concat('R', value) as value").alias("right")
    testInnerJoin(
      "SPARK-15822 - test structs as keys - struct element with different name",
      left,
      right,
      () => (left.col("key") === right.col("key")).expr,
      Seq(
        (Row(0, 0), "L0", Row(0, 0), "R0"),
        (Row(1, 1), "L1", Row(1, 1), "R1"),
        (Row(2, 2), "L2", Row(2, 2), "R2")))
  }
}
