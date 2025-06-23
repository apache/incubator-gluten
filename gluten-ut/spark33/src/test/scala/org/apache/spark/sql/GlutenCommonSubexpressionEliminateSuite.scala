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

import org.apache.gluten.execution.{ProjectExecTransformer, TransformSupport}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.Aggregate

import scala.reflect.ClassTag

class GlutenCommonSubexpressionEliminateSuite extends GlutenSQLTestsTrait {

  testGluten("test common subexpression eliminate") {
    def checkOperatorCount[T <: TransformSupport](count: Int)(df: DataFrame)(implicit
        tag: ClassTag[T]): Unit = {
      assert(
        getExecutedPlan(df).count(
          plan => {
            plan.getClass == tag.runtimeClass
          }) == count,
        s"executed plan: ${getExecutedPlan(df)}")
    }

    withSQLConf(("spark.gluten.sql.commonSubexpressionEliminate", "true"),
      ("spark.sql.adaptive.enabled", "false")) {
      // CSE in project
      val dfProject = spark.sql("select hash(id), hash(id)+1, hash(id)-1 from range(3)")

      checkAnswer(
        dfProject,
        Seq(
          Row(-1670924195, -1670924194, -1670924196),
          Row(-1712319331, -1712319330, -1712319332),
          Row(-797927272, -797927271, -797927273)))

      checkOperatorCount[ProjectExecTransformer](2)(dfProject)

      // CSE in window
      val dfWindow = spark.sql("SELECT id, AVG(id) OVER (PARTITION BY id % 2 ORDER BY id) as avg_id, " +
        "SUM(id) OVER (PARTITION BY id % 2 ORDER BY id) as sum_id FROM range(3)")

      checkAnswer(
        dfWindow,
        Seq(
          Row(0, 0.0,0),
          Row(2, 1.0, 2),
          Row(1, 1.0, 1)))

      checkOperatorCount[ProjectExecTransformer](4)(dfWindow)

      // CSE in aggregate
      var dfAggregate = spark.sql("select id % 2, max(hash(id)), min(hash(id)) " +
        "from range(3) group by id % 2")

      checkAnswer(
        dfAggregate,
        Seq(
          Row(0, -797927272, -1670924195),
          Row(1, -1712319331, -1712319331)))

      checkOperatorCount[ProjectExecTransformer](2)(dfAggregate)

      dfAggregate = spark.sql("select id % 10, sum(id +100) + max(id+100) from range(100) group by id % 10")

      checkAnswer(
        dfAggregate,
        Seq(
          Row(0, 1640),
          Row(7, 1717),
          Row(6, 1706),
          Row(9, 1739),
          Row(5, 1695),
          Row(1, 1651),
          Row(3, 1673),
          Row(8, 1728),
          Row(2, 1662),
          Row(4, 1684)))

      checkOperatorCount[ProjectExecTransformer](3)(dfAggregate)

      // issue https://github.com/oap-project/gluten/issues/4642
      dfAggregate = spark.sql("select id, if(id % 2 = 0, sum(id), max(id)) as s1, " +
        "if(id %2 = 0, sum(id+1), sum(id+2)) as s2 from range(3) group by id")

      checkAnswer(
        dfAggregate,
        Seq(
          Row(0, 0, 1),
          Row(1, 1, 3),
          Row(2, 2, 3)))

      checkOperatorCount[ProjectExecTransformer](2)(dfAggregate)

      // CSE in sort
     var dfSort = spark.sql("select id from range(3) " +
        "order by hash(id%10), hash(hash(id%10))")

      checkAnswer(
        dfSort,
        Seq(
          Row(1),
          Row(0),
          Row(2)))

      checkOperatorCount[ProjectExecTransformer](3)(dfSort)

      dfSort = spark.sql(s"""
                            |SELECT 'test' AS test
                            |  , Sum(CASE
                            |    WHEN name = '2' THEN 0
                            |      ELSE id
                            |    END) AS c1
                            |  , Sum(CASE
                            |    WHEN name = '2' THEN id
                            |      ELSE 0
                            |    END) AS c2
                            | , CASE WHEN name = '2' THEN Sum(id) ELSE 0
                            |   END AS c3
                            |FROM (select id, cast(id as string) name from range(3))
                            |GROUP BY name
                            |""".stripMargin)

      checkAnswer(
        dfSort,
        Seq(
          Row("test", 0, 0, 0),
          Row("test", 1, 0, 0),
          Row("test", 0, 2, 2)))

      checkOperatorCount[ProjectExecTransformer](5)(dfSort)

      val df = spark.sql("select id % 2, max(hash(id)), min(hash(id)) from range(10) group by id % 2")
      df.queryExecution.optimizedPlan.collect {
        case Aggregate(_, aggregateExpressions, _) =>
          val result =
            aggregateExpressions
              .map(a => a.asInstanceOf[Alias].child)
              .filter(_.isInstanceOf[AggregateExpression])
              .map(expr => expr.asInstanceOf[AggregateExpression].aggregateFunction)
              .filter(aggFunc => aggFunc.children.head.isInstanceOf[AttributeReference])
              .map(aggFunc => aggFunc.children.head.asInstanceOf[AttributeReference].name)
              .distinct
          assertResult(1)(result.size)
      }

      checkOperatorCount[ProjectExecTransformer](2)(df)
    }
  }
}
