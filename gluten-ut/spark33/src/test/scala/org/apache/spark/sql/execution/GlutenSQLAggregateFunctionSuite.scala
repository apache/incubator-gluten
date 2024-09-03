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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.HashAggregateExecBaseTransformer

import org.apache.spark.sql.{GlutenSQLTestsTrait, Row}

class GlutenSQLAggregateFunctionSuite extends GlutenSQLTestsTrait {

  testGluten("GLUTEN-4853: The result order is reversed for count and count distinct") {
    val query =
      """
        |select count(distinct if(sex = 'x', id, null)) as uv, count(if(sex = 'x', id, null)) as pv
        |from values (1, 'x'), (1, 'x'), (2, 'y'), (3, 'x'), (3, 'x'), (4, 'y'), (5, 'x')
        |AS tab(id, sex)
        |""".stripMargin
    val df = sql(query)
    checkAnswer(df, Seq(Row(3, 5)))
    assert(getExecutedPlan(df).count(_.isInstanceOf[HashAggregateExecBaseTransformer]) == 4)
  }

  testGluten("GLUTEN-7096: Same names in group by may cause exception") {
    sql("create table if not exists user (day string, rtime int, uid string, owner string)")
    sql("insert into user values ('2024-09-01', 123, 'user1', 'owner1')")
    sql("insert into user values ('2024-09-01', 123, 'user1', 'owner1')")
    sql("insert into user values ('2024-09-02', 567, 'user2', 'owner2')")
    val query =
      """
        |select days, rtime, uid, uid, day1
        |from (
        | select day1 as days, rtime, uid, uid, day1
        | from (
        |   select distinct coalesce(day, "today") as day1, rtime, uid, owner
        |   from user where day = '2024-09-01'
        | )) group by days, rtime, guest_uid, owner_uid, day1
        |""".stripMargin
    val df = sql(query)
    checkAnswer(df, Seq(Row("2024-09-01", 123, "user1", "owner1", "2024-09-01")))
    sql("drop table if exists user")
  }

  testGluten("GLUTEN-7096: Same names with different qualifier in group by may cause exception") {
    sql("create table if not exists user (day string, rtime int, uid string, owner string)")
    sql("insert into user values ('2024-09-01', 123, 'user1', 'owner1')")
    sql("insert into user values ('2024-09-01', 123, 'user1', 'owner1')")
    sql("insert into user values ('2024-09-02', 567, 'user2', 'owner2')")
    val query =
      """
        |select days, rtime, uid, uid, day1
        |from (
        | select day1 as days, rtime, uid, uid, day1
        | from (
        |   select distinct coalesce(day, "today") as day1, rtime, uid, owner
        |   from user where day = '2024-09-01'
        | ) t1 ) t2 group by days, rtime, guest_uid, owner_uid, day1
        |""".stripMargin
    val df = sql(query)
    checkAnswer(df, Seq(Row("2024-09-01", 123, "user1", "owner1", "2024-09-01")))
    sql("drop table if exists user")
  }
}
