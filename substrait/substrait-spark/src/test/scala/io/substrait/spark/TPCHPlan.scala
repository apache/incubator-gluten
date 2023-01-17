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
package io.substrait.spark

import org.apache.spark.sql.TPCHBase

class TPCHPlan extends TPCHBase with SubstraitPlanTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel("WARN")
  }

  tpchQueries.foreach {
    q =>
      test(s"check simplified (tpch/$q)") {
        testQuery("tpch", q)
      }
  }

  test("Decimal") {
    assertSqlSubstraitRelRoundTrip("select l_returnflag," +
      " sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) from lineitem group by l_returnflag")
  }

  test("simpleJoin") {
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem " +
        "join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ")
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem " +
        "left join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ")
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem " +
        "right join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ")
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem " +
        "full join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ")
  }

  test("simpleOrderByClause") {
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey from lineitem where l_shipdate < date '1998-01-01' " +
        "order by l_shipdate, l_discount")
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey from lineitem where l_shipdate < date '1998-01-01' " +
        "order by l_shipdate asc, l_discount desc")
//    assertSqlSubstraitRelRoundTrip(
//      "select l_partkey from lineitem where l_shipdate < date '1998-01-01' " +
//        "order by l_shipdate asc, l_discount desc limit 100 offset 1000")
//    assertSqlSubstraitRelRoundTrip(
//      "select l_partkey from lineitem where l_shipdate < date '1998-01-01' " +
//        "order by l_shipdate asc, l_discount desc limit 100")
//    assertSqlSubstraitRelRoundTrip(
//      "select l_partkey from lineitem where l_shipdate < date '1998-01-01' limit 100")
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey from lineitem where l_shipdate < date '1998-01-01' " +
        "order by l_shipdate asc, l_discount desc nulls first")
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey from lineitem where l_shipdate < date '1998-01-01' " +
        "order by l_shipdate asc, l_discount desc nulls last")
  }

  test("simpleTest") {
    val query = "select p_size  from part where p_partkey > cast(100 as bigint)"
    assertSqlSubstraitRelRoundTrip(query)
  }

  test("simpleTest2") {
    val query = "select l_partkey, l_discount from lineitem where l_orderkey > cast(100 as bigint)"
    assertSqlSubstraitRelRoundTrip(query)
  }

  test("simpleTestAgg") {
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey, count(l_tax), COUNT(distinct l_discount) from lineitem group by l_partkey")

    assertSqlSubstraitRelRoundTrip(
      "select count(l_tax), COUNT(distinct l_discount)" +
        " from lineitem group by l_partkey + l_orderkey")

    assertSqlSubstraitRelRoundTrip(
      "select l_partkey + l_orderkey, count(l_tax), COUNT(distinct l_discount)" +
        " from lineitem group by l_partkey + l_orderkey")
  }

  ignore("avg(distinct)") {
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey, sum(l_tax), sum(distinct l_tax)," +
        " avg(l_discount), avg(distinct l_discount) from lineitem group by l_partkey")
  }

  test("simpleTestAgg3") {
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey, sum(l_extendedprice * (1.0-l_discount)) from lineitem group by l_partkey")
  }

  ignore("simpleTestAggFilter") {
    assertSqlSubstraitRelRoundTrip(
      "select sum(l_tax) filter(WHERE l_orderkey > l_partkey) from lineitem")
    // cast is added to avoid the difference by implicit cast
    assertSqlSubstraitRelRoundTrip(
      "select sum(l_tax) filter(WHERE l_orderkey > cast(10.0 as bigint)) from lineitem")
  }

  test("simpleTestAggNoGB") {
    assertSqlSubstraitRelRoundTrip("select count(l_tax), count(distinct l_discount) from lineitem")
  }

  test("simpleTestApproxCountDistinct") {
    val query = "select approx_count_distinct(l_tax)  from lineitem"
    val plan = assertSqlSubstraitRelRoundTrip(query)
  }

  test("simpleTestDateInterval") {
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey+l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' ")
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey+l_orderkey, l_shipdate from lineitem " +
        "where l_shipdate < date '1998-01-01' + interval '3' month ")
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey+l_orderkey, l_shipdate from lineitem " +
        "where l_shipdate < date '1998-01-01' + interval '1' year")
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey+l_orderkey, l_shipdate from lineitem " +
        "where l_shipdate < date '1998-01-01' + interval '1-3' year to month")
  }

  test("simpleTestDecimal") {
    assertSqlSubstraitRelRoundTrip(
      "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0 from lineitem" +
        " where l_shipdate < date '1998-01-01' ")
  }

  ignore("simpleTestGroupingSets [has Expand]") {
    assertSqlSubstraitRelRoundTrip(
      "select sum(l_discount) from lineitem group by grouping sets " +
        "((l_orderkey, L_COMMITDATE), l_shipdate)")

    assertSqlSubstraitRelRoundTrip(
      "select sum(l_discount) from lineitem group by grouping sets " +
        "((l_orderkey, L_COMMITDATE), l_shipdate), l_linestatus")

    assertSqlSubstraitRelRoundTrip(
      "select sum(l_discount) from lineitem group by grouping sets " +
        "((l_orderkey, L_COMMITDATE), l_shipdate, ())")

    assertSqlSubstraitRelRoundTrip(
      "select sum(l_discount) from lineitem group by grouping sets " +
        "((l_orderkey, L_COMMITDATE), l_shipdate, ()), l_linestatus")
    assertSqlSubstraitRelRoundTrip(
      "select sum(l_discount) from lineitem group by grouping sets " +
        "((l_orderkey, L_COMMITDATE), (l_orderkey, L_COMMITDATE, l_linestatus), l_shipdate, ())")
  }

  test("tpch_q1_variant") {
    // difference from tpch_q1 : 1) remove order by clause; 2) remove interval date literal
    assertSqlSubstraitRelRoundTrip(
      "select\n"
        + "  l_returnflag,\n"
        + "  l_linestatus,\n"
        + "  sum(l_quantity) as sum_qty,\n"
        + "  sum(l_extendedprice) as sum_base_price,\n"
        + "  sum(l_extendedprice * (1.0 - l_discount)) as sum_disc_price,\n"
        + "  sum(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) as sum_charge,\n"
        + "  avg(l_quantity) as avg_qty,\n"
        + "  avg(l_extendedprice) as avg_price,\n"
        + "  avg(l_discount) as avg_disc,\n"
        + "  count(*) as count_order\n"
        + "from\n"
        + "  lineitem\n"
        + "where\n"
        + "  l_shipdate <= date '1998-12-01' \n"
        + "group by\n"
        + "  l_returnflag,\n"
        + "  l_linestatus\n")
  }
}
