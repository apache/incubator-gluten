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

  test("scan") {
//    val queryString = resourceToString(s"tpch/${tpchQueries(5)}.sql",
//      classLoader = Thread.currentThread().getContextClassLoader)
//    logInfo(queryString)
    assertSqlSubstraitRelRoundTrip("select * from lineitem")
  }

  test("simpleTest") {
    // val query = "select p_size  from part where p_partkey > cast(100 as bigint)"
    val query = "select p_size from part where p_partkey is not null"
    assertSqlSubstraitRelRoundTrip(query)
  }
}
