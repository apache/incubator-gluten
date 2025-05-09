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
package org.apache.gluten.execution

import org.apache.gluten.memory.memtarget.DynamicOffHeapSizingMemoryTarget
import org.apache.gluten.tags.SkipTest

import org.apache.spark.SparkConf

@SkipTest
class DynamicOffHeapSizingSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.executor.memory", "2GB")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction", "0.95")
      .set("spark.gluten.memory.dynamic.offHeap.sizing.enabled", "true")
  }

  test("Dynamic off-heap sizing") {
    val query =
      """
        | select l_quantity, c_acctbal, o_orderdate, p_type, n_name, s_suppkey
        | from customer, orders, lineitem, part, supplier, nation
        | where c_custkey = o_custkey and o_orderkey = l_orderkey and l_partkey = p_partkey
        | and l_suppkey = s_suppkey and s_nationkey = n_nationkey
        | order by c_acctbal desc, o_orderdate, l_suppkey, n_name, p_type
        | limit 1000
      """.stripMargin
    var allocatedSize = 0
    assert(allocatedSize >= 0)
    for (i <- 0 until 50) {
      // scalastyle:off println
      println(
        s"Total memory: ${Runtime.getRuntime().totalMemory()} bytes,"
          + s" free memory: ${Runtime.getRuntime().freeMemory()} bytes,"
          + s" max memory: ${Runtime.getRuntime().maxMemory()} bytes"
      )
      // scalastyle:on println
      val size = 1024 * 1024 * 1024 - 51
      val data = new Array[Byte](size + i)
      allocatedSize += data.length
      runQueryAndCompare(query) { _ => }
    }
    assert(DynamicOffHeapSizingMemoryTarget.getTotalExplicitGCCount() > 0)
  }
}
