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

package io.glutenproject.execution

import org.apache.spark.SparkConf

class GlutenClickHouseDSV2Suite extends GlutenClickHouseTPCHAbstractSuite {

  /**
    * Run Gluten + ClickHouse Backend with ColumnarShuffleManager
    */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "true")
  }

  test("TPCH Q1") {
    runTPCHQuery(1, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q2") {
    runTPCHQuery(2, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q3") {
    runTPCHQuery(3, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q4") {
    runTPCHQuery(4, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q5") {
    runTPCHQuery(5, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q6") {
    runTPCHQuery(6, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q7") {
    runTPCHQuery(7, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q8") {
    runTPCHQuery(8, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q9") {
    runTPCHQuery(9, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q10") {
    runTPCHQuery(10, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q11") {
    runTPCHQuery(11, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q12") {
    runTPCHQuery(12, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q13") {
    runTPCHQuery(13, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q14") {
    runTPCHQuery(14, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q15") {
    runTPCHQuery(15, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q16") {
    runTPCHQuery(16, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q17") {
    runTPCHQuery(17, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q18") {
    runTPCHQuery(18, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q19") {
    runTPCHQuery(19, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q20") {
    runTPCHQuery(20, chTpchQueries, queriesResults) { df =>
    }
  }

  test("TPCH Q22") {
    runTPCHQuery(22, chTpchQueries, queriesResults) { df =>
    }
  }
}
