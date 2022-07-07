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

class GlutenClickHouseColumnarShufflemanagerSuite extends GlutenClickHouseTPCHAbstractSuite {

  /**
    * Run Gluten + ClickHouse Backend with ColumnarShuffleManager
    */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
  }

  ignore("TPCH Q1") {
    runTPCHQuery(1) { df =>
    }
  }

  ignore("TPCH Q2") {
    runTPCHQuery(2) { df =>
    }
  }

  ignore("TPCH Q3") {
    runTPCHQuery(3) { df =>
    }
  }

  ignore("TPCH Q4") {
    runTPCHQuery(4) { df =>
    }
  }

  ignore("TPCH Q5") {
    withSQLConf(
      ("spark.sql.autoBroadcastJoinThreshold", "-1")) {
      runTPCHQuery(5) { df =>
      }
    }
  }

  ignore("TPCH Q6") {
    runTPCHQuery(6) { df =>
    }
  }

  ignore("TPCH Q7") {
    runTPCHQuery(7) { df =>
    }
  }

  ignore("TPCH Q8") {
    runTPCHQuery(8) { df =>
    }
  }

  ignore("TPCH Q9") {
    runTPCHQuery(9) { df =>
    }
  }

  ignore("TPCH Q10") {
    runTPCHQuery(10) { df =>
    }
  }

  ignore("TPCH Q11") {
    runTPCHQuery(11) { df =>
    }
  }

  ignore("TPCH Q12") {
    runTPCHQuery(12) { df =>
    }
  }

  ignore("TPCH Q13") {
    runTPCHQuery(13) { df =>
    }
  }

  ignore("TPCH Q14") {
    runTPCHQuery(14) { df =>
    }
  }

  ignore("TPCH Q15") {
    runTPCHQuery(15) { df =>
    }
  }

  ignore("TPCH Q16") {
    runTPCHQuery(16) { df =>
    }
  }

  ignore("TPCH Q17") {
    runTPCHQuery(17) { df =>
    }
  }

  ignore("TPCH Q18") {
    runTPCHQuery(18) { df =>
    }
  }

  ignore("TPCH Q19") {
    runTPCHQuery(19) { df =>
    }
  }

  ignore("TPCH Q20") {
    runTPCHQuery(20) { df =>
    }
  }

  ignore("TPCH Q22") {
    runTPCHQuery(22) { df =>
    }
  }
}
