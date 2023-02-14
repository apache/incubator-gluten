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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql._
import org.apache.spark.sql.sources.{And, EqualTo, Or}

class GlutenFileSourceStrategySuite extends FileSourceStrategySuite
  with GlutenSQLTestsBaseTrait {

  test(GlutenTestConstants.GLUTEN_TEST +
    "SPARK-32352: Partially push down support data filter if it mixed in partition filters") {
    val table =
      createTable(
        files = Seq(
          "p1=1/file1" -> 10,
          "p1=2/file2" -> 10,
          "p1=3/file3" -> 10,
          "p1=4/file4" -> 10))

    checkScan(table.where("(c1 = 1) OR (c1 = 2)")) { partitions =>
      assert(partitions.size == 1, "when checking partitions")
    }
    checkDataFilters(Set(Or(EqualTo("c1", 1), EqualTo("c1", 2))))

    checkScan(table.where("(p1 = 1 AND c1 = 1) OR (p1 = 2 and c1 = 2)")) { partitions =>
      assert(partitions.size == 1, "when checking partitions")
    }
    // Gluten aims to push down all the conditions in Filter into Scan.
    checkDataFilters(Set(Or(EqualTo("c1", 1), EqualTo("c1", 2)),
      Or(And(EqualTo("p1", 1), EqualTo("c1", 1)), And(EqualTo("p1", 2), EqualTo("c1", 2)))))

    checkScan(table.where("(p1 = '1' AND c1 = 2) OR (c1 = 1 OR p1 = '2')")) { partitions =>
      assert(partitions.size == 1, "when checking partitions")
    }
    checkDataFilters(Set.empty)

    checkScan(table.where("p1 = '1' OR (p1 = '2' AND c1 = 1)")) { partitions =>
      assert(partitions.size == 1, "when checking partitions")
    }
    // Gluten aims to push down all the conditions in Filter into Scan.
    checkDataFilters(Set(Or(EqualTo("p1", 1), And(EqualTo("p1", 2), EqualTo("c1", 1)))))
  }
}
