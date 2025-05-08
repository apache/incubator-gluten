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

import org.apache.spark.sql.internal.SQLConf

class GlutenInjectRuntimeFilterSuite extends InjectRuntimeFilterSuite with GlutenSQLTestsBaseTrait {

  test("gluten Runtime bloom filter join: two joins") {
    withSQLConf(
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
//      // bf2 as creation side and inject runtime filter for bf1 and bf3.
//      assertRewroteWithBloomFilter("select * from bf1 join bf2 join bf3 on bf1.c1 = bf2.c2 " +
//        "and bf3.c3 = bf2.c2 where bf2.a2 = 5", 2)
//      logWarning("line 1")
//      assertRewroteWithBloomFilter("select * from bf1 left outer join bf2 join bf3 on " +
//        "bf1.c1 = bf2.c2 and bf3.c3 = bf2.c2 where bf2.a2 = 5", 2)
//      logWarning("line 2")
//      assertRewroteWithBloomFilter("select * from bf1 right outer join bf2 join bf3 on " +
//        "bf1.c1 = bf2.c2 and bf3.c3 = bf2.c2 where bf2.a2 = 5", 2)
//      logWarning("line 3")
      // bf1 and bf2 hasn't shuffle. bf1 as creation side and inject runtime filter for bf3.
//      assertRewroteWithBloomFilter(
//        "select * from (select * from bf1 left semi join bf2 on " +
//          "bf1.c1 = bf2.c2 where bf1.a1 = 5) as a join bf3 on bf3.c3 = a.c1")
//      logWarning("line 4")
      assertRewroteWithBloomFilter(
        "select * from (select * from bf1 left anti join bf2 on " +
          "bf1.c1 = bf2.c2 where bf1.a1 = 5) as a join bf3 on bf3.c3 = a.c1")
      logWarning("line 5")
//      // bf1 as creation side and inject runtime filter for bf2 and bf3.
//      assertRewroteWithBloomFilter("select * from bf1 join bf2 join bf3 on bf1.c1 = bf2.c2 " +
//        "and bf3.c3 = bf1.c1 where bf1.a1 = 5", 2)
//      logWarning("line 6")
//      assertRewroteWithBloomFilter("select * from bf1 left outer join bf2 join bf3 on " +
//        "bf1.c1 = bf2.c2 and bf3.c3 = bf1.c1 where bf1.a1 = 5", 2)
//      logWarning("line 7")
//      assertRewroteWithBloomFilter("select * from bf1 right outer join bf2 join bf3 on " +
//        "bf1.c1 = bf2.c2 and bf3.c3 = bf1.c1 where bf1.a1 = 5", 2)
//      logWarning("line 8")
//      // bf2 as creation side and inject runtime filter for bf1 and bf3(join keys are transitive).
//      assertRewroteWithBloomFilter("select * from (select * from bf1 join bf2 on " +
//        "bf1.c1 = bf2.c2 where bf2.a2 = 5) as a join bf3 on bf3.c3 = a.c1", 2)
//      logWarning("line 9")
//      assertRewroteWithBloomFilter("select * from (select * from bf1 left join bf2 on " +
//        "bf1.c1 = bf2.c2 where bf2.a2 = 5) as a join bf3 on bf3.c3 = a.c1", 2)
//      logWarning("line 10")
//      assertRewroteWithBloomFilter("select * from (select * from bf1 right join bf2 on " +
//        "bf1.c1 = bf2.c2 where bf2.a2 = 5) as a join bf3 on bf3.c3 = a.c1", 2)
//      logWarning("line 11")
//      assertRewroteWithBloomFilter("select * from bf1 join bf2 join bf3 on bf1.c1 = bf2.c2 " +
//        "and bf3.c3 = bf1.c1 where bf2.a2 = 5", 2)
//      logWarning("line 12")
//      assertRewroteWithBloomFilter("select * from bf1 left outer join bf2 join bf3 on " +
//        "bf1.c1 = bf2.c2 and bf3.c3 = bf1.c1 where bf2.a2 = 5", 2)
//      logWarning("line 13")
//      assertRewroteWithBloomFilter("select * from bf1 right outer join bf2 join bf3 on " +
//        "bf1.c1 = bf2.c2 and bf3.c3 = bf1.c1 where bf2.a2 = 5", 2)
//      logWarning("line 14")
    }

//    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
//      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1200") {
//      // bf1 as creation side and inject runtime filter for bf2 and bf3.
//      assertRewroteWithBloomFilter("select * from (select * from bf1 left semi join bf2 on " +
//        "bf1.c1 = bf2.c2 where bf1.a1 = 5) as a join bf3 on bf3.c3 = a.c1", 2)
//      // left anti join unsupported. bf1 as creation side and inject runtime filter for bf3.
//      assertRewroteWithBloomFilter("select * from (select * from bf1 left anti join bf2 on " +
//        "bf1.c1 = bf2.c2 where bf1.a1 = 5) as a join bf3 on bf3.c3 = a.c1")
//      // bf2 as creation side and inject runtime filter for bf1 and bf3(by passing key).
//      assertRewroteWithBloomFilter("select * from (select * from bf1 left semi join bf2 on " +
//        "(bf1.c1 = bf2.c2 and bf2.a2 = 5)) as a join bf3 on bf3.c3 = a.c1", 2)
//      // left anti join unsupported.
//      // bf2 as creation side and inject runtime filter for bf3(by passing key).
//      assertDidNotRewriteWithBloomFilter("select * from (select * from bf1 left anti join bf2 " +
//        "on (bf1.c1 = bf2.c2 and bf2.a2 = 5)) as a join bf3 on bf3.c3 = a.c1")
//      // left anti join unsupported and hasn't selective filter.
//      assertRewroteWithBloomFilter("select * from (select * from bf1 left anti join bf2 on " +
//        "(bf1.c1 = bf2.c2 and bf1.a1 = 5)) as a join bf3 on bf3.c3 = a.c1", 0)
//    }
  }

}
