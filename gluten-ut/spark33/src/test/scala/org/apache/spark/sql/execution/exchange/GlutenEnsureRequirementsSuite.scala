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
package org.apache.spark.sql.execution.exchange

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.internal.SQLConf

class GlutenEnsureRequirementsSuite extends EnsureRequirementsSuite with GlutenSQLTestsBaseTrait {
  override def sparkConf: SparkConf = {
    // Native SQL configs
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "5")
  }

  testGluten(
    "SPARK-35675: EnsureRequirements remove shuffle should respect PartitioningCollection") {
    import testImplicits._
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "5",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df1 = Seq((1, 2)).toDF("c1", "c2")
      val df2 = Seq((1, 3)).toDF("c3", "c4")
      val res = df1.join(df2, $"c1" === $"c3").repartition($"c1")
      assert(res.queryExecution.executedPlan.collect { case s: ShuffleExchangeLike => s }.size == 2)
    }
  }
}
