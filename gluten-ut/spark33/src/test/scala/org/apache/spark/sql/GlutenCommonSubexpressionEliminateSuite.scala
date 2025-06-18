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

    withSQLConf(("spark.gluten.sql.commonSubexpressionEliminate", "true")) {
      // CSE in project
      val df = spark.sql("select hash(id), hash(id)+1, hash(id)-1 from range(3)")

      checkAnswer(
        df,
        Seq(
          Row(-1670924195, -1670924194, -1670924196),
          Row(-1712319331, -1712319330, -1712319332),
          Row(-797927272, -797927271, -797927273)))

      checkOperatorCount[ProjectExecTransformer](2)(df)
    }
  }
}
