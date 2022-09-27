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

package org.apache.spark.sql.catalyst.expressions

import io.glutenproject.GlutenConfig
import org.apache.spark.sql.{GlutenTestConstants, GlutenTestsTrait}
import org.apache.spark.sql.types.DataType

class GlutenNullExpressionsSuite extends NullExpressionsSuite with GlutenTestsTrait {

  override def blackTestNameList: Seq[String] = {
    if (GlutenConfig.getSessionConf.isClickHouseBackend) {
      Seq(GlutenTestConstants.IGNORE_ALL)
    } else Seq()
  }

  test("gluten isnull and isnotnull") {
    testAllTypes { (value: Any, tpe: DataType) =>
      checkEvaluation(IsNull(Literal.create(value, tpe)), false)
      checkEvaluation(IsNotNull(Literal.create(value, tpe)), true)
      checkEvaluation(IsNull(Literal.create(null, tpe)), true)
      checkEvaluation(IsNotNull(Literal.create(null, tpe)), false)
    }
  }
}
