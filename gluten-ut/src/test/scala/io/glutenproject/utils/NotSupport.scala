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

package io.glutenproject.utils

import io.glutenproject.GlutenConfig
import io.glutenproject.utils.clickhouse.ClickHouseNotSupport
import io.glutenproject.utils.velox.VeloxNotSupport
import org.apache.spark.sql.GlutenTestConstants
import org.apache.spark.sql.catalyst.analysis.FunctionRegistryBase
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullExpressionsSuite}

import scala.reflect.ClassTag

abstract class NotSupport {
  protected def notSupportSuiteList: Map[String, Map[String, ExpressionInfo]]
  protected def fullSupportSuiteList: Set[String]

  protected def notSupport[T <: Expression: ClassTag](
      caseMethodName: String,
      expressionName: String): (String, ExpressionInfo) = {
    (caseMethodName, FunctionRegistryBase.expressionInfo(expressionName, None))
  }

  protected def simpleClassName[T: ClassTag](implicit ct: ClassTag[T]) : String =
    ct.runtimeClass.getSimpleName

  def NotYetSupportCase(suiteName: String): Option[Seq[String]] = {
    if (fullSupportSuiteList.contains(suiteName)) {
      Some(Seq.empty)
    } else {
      notSupportSuiteList.get(suiteName).map(_.keys.toSeq)
    }
  }
}

object NotSupport {
  def NotYetSupportCase(suiteName: String): Seq[String] = {
    val result =
      if (SystemParameters.getGlutenBackend.equalsIgnoreCase(GlutenConfig.GLUTEN_VELOX_BACKEND)) {
        VeloxNotSupport.NotYetSupportCase(suiteName)
    } else {
        ClickHouseNotSupport.NotYetSupportCase(suiteName)
    }
    result.getOrElse(Seq(GlutenTestConstants.IGNORE_ALL))
  }
}
