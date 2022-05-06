/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.backendsapi.velox

import scala.collection.JavaConverters._

import io.glutenproject.backendsapi.ITransformerApi
import io.glutenproject.GlutenConfig
import io.glutenproject.expression.ArrowConverterUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}

class VeloxTransformerApi extends ITransformerApi with Logging {

  /**
   * Do validate for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def validateColumnarShuffleExchangeExec(outputPartitioning: Partitioning,
                                                   outputAttributes: Seq[Attribute]
                                                  ): Boolean = {
    // check input datatype
    for (attr <- outputAttributes) {
      try ArrowConverterUtils.createArrowField(attr) catch {
        case e: UnsupportedOperationException =>
          logInfo(s"${attr.dataType} is not supported in VeloxColumnarShuffledExchangeExec.")
          return false
      }
    }
    outputPartitioning match {
      case HashPartitioning(exprs, n) =>
        exprs.foreach(expr => {
          if (!expr.isInstanceOf[Attribute]) {
            logInfo("Expressions are not supported in HashPartitioning.")
            return false
          }})
      case _ =>
    }
    true
  }

  /**
   * Get the backend api name.
   *
   * @return
   */
  override def getBackendName: String = GlutenConfig.GLUTEN_VELOX_BACKEND
}
