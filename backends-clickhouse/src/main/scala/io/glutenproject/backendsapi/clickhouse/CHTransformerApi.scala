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

package io.glutenproject.backendsapi.clickhouse

import io.glutenproject.backendsapi.ITransformerApi
import io.glutenproject.GlutenConfig
import io.glutenproject.expression.{ExpressionConverter, ExpressionTransformer}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.SelectionNode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning}
import org.apache.spark.sql.execution.datasources.FileFormat

class CHTransformerApi extends ITransformerApi with Logging {

  /**
   * Do validate for ColumnarShuffleExchangeExec.
   * For ClickHouse backend, it will return true directly.
   * @return
   */
  override def validateColumnarShuffleExchangeExec(outputPartitioning: Partitioning,
                                                   outputAttributes: Seq[Attribute]
                                                  ) = {
    !outputPartitioning.isInstanceOf[RangePartitioning]

    // check repartition expression
    val substraitContext = new SubstraitContext
    outputPartitioning match {
      case HashPartitioning(exprs, _) =>
        !(exprs.map(expr => {
          val node = ExpressionConverter
            .replaceWithExpressionTransformer(expr, outputAttributes)
            .asInstanceOf[ExpressionTransformer]
            .doTransform(substraitContext.registeredFunction)
          if (!node.isInstanceOf[SelectionNode]) {
            logInfo("Expressions are not supported in HashPartitioning.")
            false
          } else {
            true
          }
        }).exists(_ == false))
      case RangePartitioning(_, _) => false
      case _ => true
    }
  }

  /**
   * Used for table scan validation.
   *
   * @return true if backend supports reading the file format.
   */
  def supportsReadFileFormat(fileFormat: FileFormat): Boolean = true

  /**
   * Get the backend api name.
   *
   * @return
   */
  override def getBackendName: String = GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND
}
