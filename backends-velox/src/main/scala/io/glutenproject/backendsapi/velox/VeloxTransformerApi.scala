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

package io.glutenproject.backendsapi.velox

import io.glutenproject.backendsapi.{BackendsApiManager, ITransformerApi}
import io.glutenproject.expression.VeloxArrowUtils
import io.glutenproject.utils.{InputPartitionsUtil, VeloxExpressionUtil}
import io.glutenproject.utils.VeloxExpressionUtil.VELOX_EXPR_BLACKLIST
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Expression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, PartitionDirectory}

class VeloxTransformerApi extends ITransformerApi with Logging {

  /**
   * Add the validation for Velox unsupported or mismatched expressions with specific input type,
   * such as Cast(ArrayType).
   */
  def doValidate(blacklist: Map[String, Set[String]], expr: Expression): Boolean = {
    // To handle cast(struct as string) AS col_name expression
    val key = if (expr.prettyName.toLowerCase().equals("alias")) {
      expr.asInstanceOf[Alias].child.prettyName.toLowerCase()
    } else expr.prettyName.toLowerCase()
    val value = blacklist.get(key)
    if (value.isEmpty) {
      return true
    }
    val inputTypeNames = value.get
    inputTypeNames.foreach { inputTypeName =>
      if (inputTypeName.equals(VeloxExpressionUtil.EMPTY_TYPE)) {
        return false
      } else {
        for (input <- expr.children ++ Seq(expr) ) {
          if (inputTypeName.equals(input.dataType.typeName)) {
            return false
          }
        }
      }
    }
    true
  }

  /**
   * Do validate the expressions based on the specific backend blacklist,
   * the existed expression will fall back to Vanilla Spark.
   */
  override def doValidate(expr: Expression): Boolean = doValidate(VELOX_EXPR_BLACKLIST, expr)

  /**
   * Do validate for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def validateColumnarShuffleExchangeExec(outputPartitioning: Partitioning,
                                                   outputAttributes: Seq[Attribute]): Boolean = {
    // check input datatype
    for (attr <- outputAttributes) {
      try VeloxArrowUtils.createArrowField(attr)
      catch {
        case e: UnsupportedOperationException =>
          logInfo(s"${attr.dataType} is not supported in VeloxColumnarShuffledExchangeExec.")
          return false
      }
    }
    true
  }

  /**
   * Used for table scan validation.
   *
   * @return true if backend supports reading the file format.
   */
  override def supportsReadFileFormat(fileFormat: FileFormat): Boolean = {
    BackendsApiManager.getSettings.supportFileFormatRead(fileFormat)
  }

  /**
   * Generate Seq[InputPartition] for FileSourceScanExecTransformer.
   */
  def genInputPartitionSeq(relation: HadoopFsRelation,
                           selectedPartitions: Array[PartitionDirectory]): Seq[InputPartition] = {
    InputPartitionsUtil.genInputPartitionSeq(relation, selectedPartitions)
  }
}
