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
package org.apache.gluten.extension

import org.apache.gluten.GlutenConfig
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EulerNumber, Expression, Literal, MakeYMInterval, Pi, Rand, SparkPartitionID, SparkVersion, Uuid}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Sum}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, RDDScanExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.WriteFilesExec
import org.apache.spark.sql.types.StringType

/** Rules to make Velox backend work correctly with query plans that have empty output schemas. */
object EmptySchemaWorkaround {
  /**
   * This rule plans [[RDDScanExec]] with a fake schema to make gluten work, because gluten does not
   * support empty output relation, see [[FallbackEmptySchemaRelation]].
   */
  case class PlanOneRowRelation(spark: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      if (!GlutenConfig.getConf.enableOneRowRelationColumnar) {
        return plan
      }

      plan.transform {
        // We should make sure the output does not change, e.g.
        // Window
        //   OneRowRelation
        case u: UnaryExecNode
            if u.child.isInstanceOf[RDDScanExec] &&
              u.child.asInstanceOf[RDDScanExec].name == "OneRowRelation" &&
              u.outputSet != u.child.outputSet =>
          val rdd = spark.sparkContext.parallelize(InternalRow(null) :: Nil, 1)
          val attr = AttributeReference("fake_column", StringType)()
          u.withNewChildren(RDDScanExec(attr :: Nil, rdd, "OneRowRelation") :: Nil)
      }
    }
  }

  /**
   * FIXME To be removed: Since Velox backend is the only one to use the strategy, and we already
   * support offloading zero-column batch in ColumnarBatchInIterator via PR #3309.
   *
   * We'd make sure all Velox operators be able to handle zero-column input correctly then remove
   * the rule together with [[PlanOneRowRelation]].
   */
  case class FallbackEmptySchemaRelation() extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
      case p =>
        if (fallbackOnEmptySchema(p)) {
          if (p.children.exists(_.output.isEmpty)) {
            // Some backends are not eligible to offload plan with zero-column input.
            // If any child have empty output, mark the plan and that child as UNSUPPORTED.
            FallbackTags.add(p, "at least one of its children has empty output")
            p.children.foreach {
              child =>
                if (child.output.isEmpty && !child.isInstanceOf[WriteFilesExec]) {
                  FallbackTags.add(child, "at least one of its children has empty output")
                }
            }
          }
        }
        p
    }

    private def fallbackOnEmptySchema(plan: SparkPlan): Boolean = {
      // Count(1) and Sum(1) are special cases that Velox backend can handle.
      // Do not fallback it and its children in the first place.
      !mayNeedOffload(plan)
    }

    /**
     * Check whether a plan needs to be offloaded even though they have empty input schema, e.g,
     * Sum(1), Count(1), rand(), etc.
     * @param plan:
     *   The Spark plan to check.
     */
    private def mayNeedOffload(plan: SparkPlan): Boolean = {
      def checkExpr(expr: Expression): Boolean = {
        expr match {
          // Block directly falling back the below functions by FallbackEmptySchemaRelation.
          case alias: Alias => checkExpr(alias.child)
          case _: Rand | _: Uuid | _: MakeYMInterval | _: SparkPartitionID | _: EulerNumber |
              _: Pi | _: SparkVersion =>
            true
          case _ => false
        }
      }

      plan match {
        case exec: HashAggregateExec if exec.aggregateExpressions.nonEmpty =>
          // Check Sum(Literal) or Count(Literal).
          exec.aggregateExpressions.forall(
            expression => {
              val aggFunction = expression.aggregateFunction
              aggFunction match {
                case Sum(Literal(_, _), _) => true
                case Count(Seq(Literal(_, _))) => true
                case _ => false
              }
            })
        case p: ProjectExec if p.projectList.nonEmpty =>
          p.projectList.forall(checkExpr(_))
        case _ =>
          false
      }
    }
  }
}
