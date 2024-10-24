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
package org.apache.gluten.extension.injector

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable

class InjectorControl private[injector] () {
  import InjectorControl._
  private val disablerBuffer: mutable.ListBuffer[Disabler] =
    mutable.ListBuffer()

  def disableOn(disabler: Disabler): Unit = synchronized {
    disablerBuffer += disabler
  }

  private[injector] def disabler(): Disabler = synchronized {
    session =>
      {
        disablerBuffer.exists(_.disabled(session))
      }
  }
}

object InjectorControl {
  trait Disabler {
    // If true, the injected rule will be disabled.
    def disabled(session: SparkSession): Boolean
  }

  object Disabler {
    implicit class DisablerOps(disabler: Disabler) {
      def wrapRule[TreeType <: TreeNode[_]](
          ruleBuilder: SparkSession => Rule[TreeType]): SparkSession => Rule[TreeType] = session =>
        {
          val rule = ruleBuilder(session)
          new Rule[TreeType] {
            override val ruleName: String = rule.ruleName
            override def apply(plan: TreeType): TreeType = {
              if (disabler.disabled(session)) {
                return plan
              }
              rule(plan)
            }
          }
        }

      def wrapStrategy(strategyBuilder: StrategyBuilder): StrategyBuilder = session => {
        val strategy = strategyBuilder(session)
        new Strategy {
          override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
            if (disabler.disabled(session)) {
              return Nil
            }
            strategy(plan)
          }
        }
      }

      def wrapParser(parserBuilder: ParserBuilder): ParserBuilder = (session, parser) => {
        val before = parser
        val after = parserBuilder(session, before)
        new ParserInterface {
          override def parsePlan(sqlText: String): LogicalPlan = {
            if (disabler.disabled(session)) {
              return before.parsePlan(sqlText)
            }
            after.parsePlan(sqlText)
          }
          override def parseExpression(sqlText: String): Expression = {
            if (disabler.disabled(session)) {
              return before.parseExpression(sqlText)
            }
            after.parseExpression(sqlText)
          }
          override def parseTableIdentifier(sqlText: String): TableIdentifier = {
            if (disabler.disabled(session)) {
              return before.parseTableIdentifier(sqlText)
            }
            after.parseTableIdentifier(sqlText)
          }
          override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
            if (disabler.disabled(session)) {
              return before.parseFunctionIdentifier(sqlText)
            }
            after.parseFunctionIdentifier(sqlText)
          }
          override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
            if (disabler.disabled(session)) {
              return before.parseMultipartIdentifier(sqlText)
            }
            after.parseMultipartIdentifier(sqlText)
          }
          override def parseQuery(sqlText: String): LogicalPlan = {
            if (disabler.disabled(session)) {
              return before.parseQuery(sqlText)
            }
            after.parseQuery(sqlText)
          }
          override def parseTableSchema(sqlText: String): StructType = {
            if (disabler.disabled(session)) {
              return before.parseTableSchema(sqlText)
            }
            after.parseTableSchema(sqlText)
          }
          override def parseDataType(sqlText: String): DataType = {
            if (disabler.disabled(session)) {
              return before.parseDataType(sqlText)
            }
            after.parseDataType(sqlText)
          }
        }
      }

      def wrapFunction(functionDescription: FunctionDescription): FunctionDescription = {
        val (identifier, info, builder) = functionDescription
        val wrappedBuilder: FunctionBuilder = children => {
          if (
            disabler.disabled(SparkSession.getActiveSession.getOrElse(
              throw new IllegalStateException("Active Spark session not found")))
          ) {
            throw new UnsupportedOperationException(
              s"Function ${info.getName} is not callable as Gluten is disabled")
          }
          builder(children)
        }
        (identifier, info, wrappedBuilder)
      }

      def wrapColumnarRule(columnarRuleBuilder: ColumnarRuleBuilder): ColumnarRuleBuilder =
        session => {
          val columnarRule = columnarRuleBuilder(session)
          new ColumnarRule {
            override val preColumnarTransitions: Rule[SparkPlan] = {
              new Rule[SparkPlan] {
                override def apply(plan: SparkPlan): SparkPlan = {
                  if (disabler.disabled(session)) {
                    return plan
                  }
                  columnarRule.preColumnarTransitions.apply(plan)
                }
              }
            }

            override val postColumnarTransitions: Rule[SparkPlan] = {
              new Rule[SparkPlan] {
                override def apply(plan: SparkPlan): SparkPlan = {
                  if (disabler.disabled(session)) {
                    return plan
                  }
                  columnarRule.postColumnarTransitions.apply(plan)
                }
              }
            }
          }
        }
    }
  }
}
