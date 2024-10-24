package org.apache.gluten.extension

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

package object injector {
  type RuleBuilder = SparkSession => Rule[LogicalPlan]
  type StrategyBuilder = SparkSession => Strategy
  type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)
  type QueryStagePrepRuleBuilder = SparkSession => Rule[SparkPlan]
  type ColumnarRuleBuilder = SparkSession => ColumnarRule
}
