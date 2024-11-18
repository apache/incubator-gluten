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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.GeneratorNestedColumnAliasing.canPruneGenerator
import org.apache.spark.sql.catalyst.optimizer.NestedColumnAliasing
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.internal.SQLConf

// ExtendedGeneratorNestedColumnAliasing process Project(Filter(Generate)),
// which is ignored by vanilla spark in optimization rule: ColumnPruning
class ExtendedGeneratorNestedColumnAliasing(spark: SparkSession)
  extends Rule[LogicalPlan]
  with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(AlwaysProcess.fn) {
      case pj @ Project(projectList, f @ Filter(condition, g: Generate))
          if canPruneGenerator(g.generator) &&
            GlutenConfig.getConf.enableExtendedGeneratorNestedColumnAliasing &&
            (SQLConf.get.nestedPruningOnExpressions || SQLConf.get.nestedSchemaPruningEnabled) =>
        val attrToExtractValues = NestedColumnAliasing.getAttributeToExtractValues(
          projectList ++ g.generator.children :+ condition,
          Seq.empty)
        if (attrToExtractValues.isEmpty) {
          pj
        } else {
          val generatorOutputSet = AttributeSet(g.qualifiedGeneratorOutput)
          val (_, attrToExtractValuesNotOnGenerator) =
            attrToExtractValues.partition {
              case (attr, _) =>
                attr.references.subsetOf(generatorOutputSet)
            }

          val pushedThrough = rewritePlanWithAliases(pj, attrToExtractValuesNotOnGenerator)
          pushedThrough
        }
      case p =>
        p
    }

  private def rewritePlanWithAliases(
      plan: LogicalPlan,
      attributeToExtractValues: Map[Attribute, Seq[ExtractValue]]): LogicalPlan = {
    val attributeToExtractValuesAndAliases =
      attributeToExtractValues.map {
        case (attr, evSeq) =>
          val evAliasSeq = evSeq.map {
            ev =>
              val fieldName = ev match {
                case g: GetStructField => g.extractFieldName
                case g: GetArrayStructFields => g.field.name
              }
              ev -> Alias(ev, s"_extract_$fieldName")()
          }

          attr -> evAliasSeq
      }

    val nestedFieldToAlias = attributeToExtractValuesAndAliases.values.flatten.map {
      case (field, alias) => field.canonicalized -> alias
    }.toMap

    // A reference attribute can have multiple aliases for nested fields.
    val attrToAliases =
      AttributeMap(attributeToExtractValuesAndAliases.mapValues(_.map(_._2)).toSeq)

    plan match {
      // Project(Filter(Generate))
      case p @ Project(projectList, child)
          if child
            .isInstanceOf[Filter] && child.asInstanceOf[Filter].child.isInstanceOf[Generate] =>
        val f = child.asInstanceOf[Filter]
        val g = f.child.asInstanceOf[Generate]

        val newProjectList = NestedColumnAliasing.getNewProjectList(projectList, nestedFieldToAlias)
        val newCondition = getNewExpression(f.condition, nestedFieldToAlias)
        val newGenerator = getNewExpression(g.generator, nestedFieldToAlias).asInstanceOf[Generator]

        val tmpG = NestedColumnAliasing
          .replaceWithAliases(g, nestedFieldToAlias, attrToAliases)
          .asInstanceOf[Generate]
        val newG = Generate(
          newGenerator,
          tmpG.unrequiredChildIndex,
          tmpG.outer,
          tmpG.qualifier,
          tmpG.generatorOutput,
          tmpG.children.head)
        val newF = Filter(newCondition, newG)
        val newP = Project(newProjectList, newF)
        newP
      case _ => plan
    }
  }

  private def getNewExpression(
      expr: Expression,
      nestedFieldToAlias: Map[Expression, Alias]): Expression = {
    expr.transform {
      case f: ExtractValue if nestedFieldToAlias.contains(f.canonicalized) =>
        nestedFieldToAlias(f.canonicalized).toAttribute
    }
  }
}
