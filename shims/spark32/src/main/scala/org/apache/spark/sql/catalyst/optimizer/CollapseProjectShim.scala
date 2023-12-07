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
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._

object CollapseProjectShim extends AliasHelper {
  def canCollapseExpressions(
      consumers: Seq[NamedExpression],
      producers: Seq[NamedExpression],
      alwaysInline: Boolean): Boolean = {
    canCollapseExpressions(consumers, getAliasMap(producers), alwaysInline)
  }

  def buildCleanedProjectList(
      upper: Seq[NamedExpression],
      lower: Seq[NamedExpression]): Seq[NamedExpression] = {
    val aliases = getAliasMap(lower)
    upper.map(replaceAliasButKeepName(_, aliases))
  }

  /** Check if we can collapse expressions safely. */
  private def canCollapseExpressions(
      consumers: Seq[Expression],
      producerMap: Map[Attribute, Expression],
      alwaysInline: Boolean = false): Boolean = {
    // We can only collapse expressions if all input expressions meet the following criteria:
    // - The input is deterministic.
    // - The input is only consumed once OR the underlying input expression is cheap.
    consumers
      .flatMap(collectReferences)
      .groupBy(identity)
      .mapValues(_.size)
      .forall {
        case (reference, count) =>
          val producer = producerMap.getOrElse(reference, reference)
          producer.deterministic && (count == 1 || alwaysInline || {
            val relatedConsumers = consumers.filter(_.references.contains(reference))
            // It's still exactly-only if there is only one reference in non-extract expressions,
            // as we won't duplicate the expensive CreateStruct-like expressions.
            val extractOnly = relatedConsumers.map(refCountInNonExtract(_, reference)).sum <= 1
            shouldInline(producer, extractOnly)
          })
      }
  }

  /**
   * Return all the references of the given expression without deduplication, which is different
   * from `Expression.references`.
   */
  private def collectReferences(e: Expression): Seq[Attribute] = e.collect {
    case a: Attribute => a
  }

  /** Check if the given expression is cheap that we can inline it. */
  private def shouldInline(e: Expression, extractOnlyConsumer: Boolean): Boolean = e match {
    case _: Attribute | _: OuterReference => true
    case _ if e.foldable => true
    // PythonUDF is handled by the rule ExtractPythonUDFs
    case _: PythonUDF => true
    // Alias and ExtractValue are very cheap.
    case _: Alias | _: ExtractValue => e.children.forall(shouldInline(_, extractOnlyConsumer))
    // These collection create functions are not cheap, but we have optimizer rules that can
    // optimize them out if they are only consumed by ExtractValue, so we need to allow to inline
    // them to avoid perf regression. As an example:
    //   Project(s.a, s.b, Project(create_struct(a, b, c) as s, child))
    // We should collapse these two projects and eventually get Project(a, b, child)
    case _: CreateNamedStruct | _: CreateArray | _: CreateMap | _: UpdateFields =>
      extractOnlyConsumer
    case _ => false
  }

  private def refCountInNonExtract(expr: Expression, ref: Attribute): Int = {
    def refCount(e: Expression): Int = e match {
      case a: Attribute if a.semanticEquals(ref) => 1
      // The first child of `ExtractValue` is the complex type to be extracted.
      case e: ExtractValue if e.children.head.semanticEquals(ref) => 0
      case _ => e.children.map(refCount).sum
    }
    refCount(expr)
  }
}
