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
package io.substrait.spark.logical
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.substrait.SparkTypeUtil

import scala.collection.mutable

object ExpandAdaptor {

  def collectGroupBys(aggregate: Aggregate): Seq[Seq[Expression]] = {
    val expand = aggregate.child.asInstanceOf[Expand]
    val groupByMapping = aggregate.groupingExpressions.map(x => x.canonicalized -> x).toMap
    expand.projections.map(
      projection =>
        expand.output
          .zip(projection)
          .filter(e => !e._2.isInstanceOf[Literal] && groupByMapping.contains(e._1.canonicalized))
          .map(e => groupByMapping(e._1.canonicalized)))
  }

  def collectOutput(expand: Expand): Seq[Expression] = {
    val outputMappings = mutable.HashMap.empty[Expression, Expression]
    expand.projections.foreach {
      proj =>
        expand.output
          .zip(proj)
          .filter(x => !x._2.isInstanceOf[Literal])
          .foreach {
            case (output, project) =>
              if (!outputMappings.contains(output.canonicalized)) {
                val attributeReference = project.asInstanceOf[AttributeReference]
                outputMappings(output.canonicalized) =
                  attributeReference.withNullability(output.nullable)
              }
          }
    }

    expand.output
      .map(o => outputMappings.get(o.canonicalized))
      .filter(_.nonEmpty)
      .map(_.get)
  }

  /*
   * Create new alias for all group by expressions for `Expand` operator.
   */
  private def constructGroupByAlias(groupByExprs: Seq[Expression]): Seq[Alias] = {
    groupByExprs.map {
      case e: NamedExpression => Alias(e, e.name)(qualifier = e.qualifier)
      case other => Alias(other, other.toString)()
    }
  }

  /** Construct [[Expand]] operator with grouping sets. */
  private def constructExpand(
      selectedGroupByExprs: Seq[Seq[Expression]],
      child: LogicalPlan,
      groupByAliases: Seq[Alias],
      gid: Attribute,
      neededOutput: Seq[Attribute]): Expand = {
    // Change the nullability of group by aliases if necessary. For example, if we have
    // GROUPING SETS ((a,b), a), we do not need to change the nullability of a, but we
    // should change the nullability of b to be TRUE.
    // TODO: For Cube/Rollup just set nullability to be `true`.
    val expandedAttributes = groupByAliases.map {
      alias =>
        if (selectedGroupByExprs.exists(!_.contains(alias.child))) {
          alias.child.asInstanceOf[NamedExpression].toAttribute.withNullability(true)
        } else {
          alias.child.asInstanceOf[NamedExpression].toAttribute
        }
    }

    val groupingSetsAttributes = selectedGroupByExprs.map {
      groupingSetExprs =>
        groupingSetExprs.map {
          expr =>
            val alias = groupByAliases
              .find(_.child.semanticEquals(expr))
              .getOrElse(
                //  throw QueryCompilationErrors.selectExprNotInGroupByError(expr, groupByAliases)
                throw new UnsupportedOperationException("groupingSizeTooLargeError")
              )
            // Map alias to expanded attribute.
            expandedAttributes
              .find(_.semanticEquals(alias.child.asInstanceOf[NamedExpression].toAttribute))
              .getOrElse(alias.child.asInstanceOf[NamedExpression].toAttribute)
        }
    }
    createExpand(
      groupingSetsAttributes,
      groupByAliases,
      expandedAttributes,
      gid,
      child,
      neededOutput)
  }

  /** Construct [[Aggregate]] operator from substrait */
  def constructAggregate(
      selectedGroupByExprs: Seq[Seq[Expression]],
      groupByExprs: Seq[Expression],
      aggregationExprs: Seq[NamedExpression],
      child: LogicalPlan,
      allOutput: Seq[NamedExpression]): LogicalPlan = {

    val attrMap = groupByExprs.zipWithIndex.toMap
    val neededOutput = allOutput.filter(e => !attrMap.contains(e)).map(_.toAttribute)

    if (groupByExprs.size > GroupingID.dataType.defaultSize * 8) {
      // throw QueryCompilationErrors.groupingSizeTooLargeError(GroupingID.dataType.defaultSize * 8)
      throw new UnsupportedOperationException("groupingSizeTooLargeError")
    }

    // Expand works by setting grouping expressions to null as determined by the
    // `selectedGroupByExprs`. To prevent these null values from being used in an aggregate
    // instead of the original value we need to create new aliases for all group by expressions
    // that will only be used for the intended purpose.
    val groupByAliases = constructGroupByAlias(groupByExprs)
    val gid =
      AttributeReference(VirtualColumn.groupingIdName, GroupingID.dataType, nullable = false)()
    val expand = constructExpand(selectedGroupByExprs, child, groupByAliases, gid, neededOutput)
    val groupingAttrs = expand.output.drop(neededOutput.length)
    Aggregate(groupingAttrs, groupingAttrs ++: aggregationExprs, expand)
  }

  /**
   * Build bit mask from attributes of selected grouping set. A bit in the bitmask is corresponding
   * to an attribute in group by attributes sequence, the selected attribute has corresponding bit
   * set to 0 and otherwise set to 1. For example, if we have GroupBy attributes (a, b, c, d), the
   * bitmask 5(whose binary form is 0101) represents grouping set (a, c).
   *
   * @param groupingSetAttrs
   *   The attributes of selected grouping set
   * @param attrMap
   *   Mapping group by attributes to its index in attributes sequence
   * @return
   *   The bitmask which represents the selected attributes out of group by attributes.
   */
  private def buildBitmask(groupingSetAttrs: Seq[Attribute], attrMap: Map[Attribute, Int]): Long = {
    val numAttributes = attrMap.size
    assert(numAttributes <= GroupingID.dataType.defaultSize * 8)
    val mask = if (numAttributes != 64) (1L << numAttributes) - 1 else 0xffffffffffffffffL
    // Calculate the attribute masks of selected grouping set. For example, if we have GroupBy
    // attributes (a, b, c, d), grouping set (a, c) will produce the following sequence:
    // (15, 7, 13), whose binary form is (1111, 0111, 1101)
    val masks = mask +: groupingSetAttrs
      .map(attrMap)
      .map(
        index =>
          // 0 means that the column at the given index is a grouping column, 1 means it is not,
          // so we unset the bit in bitmap.
          ~(1L << (numAttributes - 1 - index)))
    // Reduce masks to generate an bitmask for the selected grouping set.
    masks.reduce(_ & _)
  }

  /**
   * Apply the all of the GroupExpressions to every input row, hence we will get multiple output
   * rows for an input row.
   *
   * @param groupingSetsAttrs
   *   The attributes of grouping sets
   * @param groupByAliases
   *   The aliased original group by expressions
   * @param groupByAttrs
   *   The attributes of aliased group by expressions
   * @param gid
   *   Attribute of the grouping id
   * @param child
   *   Child operator
   */
  private def createExpand(
      groupingSetsAttrs: Seq[Seq[Attribute]],
      groupByAliases: Seq[Alias],
      groupByAttrs: Seq[Attribute],
      gid: Attribute,
      child: LogicalPlan,
      neededOutput: Seq[Attribute]): Expand = {
    val attrMap = groupByAttrs.zipWithIndex.toMap

    val hasDuplicateGroupingSets = groupingSetsAttrs.size !=
      groupingSetsAttrs.map(_.map(_.exprId).toSet).distinct.size

    // Create an array of Projections for the child projection, and replace the projections'
    // expressions which equal GroupBy expressions with Literal(null), if those expressions
    // are not set for this grouping set.
    val projections = groupingSetsAttrs.zipWithIndex.map {
      case (groupingSetAttrs, i) =>
        val projAttrs = neededOutput ++ groupByAttrs.map {
          attr =>
            if (!groupingSetAttrs.contains(attr)) {
              // if the input attribute in the Invalid Grouping Expression set of for this group
              // replace it with constant null
              Literal.create(null, attr.dataType)
            } else {
              attr
            }
          // groupingId is the last output, here we use the bit mask as the concrete value for it.
        } :+ {
          val bitMask = buildBitmask(groupingSetAttrs, attrMap)
          val dataType = GroupingID.dataType
          Literal.create(
            if (SparkTypeUtil.sameType(dataType, IntegerType)) bitMask.toInt else bitMask,
            dataType)
        }

        if (hasDuplicateGroupingSets) {
          // If `groupingSetsAttrs` has duplicate entries (e.g., GROUPING SETS ((key), (key))),
          // we add one more virtual grouping attribute (`_gen_grouping_pos`) to avoid
          // wrongly grouping rows with the same grouping ID.
          projAttrs :+ Literal.create(i, IntegerType)
        } else {
          projAttrs
        }
    }

    // the `groupByAttrs` has different meaning in `Expand.output`, it could be the original
    // grouping expression or null, so here we create new instance of it.
    val output = if (hasDuplicateGroupingSets) {
      val gpos = AttributeReference("_gen_grouping_pos", IntegerType, nullable = false)()
      neededOutput ++ groupByAttrs.map(_.newInstance) :+ gid :+ gpos
    } else {
      neededOutput ++ groupByAttrs.map(_.newInstance) :+ gid
    }
    Expand(projections, output, child)
  }
}
