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
package org.apache.spark.shuffle.utils

import io.glutenproject.execution.SortExecTransformer
import io.glutenproject.expression.ExpressionConverter
import io.glutenproject.expression.ExpressionTransformer
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.plan.PlanNode
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.vectorized.{BlockNativeConverter, BlockSplitIterator, CHNativeBlock, CloseablePartitionedBlockIterator, NativePartitioning}

import org.apache.spark.{Partitioner, RangePartitioner, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BoundReference, NamedExpression, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import io.substrait.proto.ProjectRel
import io.substrait.proto.Rel
import io.substrait.proto.RelCommon
import play.api.libs.json._

import java.util
import java.util.Base64

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

/**
 * In spark RangePartitioner, the rangeBounds is private, so we make a copied-implementation here.
 * It is based on the fact that, there has been a pre-projection before the range partition and
 * remove all function expressions in the sort ordering expressions.
 */
class RangePartitionerBoundsGenerator[K: Ordering: ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    ordering: Seq[SortOrder],
    inputAttributes: Seq[Attribute],
    private var ascending: Boolean = true,
    val samplePointsPerPartitionHint: Int = 20
) {

  def getRangeBounds(): Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach {
          case (idx, n, sample) =>
            if (fraction * n > sampleSizePerPartition) {
              imbalancedPartitions += idx
            } else {
              val weight = (n.toDouble / sample.length).toFloat
              for (key <- sample) {
                candidates += ((key, weight))
              }
            }
        }
        if (imbalancedPartitions.nonEmpty) {
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, math.min(partitions, candidates.size))
      }
    }
  }

  /*
   return json structure
   {
    "ordering":[
      {
       "column_ref":0,
        "data_type":"xxx",
        "is_nullable":true,
        "direction":0
      },
        ...
    ],
    "range_bounds":[
     {
         "is_null":false,
         "value": ...
       },
       {
         "is_null":true
       },
       ...
    ]
    }
   */
  private def getExpressionFieldReference(
      context: SubstraitContext,
      ordering: SortOrder,
      attributes: Seq[Attribute]): Int = {
    val funcs = context.registeredFunction
    val colExpr =
      ExpressionConverter.replaceWithExpressionTransformer(ordering.child, attributes)
    val projExprNode = colExpr.asInstanceOf[ExpressionTransformer].doTransform(funcs)
    val pb = projExprNode.toProtobuf
    if (!pb.hasSelection()) {
      throw new IllegalArgumentException(s"A sorting field should be an attribute")
    } else {
      pb.getSelection().getDirectReference().getStructField.getField()
    }
  }

  private def buildProjectionPlan(
      context: SubstraitContext,
      sortExpressions: Seq[NamedExpression]): PlanNode = {
    val args = context.registeredFunction
    val columnarProjExprs = sortExpressions.map(
      expr => {
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, attributeSeq = inputAttributes)
      })
    val projExprNodeList = new java.util.ArrayList[ExpressionNode]()
    for (expr <- columnarProjExprs) {
      projExprNodeList.add(expr.asInstanceOf[ExpressionTransformer].doTransform(args))
    }
    val projectRel = RelBuilder.makeProjectRel(null, projExprNodeList, context, 0)
    val outNames = new util.ArrayList[String]
    val relNodes = new util.ArrayList[RelNode]()
    relNodes.add(projectRel)
    PlanBuilder.makePlan(context, relNodes, outNames)
  }

  private def buildProjectionAttributesByOrderings(
      sortOrders: Seq[SortOrder]): (Seq[NamedExpression], Seq[SortOrder]) = {
    val projectionAttrs = new util.ArrayList[NamedExpression]()
    val newSortOrders = new util.ArrayList[SortOrder]()
    var aliasNo = 0
    sortOrders.foreach(
      order => {
        if (!order.child.isInstanceOf[Attribute]) {
          val alias = new Alias(order.child, s"sort_col_$aliasNo")()
          aliasNo += 1
          projectionAttrs.add(alias)
          newSortOrders.add(
            SortOrder(
              alias.toAttribute,
              order.direction,
              order.nullOrdering,
              order.sameOrderExpressions))
        } else {
          newSortOrders.add(order)
        }
      })
    (projectionAttrs.asScala, newSortOrders.asScala)
  }

  private def buildOrderingJson(
      context: SubstraitContext,
      orderings: Seq[SortOrder],
      attributes: Seq[Attribute]): JsValue = {

    val data = orderings.map {
      order =>
        {
          val orderJson = Json.toJson(
            Map(
              "column_ref" -> Json.toJson(getExpressionFieldReference(context, order, attributes)),
              "data_type" -> Json.toJson(order.dataType.toString),
              "is_nullable" -> Json.toJson(order.nullable),
              "direction" -> Json.toJson(SortExecTransformer
                .transformSortDirection(order.direction.sql, order.nullOrdering.sql))
            ))
          orderJson
        }
    }
    Json.toJson(data)
  }

  private def getFieldValue(row: UnsafeRow, dataType: DataType, field: Int): JsValue = {
    dataType match {
      case _: BooleanType => Json.toJson(row.getBoolean(field))
      case _: ByteType => Json.toJson(row.getByte(field))
      case _: ShortType => Json.toJson(row.getShort(field))
      case _: IntegerType => Json.toJson(row.getInt(field))
      case _: LongType => Json.toJson(row.getLong(field))
      case _: FloatType => Json.toJson(row.getFloat(field))
      case _: DoubleType => Json.toJson(row.getDouble(field))
      case _: StringType => Json.toJson(row.getString(field))
      case _: DateType => Json.toJson(row.getShort(field))
      case d =>
        throw new IllegalArgumentException(s"Unsupported data type ${d.toString}")
    }
  }
  private def buildRangeBoundJson(row: UnsafeRow, ordering: Seq[SortOrder]): JsValue = {
    val data = Json.toJson(
      (0 until row.numFields).map {
        i =>
          {
            if (row.isNullAt(i)) {
              Json.toJson(
                Map(
                  "is_null" -> Json.toJson(true)
                ))
            } else {
              val order = ordering(i)
              Json.toJson(
                Map(
                  "is_null" -> Json.toJson(false),
                  "value" -> getFieldValue(row, order.dataType, i)
                ))
            }
          }
      }
    )
    data
  }

  private def buildRangeBoundsJson(): JsValue = {
    val bounds = getRangeBounds()
    val data = Json.toJson(
      bounds.map(
        bound => {
          // Be careful, it should be an unsafe row here
          val row = bound.asInstanceOf[UnsafeRow]
          buildRangeBoundJson(row, ordering)
        })
    )
    data
  }

  // Make a json structure that can be passed to native engine
  def getRangeBoundsJsonString(): String = {
    val context = new SubstraitContext()
    val (sortExpressions, newOrderings) = buildProjectionAttributesByOrderings(
      ordering
    )
    val totalAttributes = new util.ArrayList[Attribute]()
    inputAttributes.foreach(attr => totalAttributes.add(attr))
    sortExpressions.foreach(expr => totalAttributes.add(expr.toAttribute))
    val data = if (sortExpressions.size == 0) {
      Json.toJson(
        Map(
          "ordering" -> buildOrderingJson(context, newOrderings, totalAttributes.asScala),
          "range_bounds" -> buildRangeBoundsJson()
        ))
    } else {
      // If there is any expressions in orderings, we build a projection plan and pass
      // it to backend
      val projectPlan = buildProjectionPlan(context, sortExpressions).toProtobuf
      val serializeProjectPlan = Base64.getEncoder().encodeToString(projectPlan.toByteArray)
      Json.toJson(
        Map(
          "projection_plan" -> Json.toJson(serializeProjectPlan),
          "ordering" -> buildOrderingJson(context, newOrderings, totalAttributes.asScala),
          "range_bounds" -> buildRangeBoundsJson()
        ))

    }
    Json.stringify(data)
  }
}

object RangePartitionerBoundsGenerator {
  def supportedFieldType(dataType: DataType): Boolean = {
    dataType match {
      case _: BooleanType => true
      case _: ByteType => true
      case _: ShortType => true
      case _: IntegerType => true
      case _: LongType => true
      case _: FloatType => true
      case _: DoubleType => true
      case _: StringType => true
      case _: DateType => true
      case _ => false
    }
  }
}
