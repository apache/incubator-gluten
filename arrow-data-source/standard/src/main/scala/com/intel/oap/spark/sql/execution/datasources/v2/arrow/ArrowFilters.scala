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

package com.intel.oap.spark.sql.execution.datasources.v2.arrow

import org.apache.arrow.dataset.DatasetTypes
import org.apache.arrow.dataset.DatasetTypes.TreeNode
import org.apache.arrow.dataset.filter.FilterImpl

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

object ArrowFilters {
  def pruneWithSchema(pushedFilters: Array[Filter], schema: StructType): Seq[Filter] = {
    pushedFilters.filter(pushedFilter => {
      isToBeAccepted(pushedFilter, schema)
    })
  }

  private def isToBeAccepted(pushedFilter: Filter, schema: StructType): Boolean = {
    pushedFilter match {
      case EqualTo(attribute, value) => existsIn(attribute, schema)
      case GreaterThan(attribute, value) => existsIn(attribute, schema)
      case GreaterThanOrEqual(attribute, value) => existsIn(attribute, schema)
      case LessThan(attribute, value) => existsIn(attribute, schema)
      case LessThanOrEqual(attribute, value) => existsIn(attribute, schema)
      case Not(child) => isToBeAccepted(child, schema)
      case And(left, right) => isToBeAccepted(left, schema) && isToBeAccepted(right, schema)
      case Or(left, right) => isToBeAccepted(left, schema) && isToBeAccepted(right, schema)
      case IsNotNull(attribute) => existsIn(attribute, schema)
      case IsNull(attribute) => existsIn(attribute, schema)
      case _ => false // fixme complete this
    }
  }

  private def existsIn(attr: String, schema: StructType): Boolean = {
    schema.foreach(f => {
      if (f.name == attr) {
        return true;
      }
    })
    false
  }

  def translateFilters(pushedFilters: Seq[Filter]): org.apache.arrow.dataset.filter.Filter = {
    val node = pushedFilters
      .flatMap(translateFilter)
      .reduceOption((t1: TreeNode, t2: TreeNode) => {
        DatasetTypes.TreeNode.newBuilder.setAndNode(
          DatasetTypes.AndNode.newBuilder()
            .setLeftArg(t1)
            .setRightArg(t2)
            .build()).build()
      })
    if (node.isDefined) {
      new FilterImpl(DatasetTypes.Condition.newBuilder()
        .setRoot(node.get).build)
    } else {
      org.apache.arrow.dataset.filter.Filter.EMPTY
    }
  }

  private def translateValue(value: Any): Option[TreeNode] = {
    value match {
      case v: Integer => Some(
        DatasetTypes.TreeNode.newBuilder.setIntNode(
          DatasetTypes.IntNode.newBuilder.setValue(v).build)
          .build)
      case v: Long => Some(
        DatasetTypes.TreeNode.newBuilder.setLongNode(
          DatasetTypes.LongNode.newBuilder.setValue(v).build)
          .build)
      case v: Float => Some(
        DatasetTypes.TreeNode.newBuilder.setFloatNode(
          DatasetTypes.FloatNode.newBuilder.setValue(v).build)
          .build)
      case v: Double => Some(
        DatasetTypes.TreeNode.newBuilder.setDoubleNode(
          DatasetTypes.DoubleNode.newBuilder.setValue(v).build)
          .build)
      case v: Boolean => Some(
        DatasetTypes.TreeNode.newBuilder.setBooleanNode(
          DatasetTypes.BooleanNode.newBuilder.setValue(v).build)
          .build)
      case _ => None // fixme complete this
    }
  }

  private def translateFilter(pushedFilter: Filter): Option[TreeNode] = {
    pushedFilter match {
      case EqualTo(attribute, value) =>
        createComparisonNode("equal", attribute, value)
      case GreaterThan(attribute, value) =>
        createComparisonNode("greater", attribute, value)
      case GreaterThanOrEqual(attribute, value) =>
        createComparisonNode("greater_equal", attribute, value)
      case LessThan(attribute, value) =>
        createComparisonNode("less", attribute, value)
      case LessThanOrEqual(attribute, value) =>
        createComparisonNode("less_equal", attribute, value)
      case Not(child) =>
        createNotNode(child)
      case And(left, right) =>
        createAndNode(left, right)
      case Or(left, right) =>
        createOrNode(left, right)
      case IsNotNull(attribute) =>
        createIsNotNullNode(attribute)
      case IsNull(attribute) =>
        createIsNullNode(attribute)
      case _ => None // fixme complete this
    }
  }

  private def createComparisonNode(opName: String,
                                   attribute: String, value: Any): Option[TreeNode] = {
    val translatedValue = translateValue(value)
    translatedValue match {
      case Some(v) => Some(
        DatasetTypes.TreeNode.newBuilder.setCpNode(
          DatasetTypes.ComparisonNode.newBuilder
            .setOpName(opName) // todo make op names enumerable
            .setLeftArg(
              DatasetTypes.TreeNode.newBuilder.setFieldNode(
                DatasetTypes.FieldNode.newBuilder.setName(attribute).build)
                .build)
            .setRightArg(v)
            .build)
          .build)
      case None => None
    }
  }

  def createNotNode(child: Filter): Option[TreeNode] = {
    val translatedChild = translateFilter(child)
    if (translatedChild.isEmpty) {
      return None
    }
    Some(DatasetTypes.TreeNode.newBuilder
      .setNotNode(DatasetTypes.NotNode.newBuilder.setArgs(translatedChild.get).build()).build())
  }

  def createIsNotNullNode(attribute: String): Option[TreeNode] = {
    Some(DatasetTypes.TreeNode.newBuilder
      .setIsValidNode(
        DatasetTypes.IsValidNode.newBuilder.setArgs(
          DatasetTypes.TreeNode.newBuilder.setFieldNode(
            DatasetTypes.FieldNode.newBuilder.setName(attribute).build)
            .build).build()).build())
  }

  def createIsNullNode(attribute: String): Option[TreeNode] = {
    Some(DatasetTypes.TreeNode.newBuilder
      .setNotNode(
        DatasetTypes.NotNode.newBuilder.setArgs(
          DatasetTypes.TreeNode.newBuilder
            .setIsValidNode(
              DatasetTypes.IsValidNode.newBuilder.setArgs(
                DatasetTypes.TreeNode.newBuilder.setFieldNode(
                  DatasetTypes.FieldNode.newBuilder.setName(attribute).build)
                  .build)
                .build()).build()).build()).build())
  }

  def createAndNode(left: Filter, right: Filter): Option[TreeNode] = {
    val translatedLeft = translateFilter(left)
    val translatedRight = translateFilter(right)
    if (translatedLeft.isEmpty || translatedRight.isEmpty) {
      return None
    }
    Some(DatasetTypes.TreeNode.newBuilder
      .setAndNode(DatasetTypes.AndNode.newBuilder
        .setLeftArg(translatedLeft.get)
        .setRightArg(translatedRight.get)
        .build())
      .build())
  }

  def createOrNode(left: Filter, right: Filter): Option[TreeNode] = {
    val translatedLeft = translateFilter(left)
    val translatedRight = translateFilter(right)
    if (translatedLeft.isEmpty || translatedRight.isEmpty) {
      return None
    }
    Some(DatasetTypes.TreeNode.newBuilder
      .setOrNode(DatasetTypes.OrNode.newBuilder
        .setLeftArg(translatedLeft.get)
        .setRightArg(translatedRight.get)
        .build())
      .build())
  }
}
