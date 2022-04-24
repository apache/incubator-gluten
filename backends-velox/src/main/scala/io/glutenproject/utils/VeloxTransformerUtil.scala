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

package io.glutenproject.utils

import scala.collection.JavaConverters._
import com.google.common.collect.Lists

import io.glutenproject.expression.CodeGeneration
import org.apache.arrow.gandiva.expression.{TreeBuilder, TreeNode}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import org.apache.spark.sql.catalyst.expressions.Attribute

object VeloxTransformerUtil {

  def prepareRelationFunction(
                               keyAttributes: Seq[Attribute],
                               outputAttributes: Seq[Attribute]): TreeNode = {
    val outputFieldList: List[Field] = outputAttributes.toList.map(attr => {
      Field
        .nullable(s"${attr.name.toUpperCase()}#${attr.exprId.id}",
          CodeGeneration.getResultType(attr.dataType))
    })

    val keyFieldList: List[Field] = keyAttributes.toList.map(attr => {
      val field = Field
        .nullable(s"${attr.name.toUpperCase()}#${attr.exprId.id}",
          CodeGeneration.getResultType(attr.dataType))
      if (outputFieldList.indexOf(field) == -1) {
        throw new UnsupportedOperationException(s"CachedRelation not found" +
          s"${attr.name.toUpperCase()}#${attr.exprId.id} in ${outputAttributes}")
      }
      field
    })

    val key_args_node = TreeBuilder.makeFunction(
      "key_field",
      keyFieldList
        .map(field => {
          TreeBuilder.makeField(field)
        })
        .asJava,
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )

    val cachedRelationFuncName = "CachedRelation"
    val cached_relation_func = TreeBuilder.makeFunction(
      cachedRelationFuncName,
      Lists.newArrayList(key_args_node),
      new ArrowType.Int(32, true) /*dummy ret type, won't be used*/ )

    TreeBuilder.makeFunction(
      "standalone",
      Lists.newArrayList(cached_relation_func),
      new ArrowType.Int(32, true))
  }
}
