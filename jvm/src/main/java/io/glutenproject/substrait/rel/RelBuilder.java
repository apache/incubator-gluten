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

package io.glutenproject.substrait.rel;

import io.glutenproject.expression.ConverterUtils;
import io.glutenproject.expression.ConverterUtils$;
import io.glutenproject.substrait.SubstraitContext;
import io.glutenproject.substrait.expression.AggregateFunctionNode;
import io.glutenproject.substrait.expression.ExpressionNode;
import io.glutenproject.substrait.extensions.AdvancedExtensionNode;
import io.glutenproject.substrait.type.TypeNode;
import org.apache.spark.sql.catalyst.expressions.Attribute;

import java.util.ArrayList;

/**
 * Contains helper functions for constructing substrait relations.
 */
public class RelBuilder {
  private RelBuilder() {}

  public static RelNode makeFilterRel(RelNode input,
                                      ExpressionNode condition) {
    return new FilterRelNode(input, condition);
  }

  public static RelNode makeFilterRel(RelNode input,
                                      ExpressionNode condition,
                                      AdvancedExtensionNode extensionNode) {
    return new FilterRelNode(input, condition, extensionNode);
  }

  public static RelNode makeProjectRel(RelNode input,
                                       ArrayList<ExpressionNode> expressionNodes) {
    return new ProjectRelNode(input, expressionNodes);
  }

  public static RelNode makeProjectRel(RelNode input,
                                       ArrayList<ExpressionNode> expressionNodes,
                                       AdvancedExtensionNode extensionNode) {
    return new ProjectRelNode(input, expressionNodes, extensionNode);
  }

  public static RelNode makeAggregateRel(RelNode input,
                                         ArrayList<ExpressionNode> groupings,
                                         ArrayList<AggregateFunctionNode> aggregateFunctionNodes) {
    return new AggregateRelNode(input, groupings, aggregateFunctionNodes);
  }

  public static RelNode makeReadRel(ArrayList<TypeNode> types, ArrayList<String> names,
                                    ExpressionNode filter, SubstraitContext context) {
    return new ReadRelNode(types, names, filter, context);
  }

  public static RelNode makeReadRel(ArrayList<TypeNode> types, ArrayList<String> names,
                                    SubstraitContext context) {
    return new ReadRelNode(types, names, context);
  }

  public static RelNode makeReadRel(ArrayList<Attribute> attributes,
                                    SubstraitContext context) {
    ArrayList<TypeNode> typeList = new ArrayList<>();
    ArrayList<String> nameList = new ArrayList<>();
    ConverterUtils$ converter = ConverterUtils$.MODULE$;
    for (Attribute attr : attributes) {
      typeList.add(converter.getTypeNode(attr.dataType(), attr.nullable()));
      nameList.add(attr.name());
    }

    // The iterator index will be added in the path of LocalFiles.
    context.setLocalFilesNode(LocalFilesBuilder.makeLocalFiles(
            converter.ITERATOR_PREFIX().concat(context.getIteratorIndex().toString())));
    return new ReadRelNode(typeList, nameList, context);
  }
}
