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
package org.apache.gluten.substrait.expression;

import org.apache.gluten.substrait.type.IntervalYearTypeNode;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Expression;
import io.substrait.proto.Expression.Literal.Builder;

public class IntervalYearLiteralNode extends LiteralNodeWithValue<Integer> {
  public IntervalYearLiteralNode(Integer value) {
    super(value, new IntervalYearTypeNode(true));
  }

  public IntervalYearLiteralNode(Integer value, TypeNode typeNode) {
    super(value, typeNode);
  }

  @Override
  protected void updateLiteralBuilder(Builder literalBuilder, Integer totalMonths) {
    Expression.Literal.IntervalYearToMonth.Builder intervalBuilder =
        Expression.Literal.IntervalYearToMonth.newBuilder();

    int years = totalMonths / 12;
    int months = totalMonths % 12;
    intervalBuilder.setYears(years);
    intervalBuilder.setMonths(months);

    literalBuilder.setIntervalYearToMonth(intervalBuilder.build());
  }
}
