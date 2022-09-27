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

package io.glutenproject.substrait.expression;

import io.glutenproject.substrait.type.TypeBuilder;
import io.glutenproject.substrait.type.TypeNode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Contains helper functions for constructing substrait relations. */
public class ExpressionBuilder {
  private ExpressionBuilder() {}

  public static Long newScalarFunction(Map<String, Long> functionMap, String functionName) {
    if (!functionMap.containsKey(functionName)) {
      Long functionId = (long) functionMap.size();
      functionMap.put(functionName, functionId);
      return functionId;
    } else {
      return functionMap.get(functionName);
    }
  }

  public static NullLiteralNode makeNullLiteral(TypeNode typeNode) {
    return new NullLiteralNode(typeNode);
  }

  public static BooleanLiteralNode makeBooleanLiteral(Boolean booleanConstant) {
    return new BooleanLiteralNode(booleanConstant);
  }

  public static IntLiteralNode makeIntLiteral(Integer intConstant) {
    return new IntLiteralNode(intConstant);
  }

  public static IntListNode makeIntList(ArrayList<Integer> intConstants) {
    return new IntListNode(intConstants);
  }

  public static LongLiteralNode makeLongLiteral(Long longConstant) {
    return new LongLiteralNode(longConstant);
  }

  public static LongListNode makeLongList(ArrayList<Long> longConstants) {
    return new LongListNode(longConstants);
  }

  public static DoubleLiteralNode makeDoubleLiteral(Double doubleConstant) {
    return new DoubleLiteralNode(doubleConstant);
  }

  public static DoubleListNode makeDoubleList(ArrayList<Double> doubleConstants) {
    return new DoubleListNode(doubleConstants);
  }

  public static DateLiteralNode makeDateLiteral(Integer dateConstant) {
    return new DateLiteralNode(dateConstant);
  }

  public static DateListNode makeDateList(ArrayList<Integer> dateConstants) {
    return new DateListNode(dateConstants);
  }

  public static StringLiteralNode makeStringLiteral(String strConstant) {
    return new StringLiteralNode(strConstant);
  }

  public static StringListNode makeStringList(ArrayList<String> strConstants) {
    return new StringListNode(strConstants);
  }

  public static DecimalLiteralNode makeDecimalLiteral(Decimal decimalConstant) {
    return new DecimalLiteralNode(decimalConstant);
  }

  public static ExpressionNode makeLiteral(Object obj, DataType dataType, Boolean nullable) {
    if (dataType instanceof IntegerType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeI32(nullable));
      } else {
        return makeIntLiteral((Integer) obj);
      }
    } else if (dataType instanceof LongType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeI64(nullable));
      } else {
        return makeLongLiteral((Long) obj);
      }
    } else if (dataType instanceof DoubleType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeFP64(nullable));
      } else {
        return makeDoubleLiteral((Double) obj);
      }
    } else if (dataType instanceof DateType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeDate(nullable));
      } else {
        return makeDateLiteral((Integer) obj);
      }
    } else if (dataType instanceof StringType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeString(nullable));
      } else {
        return makeStringLiteral(obj.toString());
      }
    } else if (dataType instanceof DecimalType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeDecimal(nullable, 0, 0));
      } else {
        return makeDecimalLiteral((Decimal) obj);
      }
    } else {
      throw new UnsupportedOperationException(
          String.format("Type not supported: %s.", dataType.toString()));
    }
  }

  public static ScalarFunctionNode makeScalarFunction(
      Long functionId, ArrayList<ExpressionNode> expressionNodes, TypeNode typeNode) {
    return new ScalarFunctionNode(functionId, expressionNodes, typeNode);
  }

  public static SelectionNode makeSelection(Integer fieldIdx) {
    return new SelectionNode(fieldIdx);
  }

  public static SelectionNode makeSelection(Integer fieldIdx, Integer childFieldIdx) {
    return new SelectionNode(fieldIdx, childFieldIdx);
  }

  public static AggregateFunctionNode makeAggregateFunction(
      Long functionId,
      ArrayList<ExpressionNode> expressionNodes,
      String phase,
      TypeNode outputTypeNode) {
    return new AggregateFunctionNode(functionId, expressionNodes, phase, outputTypeNode);
  }

  public static CastNode makeCast(TypeNode typeNode, ExpressionNode expressionNode) {
    return new CastNode(typeNode, expressionNode);
  }

  public static StringMapNode makeStringMap(Map<String, String> values) {
    return new StringMapNode(values);
  }

  public static SingularOrListNode makeSingularOrListNode(ExpressionNode value,
                                                          List<ExpressionNode> expressionNodes) {
    return new SingularOrListNode(value, expressionNodes);
  }
}
