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

import io.glutenproject.expression.ConverterUtils;
import io.glutenproject.substrait.type.TypeBuilder;
import io.glutenproject.substrait.type.TypeNode;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;

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

  public static ByteLiteralNode makeByteLiteral(Byte byteConstant) {
    return new ByteLiteralNode(byteConstant);
  }

  public static ShortLiteralNode makeShortLiteral(Short shortConstant) {
    return new ShortLiteralNode(shortConstant);
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

  public static FloatLiteralNode makeFloatLiteral(Float floatConstant) {
    return new FloatLiteralNode(floatConstant);
  }

  public static DateLiteralNode makeDateLiteral(Integer dateConstant) {
    return new DateLiteralNode(dateConstant);
  }

  public static DateListNode makeDateList(ArrayList<Integer> dateConstants) {
    return new DateListNode(dateConstants);
  }

  public static TimestampLiteralNode makeTimestampLiteral(Long tsConstants) {
    return new TimestampLiteralNode(tsConstants);
  }

  public static StringLiteralNode makeStringLiteral(String strConstant) {
    return new StringLiteralNode(strConstant);
  }

  public static StringListNode makeStringList(ArrayList<String> strConstants) {
    return new StringListNode(strConstants);
  }

  public static BinaryLiteralNode makeBinaryLiteral(byte[] bytesConstant) {
    return new BinaryLiteralNode(bytesConstant);
  }

  public static DecimalLiteralNode makeDecimalLiteral(Decimal decimalConstant) {
    return new DecimalLiteralNode(decimalConstant);
  }

  public static ExpressionNode makeLiteral(Object obj, DataType dataType, Boolean nullable) {
    if (dataType instanceof BooleanType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeBoolean(nullable));
      } else {
        return makeBooleanLiteral((Boolean) obj);
      }
    } else if (dataType instanceof ByteType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeI8(nullable));
      } else {
        return makeByteLiteral((Byte) obj);
      }
    } else if (dataType instanceof ShortType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeI16(nullable));
      } else {
        return makeShortLiteral((Short) obj);
      }
    } else if (dataType instanceof IntegerType) {
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
    } else if (dataType instanceof FloatType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeFP32(nullable));
      } else {
        return makeFloatLiteral((Float) obj);
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
    } else if (dataType instanceof TimestampType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeTimestamp(nullable));
      } else {
        return makeTimestampLiteral((Long) obj);
      }
    } else if (dataType instanceof StringType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeString(nullable));
      } else {
        return makeStringLiteral(obj.toString());
      }
    } else if (dataType instanceof BinaryType) {
      if (obj == null) {
        return makeNullLiteral(TypeBuilder.makeBinary(nullable));
      } else {
        return makeBinaryLiteral((byte[]) obj);
      }
    } else if (dataType instanceof DecimalType) {
      if (obj == null) {
        DecimalType decimal = (DecimalType)dataType;
        checkDecimalScale(decimal.scale());
        return makeNullLiteral(TypeBuilder.makeDecimal(nullable, decimal.precision(),
          decimal.scale()));
      } else {
        Decimal decimal = (Decimal) obj;
        checkDecimalScale(decimal.scale());
        return makeDecimalLiteral(decimal);
      }
    } else if (dataType instanceof ArrayType) {
      if (obj == null) {
        ArrayType arrayType = (ArrayType)dataType;
        return makeNullLiteral(TypeBuilder.makeList(nullable,
                ConverterUtils.getTypeNode(arrayType.elementType(), nullable)));
      } else {
        Object[] elements = ((GenericArrayData) obj).array();
        ArrayList<String> list = new ArrayList<>();
        for (Object element : elements) {
          if (element instanceof UTF8String) {
            list.add(element.toString());
          } else {
            throw new UnsupportedOperationException(
                    String.format("Type not supported: %s.", dataType.toString()));
          }
        }
        return makeStringList(list);
      }
    } else if (dataType instanceof MapType) {
      if (obj == null) {
        MapType mapType = (MapType) dataType;
        TypeNode keyType = ConverterUtils.getTypeNode(mapType.keyType(), false);
        TypeNode valType = ConverterUtils.getTypeNode(mapType.valueType(),
         mapType.valueContainsNull());
        return makeNullLiteral(TypeBuilder.makeMap(nullable, keyType, valType));
      } else {
          throw new UnsupportedOperationException(
            String.format("Type not supported: %s.", dataType.toString()));
      }
    } else if (dataType instanceof NullType) {
        return makeNullLiteral(TypeBuilder.makeNothing());
    } else {
      /// TODO(taiyang-li) implement Literal Node for Struct/Map/Array
      throw new UnsupportedOperationException(
          String.format("Type not supported: %s, obj: %s, class: %s",
          dataType.toString(), obj.toString(), obj.getClass().toString()));
    }
  }

  public static void checkDecimalScale(int scale) {
    if (scale < 0) {
      // Substrait don't support decimal type with negative scale.
      throw new UnsupportedOperationException(String.format(
        "DecimalType with negative scale not supported: %s.", scale));
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

  public static CastNode makeCast(TypeNode typeNode, ExpressionNode expressionNode,
                                  boolean ansiEnabled) {
    return new CastNode(typeNode, expressionNode, ansiEnabled);
  }

  public static StringMapNode makeStringMap(Map<String, String> values) {
    return new StringMapNode(values);
  }

  public static SingularOrListNode makeSingularOrListNode(ExpressionNode value,
                                                          List<ExpressionNode> expressionNodes) {
    return new SingularOrListNode(value, expressionNodes);
  }

  public static WindowFunctionNode makeWindowFunction(
      Integer functionId,
      ArrayList<ExpressionNode> expressionNodes,
      String columnName,
      TypeNode outputTypeNode,
      String upperBound,
      String lowerBound,
      String windowType) {
    return new WindowFunctionNode(functionId, expressionNodes, columnName,
        outputTypeNode, upperBound, lowerBound, windowType);
  }
}
