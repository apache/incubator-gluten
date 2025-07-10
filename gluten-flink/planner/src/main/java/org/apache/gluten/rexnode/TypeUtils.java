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
package org.apache.gluten.rexnode;

import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.*;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.stream.Collectors;

public class TypeUtils {
  private static final List<Class<?>> NUMERIC_TYPE_PRIORITY_LIST =
      List.of(
          TinyIntType.class,
          SmallIntType.class,
          IntegerType.class,
          BigIntType.class,
          RealType.class,
          DoubleType.class,
          DecimalType.class);

  private static int getNumericTypePriority(Type type) {
    int index = NUMERIC_TYPE_PRIORITY_LIST.indexOf(type.getClass());
    if (index == -1) {
      // If the type is not found in the list, throw an exception
      throw new RuntimeException("Unsupported type: " + type.getClass().getName());
    }
    return index;
  }

  public static boolean isNumericType(Type type) {
    return NUMERIC_TYPE_PRIORITY_LIST.contains(type.getClass());
  }

  public static boolean isStringType(Type type) {
    return type instanceof VarCharType;
  }

  public static List<TypedExpr> promoteTypeForArithmeticExpressions(List<TypedExpr> expressions) {
    if (expressions.isEmpty()) {
      throw new RuntimeException("No expressions found");
    }
    if (areAllSameType(expressions)) {
      return expressions;
    }

    boolean hasDecimal = false;
    boolean hasOtherNumeric = false;

    for (TypedExpr expr : expressions) {
      Type type = expr.getReturnType();
      if (type instanceof DecimalType) {
        hasDecimal = true;
      } else if (isBasicNumericType(type)) {
        hasOtherNumeric = true;
      }
    }
    if (hasDecimal) {
      Type targetType = calculateDecimalType(expressions);
      return expressions.stream()
          .map(
              expr ->
                  expr.getReturnType().equals(targetType)
                      ? expr
                      : CastTypedExpr.create(targetType, expr, false))
          .collect(Collectors.toList());
    }

    if (hasOtherNumeric) {
      return promoteBasicNumericTypes(expressions);
    }

    return expressions;
  }

  private static boolean isBasicNumericType(Type type) {
    return type instanceof TinyIntType
        || type instanceof SmallIntType
        || type instanceof IntegerType
        || type instanceof BigIntType
        || type instanceof RealType
        || type instanceof DoubleType;
  }

  private static List<TypedExpr> promoteBasicNumericTypes(List<TypedExpr> expressions) {
    Type targetType =
        expressions.stream()
            .map(
                expr -> {
                  Type returnType = expr.getReturnType();
                  int priority = getNumericTypePriority(returnType);
                  return new Tuple2<>(priority, returnType);
                })
            .max((t1, t2) -> Integer.compare(t1.f0, t2.f0))
            .orElseThrow(() -> new RuntimeException("No expressions found"))
            .f1;

    return expressions.stream()
        .map(
            expr ->
                expr.getReturnType().equals(targetType)
                    ? expr
                    : CastTypedExpr.create(targetType, expr, false))
        .collect(Collectors.toList());
  }

  private static Type calculateDecimalType(List<TypedExpr> expressions) {
    int maxIntegerDigits = 0;
    int maxScale = 0;

    for (TypedExpr expr : expressions) {
      Type type = expr.getReturnType();

      if (type instanceof DecimalType) {
        DecimalType decimal = (DecimalType) type;
        int integerDigits = decimal.getPrecision() - decimal.getScale();
        maxIntegerDigits = Math.max(maxIntegerDigits, integerDigits);
        maxScale = Math.max(maxScale, decimal.getScale());
      } else if (type instanceof BigIntType) {
        maxIntegerDigits = Math.max(maxIntegerDigits, 19);
      } else if (type instanceof IntegerType) {
        maxIntegerDigits = Math.max(maxIntegerDigits, 10);
      } else if (type instanceof SmallIntType) {
        maxIntegerDigits = Math.max(maxIntegerDigits, 5);
      } else if (type instanceof TinyIntType) {
        maxIntegerDigits = Math.max(maxIntegerDigits, 3);
      }
    }

    int totalPrecision = maxIntegerDigits + maxScale;

    if (totalPrecision > 38) {
      if (maxIntegerDigits >= 32) {
        maxScale = Math.min(maxScale, 6);
      } else {
        maxScale = 38 - maxIntegerDigits;
      }
      totalPrecision = maxIntegerDigits + maxScale;
    }

    return new DecimalType(totalPrecision, maxScale);
  }

  private static boolean areAllSameType(List<TypedExpr> expressions) {
    if (expressions.size() <= 1) {
      return true;
    }

    Type firstType = expressions.get(0).getReturnType();
    return expressions.stream().allMatch(expr -> expr.getReturnType().equals(firstType));
  }
}
