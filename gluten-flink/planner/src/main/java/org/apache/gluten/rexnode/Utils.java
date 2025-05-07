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
import io.github.zhztheplayer.velox4j.serializable.ISerializableRegistry;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.VariantRegistry;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors

/** Utility to store some useful functions. */
public class Utils {

    private static boolean registryInitialized = false;

    // Get names for project node.
    public static List<String> getNamesFromRowType(LogicalType logicalType) {
        if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            return rowType.getFieldNames();
        } else {
            throw new RuntimeException("Output type is not row type: " + logicalType);
        }
    }

    // Init serialize related registries.
    public static void registerRegistry() {
        if (!registryInitialized) {
            registryInitialized = true;
            VariantRegistry.registerAll();
            ISerializableRegistry.registerAll();
        }
    }

    private static final List<String> NUMBER_TYPE_PRIORITY_LIST = List.of(
        "TinyIntType",
        "SmallIntType",
        "IntegerType",
        "BigIntType",
        "RealType",
        "DoubleType"
    );

    private static int getNumberTypePriority(String typeName) {
        int index = NUMBER_TYPE_PRIORITY_LIST.indexOf(typeName);
        if (index == -1) {
            throw new RuntimeException("Unsupported type: " + typeName);
        }
        return index;
    }

    public static List<TypedExpr> promoteTypeForNumberExpressions(List<TypedExpr> expressions) {
        Type highestType = expressions.stream()
            .map(expr -> {
                Type returnType = expr.getReturnType();
                int priority = getNumberTypePriority(returnType.getClass().getSimpleName());
            return new Tuple2<>(priority, returnType);
        })
        .max((t1, t2) -> Integer.compare(t1.f0, t2.f0))
        .orElseThrow(() -> new RuntimeException("No expressions found")).f1;

        return expressions.stream()
            .map(expr -> expr.getReturnType().equals(highestType) ? expr : CastTypedExpr.create(highestType, expr, false))
            .collect(Collectors.toList());
    }
}
