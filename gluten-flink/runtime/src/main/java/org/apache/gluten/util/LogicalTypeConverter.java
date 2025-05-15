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
package org.apache.gluten.util;

import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.RowType.RowField;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Convertor to convert Flink LogicalType to velox data Type */
public class LogicalTypeConverter {

    public static LogicalType fromVLType(Type type) {
        if (type instanceof io.github.zhztheplayer.velox4j.type.IntegerType) {
            return new IntType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.BigIntType) {
            return new BigIntType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.SmallIntType) {
            return new SmallIntType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.TinyIntType) {
            return new TinyIntType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.RealType) {
            return new FloatType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.DoubleType) {
            return new DoubleType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.BooleanType) {
            return new BooleanType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.TimestampType) {
            return new TimestampType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.VarCharType) {
            return new VarCharType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.VarbinaryType) {
            return new VarBinaryType();
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.DecimalType) {
            io.github.zhztheplayer.velox4j.type.DecimalType decimalType =
                    (io.github.zhztheplayer.velox4j.type.DecimalType) type;
            return new DecimalType(decimalType.getPrecision(), decimalType.getScale());
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.RowType) {
            io.github.zhztheplayer.velox4j.type.RowType rowType = (io.github.zhztheplayer.velox4j.type.RowType) type;
            List<Type> subTypes = rowType.getChildren();
            List<String> subNames = rowType.getNames();
            List<RowField> subRowFields = new ArrayList<>();
            for (int i = 0; i < subTypes.size(); ++i) {
                Type subType = subTypes.get(i);
                String subName = subNames.get(i);
                LogicalType flinkType = fromVLType(subType);
                RowField rowField = new RowField(subName, flinkType);
                subRowFields.add(rowField);
            }
            return new RowType(subRowFields);
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.MapType) {
            io.github.zhztheplayer.velox4j.type.MapType mapType = (io.github.zhztheplayer.velox4j.type.MapType) type;
            List<Type> keyValueTypes = mapType.getChildren();
            LogicalType flinkKeyType = fromVLType(keyValueTypes.get(0));
            LogicalType flinkValType = fromVLType(keyValueTypes.get(1));
            return new MapType(flinkKeyType, flinkValType);
        } else if (type instanceof io.github.zhztheplayer.velox4j.type.ArrayType) {
            io.github.zhztheplayer.velox4j.type.ArrayType arrayType =
                (io.github.zhztheplayer.velox4j.type.ArrayType) type;
            List<Type> elementTypes = arrayType.getChildren();
            LogicalType flinkElementType = fromVLType(elementTypes.get(0));
            return new ArrayType(flinkElementType);
        } else {
            throw new RuntimeException("Unsupported type:" + type);
        }
    }

    public static Type toVLType(LogicalType logicalType) {
        if (logicalType instanceof RowType) {
            RowType flinkRowType = (RowType) logicalType;
            List<Type> fieldTypes = flinkRowType.getChildren().stream().
                    map(LogicalTypeConverter::toVLType).
                    collect(Collectors.toList());
            return new io.github.zhztheplayer.velox4j.type.RowType(
                    flinkRowType.getFieldNames(),
                    fieldTypes);
        } else if (logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            Type vlType = toVLType(arrayType.getElementType());
            return io.github.zhztheplayer.velox4j.type.ArrayType.create(vlType);
        } else if (logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            Type vlKeyType = toVLType(mapType.getKeyType());
            Type vlValueType = toVLType(mapType.getValueType());
            return io.github.zhztheplayer.velox4j.type.MapType.create(vlKeyType, vlValueType);
        } else if (logicalType instanceof BooleanType) {
            return new io.github.zhztheplayer.velox4j.type.BooleanType();
        } else if (logicalType instanceof IntType) {
            return new IntegerType();
        } else if (logicalType instanceof SmallIntType) {
            return new io.github.zhztheplayer.velox4j.type.SmallIntType();
        } else if (logicalType instanceof TinyIntType) {
            return new io.github.zhztheplayer.velox4j.type.TinyIntType();
        } else if (logicalType instanceof FloatType) {
            return new io.github.zhztheplayer.velox4j.type.RealType();
        } else if (logicalType instanceof DoubleType) {
            return new io.github.zhztheplayer.velox4j.type.DoubleType();
        } else if (logicalType instanceof BigIntType) {
            return new io.github.zhztheplayer.velox4j.type.BigIntType();
        } else if (logicalType instanceof VarCharType) {
            return new io.github.zhztheplayer.velox4j.type.VarCharType();
        } else if (logicalType instanceof VarBinaryType) {
            return new io.github.zhztheplayer.velox4j.type.VarbinaryType();
        } else if (logicalType instanceof TimestampType) {
            // TODO: may need precision
            return new io.github.zhztheplayer.velox4j.type.TimestampType();
        } else if (logicalType instanceof DateType)  {
            return new io.github.zhztheplayer.velox4j.type.DateType();
        } else if (logicalType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) logicalType;
            return new io.github.zhztheplayer.velox4j.type.DecimalType(
                    decimalType.getPrecision(),
                    decimalType.getScale());
        } else if (logicalType instanceof DayTimeIntervalType) {
            // TODO: it seems interval now can be used as bigint for nexmark.
            return new io.github.zhztheplayer.velox4j.type.BigIntType();
        } else {
            throw new RuntimeException("Unsupported logical type: " + logicalType);
        }
    }
}
