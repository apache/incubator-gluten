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

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.List;
import java.util.stream.Collectors;

/** Convertor to convert Flink LogicalType to velox data Type */
public class LogicalTypeConverter {

  public static Type toVLType(LogicalType logicalType) {
    if (logicalType instanceof RowType) {
      RowType flinkRowType = (RowType) logicalType;
      List<Type> fieldTypes =
          flinkRowType.getChildren().stream()
              .map(LogicalTypeConverter::toVLType)
              .collect(Collectors.toList());
      return new io.github.zhztheplayer.velox4j.type.RowType(
          flinkRowType.getFieldNames(), fieldTypes);
    } else if (logicalType instanceof BooleanType) {
      return new io.github.zhztheplayer.velox4j.type.BooleanType();
    } else if (logicalType instanceof IntType) {
      return new IntegerType();
    } else if (logicalType instanceof BigIntType) {
      return new io.github.zhztheplayer.velox4j.type.BigIntType();
    } else if (logicalType instanceof DoubleType) {
      return new io.github.zhztheplayer.velox4j.type.DoubleType();
    } else if (logicalType instanceof VarCharType) {
      return new io.github.zhztheplayer.velox4j.type.VarCharType();
    } else if (logicalType instanceof TimestampType) {
      // TODO: may need precision
      return new io.github.zhztheplayer.velox4j.type.TimestampType();
    } else if (logicalType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) logicalType;
      return new io.github.zhztheplayer.velox4j.type.DecimalType(
          decimalType.getPrecision(), decimalType.getScale());
    } else if (logicalType instanceof DayTimeIntervalType) {
      // TODO: it seems interval now can be used as bigint for nexmark.
      return new io.github.zhztheplayer.velox4j.type.BigIntType();
    } else {
      throw new RuntimeException("Unsupported logical type: " + logicalType);
    }
  }
}
