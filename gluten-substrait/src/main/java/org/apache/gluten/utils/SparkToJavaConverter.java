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
package org.apache.gluten.utils;

import org.apache.gluten.substrait.type.BinaryTypeNode;
import org.apache.gluten.substrait.type.BooleanTypeNode;
import org.apache.gluten.substrait.type.DateTypeNode;
import org.apache.gluten.substrait.type.DecimalTypeNode;
import org.apache.gluten.substrait.type.FP32TypeNode;
import org.apache.gluten.substrait.type.FP64TypeNode;
import org.apache.gluten.substrait.type.I16TypeNode;
import org.apache.gluten.substrait.type.I32TypeNode;
import org.apache.gluten.substrait.type.I64TypeNode;
import org.apache.gluten.substrait.type.I8TypeNode;
import org.apache.gluten.substrait.type.ListNode;
import org.apache.gluten.substrait.type.MapNode;
import org.apache.gluten.substrait.type.StringTypeNode;
import org.apache.gluten.substrait.type.StructNode;
import org.apache.gluten.substrait.type.TimestampTypeNode;
import org.apache.gluten.substrait.type.TypeNode;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.Decimal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkToJavaConverter {

  public static Object toJava(Object obj, TypeNode nodeType) {
    if (obj == null) {
      return null;
    }
    if ((nodeType instanceof BooleanTypeNode)
        || (nodeType instanceof I8TypeNode)
        || (nodeType instanceof I32TypeNode)
        || (nodeType instanceof I64TypeNode)
        || (nodeType instanceof FP32TypeNode)
        || (nodeType instanceof FP64TypeNode)
        || (nodeType instanceof DateTypeNode)
        || (nodeType instanceof TimestampTypeNode)
        || (nodeType instanceof StringTypeNode)
        || (nodeType instanceof BinaryTypeNode)) {
      return obj;
    } else if (nodeType instanceof DecimalTypeNode) {
      return ((Decimal) obj).toJavaBigDecimal();
    } else if (nodeType instanceof ListNode) {
      ListNode listType = (ListNode) nodeType;
      /**
       * if (obj instanceof UnsafeArrayData) { UnsafeArrayData unsafeArray = (UnsafeArrayData) obj;
       * List<Object> javaList = new ArrayList<>(unsafeArray.numElements()); for (Object elem :
       * unsafeArray.array()) { javaList.add(toJava(elem, listType.getNestedType())); } return
       * javaList; } else if (obj instanceof ArrayData) {
       */
      ArrayData array = (ArrayData) obj;
      List<Object> javaList = new ArrayList<>(array.numElements());
      for (Object elem : array.array()) {
        javaList.add(toJava(elem, listType.getNestedType()));
      }
      return javaList;
    } else if (nodeType instanceof MapNode) {
      MapNode mapType = (MapNode) nodeType;
      MapData map = (MapData) obj;
      Map<Object, Object> javaMap = new HashMap<>(map.numElements());
      for (int i = 0; i < map.numElements(); i++) {
        javaMap.put(
            toJava(map.keyArray().array()[i], mapType.getKeyType()),
            toJava(map.valueArray().array()[i], mapType.getValueType()));
      }
      return javaMap;
    } else if (nodeType instanceof StructNode) {
      StructNode structType = (StructNode) nodeType;
      InternalRow struct = (InternalRow) obj;
      List<Object> javaList = new ArrayList<>(struct.numFields());
      for (int i = 0; i < struct.numFields(); i++) {
        if (struct.isNullAt(i)) {
          javaList.add(i, null);
        } else {
          TypeNode fieldType = structType.getFieldTypes().get(i);
          if (fieldType instanceof BooleanTypeNode) {
            javaList.add(i, toJava(struct.getBoolean(i), fieldType));
          } else if (fieldType instanceof I8TypeNode) {
            javaList.add(i, toJava(struct.getByte(i), fieldType));
          } else if (fieldType instanceof I16TypeNode) {
            javaList.add(i, toJava(struct.getShort(i), fieldType));
          } else if (fieldType instanceof I32TypeNode) {
            javaList.add(i, toJava(struct.getInt(i), fieldType));
          } else if (fieldType instanceof I64TypeNode) {
            javaList.add(i, toJava(struct.getLong(i), fieldType));
          } else if (fieldType instanceof FP32TypeNode) {
            javaList.add(i, toJava(struct.getFloat(i), fieldType));
          } else if (fieldType instanceof FP64TypeNode) {
            javaList.add(i, toJava(struct.getDouble(i), fieldType));
          } else if (fieldType instanceof DateTypeNode) {
            javaList.add(i, toJava(struct.getInt(i), fieldType));
          } else if (fieldType instanceof TimestampTypeNode) {
            javaList.add(i, toJava(struct.getLong(i), fieldType));
          } else if (fieldType instanceof StringTypeNode) {
            javaList.add(i, toJava(struct.getString(i), fieldType));
          } else if (fieldType instanceof BinaryTypeNode) {
            javaList.add(i, toJava(struct.getBinary(i), fieldType));
          } else if (fieldType instanceof DecimalTypeNode) {
            DecimalTypeNode decimalType = (DecimalTypeNode) fieldType;
            javaList.add(
                i,
                toJava(struct.getDecimal(i, decimalType.precision, decimalType.scale), fieldType));
          } else if (fieldType instanceof ListNode) {
            javaList.add(i, toJava(struct.getArray(i), fieldType));
          } else if (fieldType instanceof MapNode) {
            javaList.add(i, toJava(struct.getMap(i), fieldType));
          } else if (fieldType instanceof StructNode) {
            StructNode structFieldType = (StructNode) fieldType;
            javaList.add(
                i, toJava(struct.getStruct(i, structFieldType.getFieldTypes().size()), fieldType));
          } else {
            throw new UnsupportedOperationException(
                fieldType + " is not supported in StructNodeType.");
          }
        }
      }
      return javaList;
    } else {
      throw new UnsupportedOperationException(nodeType + " is not supported to convert.");
    }
  }
}
