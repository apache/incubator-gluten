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
package org.apache.gluten.vectorized;

import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.data.BaseVector;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.ArrayType;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.DoubleType;
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.MapType;
import io.github.zhztheplayer.velox4j.type.RealType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.SmallIntType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.TinyIntType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.type.VarbinaryType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.ArrayData.ElementGetter;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.gluten.util.LogicalTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converter between velox RowVector and Flink RowData. */
public class FlinkRowToVLVectorConvertor {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkRowToVLVectorConvertor.class);

    private static final Map<Class<? extends Type>, Class<? extends FieldVector>> VL_TYPE_TO_VECTOR_CLASS = new HashMap<>();

    private static final Map<Class<? extends Type>, FieldType> VL_TYPE_TO_ARROW_TYPE = new HashMap<>();

    static {
        VL_TYPE_TO_VECTOR_CLASS.put(IntegerType.class, IntVector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(BigIntType.class, BigIntVector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(SmallIntType.class, SmallIntVector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(TinyIntType.class, TinyIntVector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(BooleanType.class, BitVector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(RealType.class, Float4Vector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(DoubleType.class, Float8Vector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(TimestampType.class, TimeStampMilliVector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(VarCharType.class, VarCharVector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(VarbinaryType.class, VarBinaryVector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(RowType.class, StructVector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(ArrayType.class, ListVector.class);
        VL_TYPE_TO_VECTOR_CLASS.put(MapType.class, MapVector.class);

        VL_TYPE_TO_ARROW_TYPE.put(IntegerType.class, FieldType.nullable(MinorType.INT.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(BigIntType.class, FieldType.nullable(MinorType.BIGINT.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(SmallIntType.class, FieldType.nullable(MinorType.SMALLINT.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(TinyIntType.class, FieldType.nullable(MinorType.TINYINT.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(BooleanType.class, FieldType.nullable(MinorType.BIT.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(RealType.class, FieldType.nullable(MinorType.FLOAT4.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(DoubleType.class, FieldType.nullable(MinorType.FLOAT8.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(TimestampType.class, FieldType.nullable(MinorType.TIMESTAMPMILLI.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(VarCharType.class, FieldType.nullable(MinorType.VARCHAR.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(VarbinaryType.class, FieldType.nullable(MinorType.VARBINARY.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(RowType.class, FieldType.nullable(MinorType.STRUCT.getType()));
        VL_TYPE_TO_ARROW_TYPE.put(ArrayType.class, FieldType.nullable(MinorType.LIST.getType()));
        /// Todo: Map type may lead to memory leak, need to fix it.
        VL_TYPE_TO_ARROW_TYPE.put(MapType.class, FieldType.nullable(new ArrowType.Map(false)));
    }

    private static FieldVector createVLVector(String fieldName, Type fieldType, BufferAllocator allocator) throws Exception {
        if (fieldType instanceof RowType) {
            return StructVector.empty(fieldName, allocator);
        } else if (fieldType instanceof MapType) {
            return MapVector.empty(fieldName, allocator, false);
        } else if (fieldType instanceof ArrayType) {
            return ListVector.empty(fieldName, allocator);
        } else if (VL_TYPE_TO_VECTOR_CLASS.containsKey(fieldType.getClass())){
            Class<?> vectorClazz = VL_TYPE_TO_VECTOR_CLASS.get(fieldType.getClass());
            Constructor<?> constructor = vectorClazz.getDeclaredConstructor(String.class, BufferAllocator.class);
            return (FieldVector) constructor.newInstance(fieldName, allocator);
        } else {
            throw new RuntimeException("Unsupported type:" + fieldType);
        }
    }

    private static void fromFlinkTypedValue(Object value, String fieldName, Type fieldType, int fieldIndex, BufferAllocator allocator, FieldVector fieldVector) {
        if (value == null) {
            fieldVector.setNull(fieldIndex);
            fieldVector.setValueCount(fieldIndex + 1);
        } else if (fieldType instanceof IntegerType) {
            IntVector intVector = (IntVector) fieldVector;
            intVector.setSafe(fieldIndex, (int) value);
            intVector.setValueCount(fieldIndex + 1);
        } else if (fieldType instanceof BigIntType) {
            BigIntVector bigIntVector = (BigIntVector) fieldVector;
            bigIntVector.setSafe(fieldIndex, (long) value);
            bigIntVector.setValueCount(fieldIndex + 1);
        } else if (fieldType instanceof SmallIntType) {
            SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
            smallIntVector.setSafe(fieldIndex, (short) value);
            smallIntVector.setValueCount(fieldIndex + 1);
        } else if (fieldType instanceof TinyIntType) {
            TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
            tinyIntVector.setSafe(fieldIndex, (byte) value);
            tinyIntVector.setValueCount(fieldIndex + 1);
        } else if (fieldType instanceof RealType) {
            Float4Vector float4Vector = (Float4Vector) fieldVector;
            float4Vector.setSafe(fieldIndex, (float) value);
            float4Vector.setValueCount(fieldIndex + 1);
        } else if (fieldType instanceof DoubleType) {
            Float8Vector doubleVector = (Float8Vector) fieldVector;
            doubleVector.setSafe(fieldIndex, (double) value);
            doubleVector.setValueCount(fieldIndex + 1);
        } else if (fieldType instanceof TimestampType) {
            TimeStampMilliVector timestampVector = (TimeStampMilliVector) fieldVector;
            TimestampData timestampData = (TimestampData) value;
            timestampVector.setSafe(fieldIndex, timestampData.getMillisecond());
            timestampVector.setValueCount(fieldIndex + 1);
        } else if (fieldType instanceof VarCharType) {
            VarCharVector varCharVector = (VarCharVector) fieldVector;
            varCharVector.setSafe(fieldIndex, ((BinaryStringData) value).toBytes());
            varCharVector.setValueCount(fieldIndex + 1);
        } else if (fieldType instanceof VarbinaryType) {
            VarBinaryVector varBinaryVector = (VarBinaryVector) fieldVector;
            varBinaryVector.setSafe(fieldIndex, (byte[]) value);
            varBinaryVector.setValueCount(fieldIndex + 1);
        } else if (fieldType instanceof RowType) {
            StructVector structVector = (StructVector) fieldVector;
            RowType rowType = (RowType) fieldType;
            RowData rowData = (RowData) value;
            List<Type> subFieldTypes = rowType.getChildren();
            List<String> subFieldNames = rowType.getNames();
            for (int i = 0; i < subFieldNames.size(); ++i) {
                String subFieldName = subFieldNames.get(i);
                Type subFieldType = subFieldTypes.get(i);
                LogicalType flinkType = LogicalTypeConverter.fromVLType(subFieldType);
                FieldGetter fieldGetter = RowData.createFieldGetter(flinkType, i);
                Object fieldValue = fieldGetter.getFieldOrNull(rowData);
                FieldVector subFieldVector = 
                    structVector.addOrGet(subFieldName, VL_TYPE_TO_ARROW_TYPE.get(subFieldType.getClass()), VL_TYPE_TO_VECTOR_CLASS.get(subFieldType.getClass()));
                fromFlinkTypedValue(fieldValue, subFieldName, subFieldType, 0, allocator, subFieldVector);
            }
        } else if (fieldType instanceof MapType) {
            MapVector mapVector = (MapVector) fieldVector;
            MapType mapType = (MapType) fieldType;
            MapData mapData = (MapData) value;
            List<Type> keyValueTypes = mapType.getChildren();
            Type keyType = keyValueTypes.get(0);
            Type valueType = keyValueTypes.get(1);
            FieldVector keyVector = (FieldVector) mapVector.addOrGetVector(VL_TYPE_TO_ARROW_TYPE.get(keyType.getClass())).getVector();
            FieldVector valueVector = (FieldVector) mapVector.addOrGetVector(VL_TYPE_TO_ARROW_TYPE.get(valueType.getClass())).getVector();
            ArrayData keyArray = mapData.keyArray();
            ArrayData valueArray = mapData.valueArray();
            ElementGetter keyGetter = ArrayData.createElementGetter(LogicalTypeConverter.fromVLType(keyType));
            for (int i = 0; i < keyArray.size(); ++i) {
                fromFlinkTypedValue(keyGetter.getElementOrNull(keyArray, i), "", keyType, i, allocator, keyVector);
            }
            ElementGetter valueGetter = ArrayData.createElementGetter(LogicalTypeConverter.fromVLType(valueType));
            for (int i = 0; i < valueArray.size(); ++i) {
                fromFlinkTypedValue(valueGetter.getElementOrNull(valueArray, i), "", valueType, i, allocator, valueVector);
            }
        } else if (fieldType instanceof ArrayType) {
            ListVector arrayVector = (ListVector) fieldVector;
            ArrayType arrayType = (ArrayType) fieldType;
            ArrayData arrayData = (ArrayData) value;
            Type elementType = arrayType.getChildren().get(0);
            FieldVector elementVector = (FieldVector) arrayVector.addOrGetVector(VL_TYPE_TO_ARROW_TYPE.get(elementType.getClass())).getVector();
            ElementGetter elementGetter = ArrayData.createElementGetter(LogicalTypeConverter.fromVLType(elementType));
            for (int i = 0; i < arrayData.size(); ++i) {
                fromFlinkTypedValue(elementGetter.getElementOrNull(arrayData, i), "", elementType, i, allocator, elementVector);
            }
        } else {
            throw new RuntimeException("Unsupported field type:" + fieldType);
        }
    }

    public static RowVector fromRowData(
            RowData row,
            BufferAllocator allocator,
            Session session,
            RowType rowType) {
        List<FieldVector> arrowVectors = new ArrayList<>(rowType.size());
        try {
            for (int i = 0; i < rowType.size(); i++) {
                Type fieldType = rowType.getChildren().get(i);
                String fieldName = rowType.getNames().get(i);
                LogicalType flinkType = LogicalTypeConverter.fromVLType(fieldType);
                FieldGetter fieldGetter = RowData.createFieldGetter(flinkType, i);
                Object fieldValue = fieldGetter.getFieldOrNull(row);
                FieldVector fieldVector = createVLVector(fieldName, fieldType, allocator);
                fromFlinkTypedValue(fieldValue, fieldName, fieldType, 0, allocator, fieldVector);
                arrowVectors.add(fieldVector);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Table arrowTable = new Table(arrowVectors);
        RowVector res = session.arrowOps().fromArrowTable(allocator, arrowTable);
        return res;
    }

    private static Object toFlinkTypedValue(ValueVector fieldVector, Type fieldType, int fieldIndex) {
        if (fieldVector.isNull(fieldIndex)) {
            return null;
        } else if (fieldVector instanceof IntVector) {
            return ((IntVector) fieldVector).get(fieldIndex);
        } else if (fieldVector instanceof BigIntVector) {
            return ((BigIntVector) fieldVector).get(fieldIndex);
        } else if (fieldVector instanceof SmallIntVector) {
            return ((SmallIntVector) fieldVector).get(fieldIndex);
        } else if (fieldVector instanceof TinyIntVector) {
            return ((TinyIntVector) fieldVector).get(fieldIndex);
        } else if (fieldVector instanceof BitVector) {
            int b = ((BitVector) fieldVector).get(fieldIndex);
            return b == 0 ? false : true;
        } else if (fieldVector instanceof Float4Vector) {
            return ((Float4Vector) fieldVector).get(fieldIndex);
        } else if (fieldVector instanceof Float8Vector) {
            return ((Float8Vector) fieldVector).get(fieldIndex);
        } else if (fieldVector instanceof TimeStampVector) {
            /// Todo: consider about timezone 
            long timestamp = ((TimeStampVector) fieldVector).get(fieldIndex);
            return TimestampData.fromEpochMillis(timestamp);
        } else if (fieldVector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) fieldVector).get(fieldIndex);
            BinaryStringData s = BinaryStringData.fromBytes(bytes);
            return s;
        } else if (fieldVector instanceof VarBinaryVector) {
            byte[] bytes = ((VarBinaryVector) fieldVector).get(fieldIndex);
            return BinaryStringData.fromBytes(bytes);
        } else if (fieldVector instanceof StructVector) {
            StructVector rowVector = (StructVector) fieldVector;
            RowType rowType = (RowType) fieldType;
            List<String> rowFieldNames = rowType.getNames();
            List<Type> rowFieldTypes = rowType.getChildren();
            List<Object> fValues = new ArrayList<>();
            for (int i = 0; i < rowFieldNames.size(); ++i) {
                FieldVector f = rowVector.getChild(rowFieldNames.get(i));
                Object fValue = toFlinkTypedValue(f, rowFieldTypes.get(i), fieldIndex);
                fValues.add(fValue);
            }
            return GenericRowData.of(fValues.toArray());
        } else if (fieldVector instanceof MapVector) {
            MapVector mapVector = (MapVector) fieldVector;
            MapType mapType = (MapType) fieldType;
            Type keyType = mapType.getChildren().get(0);
            Type valueType = mapType.getChildren().get(1);
            StructVector structVector = (StructVector) mapVector.getDataVector();
            ValueVector keyVector = structVector.getChildByOrdinal(0);
            ValueVector valueVector = structVector.getChildByOrdinal(1);
            Map m = new HashMap<>();
            for (int i = 0; i < keyVector.getValueCount(); ++i) {
                Object key = toFlinkTypedValue(keyVector, keyType, i);
                Object value = toFlinkTypedValue(valueVector, valueType, i);
                m.put(key, value);
            }
            return new GenericMapData(m);
        } else if (fieldVector instanceof ListVector) {
            ListVector listVector = (ListVector) fieldVector;
            ArrayType arrayType = (ArrayType) fieldType;
            Type elementType = arrayType.getChildren().get(0);
            List<Object> arrayDatas = new ArrayList<>();
            FieldVector dataVector = listVector.getDataVector();
            for (int i = 0; i < dataVector.getValueCount(); ++i) {
                Object value = toFlinkTypedValue(dataVector, elementType, i);
                arrayDatas.add(value);
            }
            return new GenericArrayData(arrayDatas.toArray());
        }
        else {
            throw new RuntimeException("Unsupported field type:" + fieldType);
        }
    }
    public static List<RowData> toRowData(
            RowVector rowVector,
            BufferAllocator allocator,
            RowType rowType) {
        if (rowVector.getSize() == 0) {
            return new ArrayList<>();
        }
        final BaseVector loadedVector = rowVector.loadedVector();
        final FieldVector valueVector = Arrow.toArrowVector(
                allocator,
                loadedVector);
        List<RowData> rowDatas = new ArrayList<>(rowVector.getSize());
        for (int j = 0; j < rowVector.getSize(); j++) {
            List<Object> fieldValues = new ArrayList<>(rowType.size());
            for (int i = 0; i < rowType.size(); i++) {
                Type fieldType = rowType.getChildren().get(i);
                FieldVector fieldVector = valueVector.getChildrenFromFields().get(i);
                fieldValues.add(toFlinkTypedValue(fieldVector, fieldType, j));
            }
            rowDatas.add(GenericRowData.of(fieldValues.toArray()));
        }
        valueVector.close();
        loadedVector.close();
        return rowDatas;
    }

}
