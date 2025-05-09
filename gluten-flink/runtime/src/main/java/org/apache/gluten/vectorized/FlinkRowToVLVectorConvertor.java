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
import io.github.zhztheplayer.velox4j.type.IntegerType;
import io.github.zhztheplayer.velox4j.type.MapType;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
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
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converter between velox RowVector and Flink RowData. */
public class FlinkRowToVLVectorConvertor {

    public static RowVector fromRowData(
            RowData row,
            BufferAllocator allocator,
            Session session,
            RowType rowType) {
        // TODO: support more types
        List<FieldVector> arrowVectors = new ArrayList<>(rowType.size());
        for (int i = 0; i < rowType.size(); i++) {
            Type fieldType = rowType.getChildren().get(i);
            if (fieldType instanceof IntegerType) {
                IntVector intVector = new IntVector(rowType.getNames().get(i), allocator);
                intVector.setSafe(0, row.getInt(i));
                intVector.setValueCount(1);
                arrowVectors.add(i, intVector);
            } else if (fieldType instanceof BigIntType) {
                BigIntVector bigIntVector = new BigIntVector(rowType.getNames().get(i), allocator);
                bigIntVector.setSafe(0, row.getLong(i));
                bigIntVector.setValueCount(1);
                arrowVectors.add(i, bigIntVector);
            } else if (fieldType instanceof VarCharType) {
                VarCharVector stringVector = new VarCharVector(rowType.getNames().get(i), allocator);
                stringVector.setSafe(0, row.getString(i).toBytes());
                stringVector.setValueCount(1);
                arrowVectors.add(i, stringVector);
            } else if (fieldType instanceof RowType) {
                // TODO: refine this
                StructVector structVector =
                        StructVector.empty(
                                rowType.getNames().get(i),
                                allocator);
                RowType subRowType = (RowType) fieldType;
                RowData subRow = row.getRow(i, subRowType.size());
                if (subRow != null) {
                    for (int j = 0; j < subRowType.size(); j++) {
                        Type subFieldType = subRowType.getChildren().get(j);
                        if (subFieldType instanceof IntegerType) {
                            IntVector intVector = structVector.addOrGet(
                                    subRowType.getNames().get(j),
                                    FieldType.nullable(MinorType.INT.getType()),
                                    IntVector.class);
                            intVector.setSafe(0, subRow.getInt(j));
                            intVector.setValueCount(1);
                        } else if (subFieldType instanceof BigIntType) {
                            BigIntVector bigIntVector = structVector.addOrGet(
                                    subRowType.getNames().get(j),
                                    FieldType.nullable(MinorType.BIGINT.getType()),
                                    BigIntVector.class);
                            bigIntVector.setSafe(0, subRow.getLong(j));
                            bigIntVector.setValueCount(1);
                        } else if (subFieldType instanceof VarCharType) {
                            VarCharVector stringVector = structVector.addOrGet(
                                    subRowType.getNames().get(j),
                                    FieldType.nullable(MinorType.VARCHAR.getType()),
                                    VarCharVector.class);
                            stringVector.setSafe(0, subRow.getString(j).toBytes());
                            stringVector.setValueCount(1);
                        } else if (subFieldType instanceof TimestampType) {
                            // TODO: support precision
                            TimeStampMilliVector timestampVector = structVector.addOrGet(
                                    subRowType.getNames().get(j),
                                    FieldType.nullable(MinorType.TIMESTAMPMILLI.getType()),
                                    TimeStampMilliVector.class);
                            timestampVector.setSafe(
                                    0,
                                    subRow.getTimestamp(j, 3).getMillisecond());
                            timestampVector.setValueCount(1);
                        } else {
                            throw new RuntimeException("Unsupported field type: " + subFieldType);
                        }
                    }
                    structVector.setValueCount(1);
                }
                arrowVectors.add(i, structVector);
            } else {
                throw new RuntimeException("Unsupported field type: " + fieldType);
            }
        }
        return session.arrowOps().fromArrowTable(allocator, new Table(arrowVectors));
    }

    private static Object toFlinkTypedValue(ValueVector fieldVector, Type fieldType, int fieldIndex) {
        if (fieldVector instanceof IntVector) {
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
        // TODO: support more types
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
