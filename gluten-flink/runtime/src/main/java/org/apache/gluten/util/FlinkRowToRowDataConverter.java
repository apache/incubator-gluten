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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class FlinkRowToRowDataConverter {

  @SuppressWarnings("unchecked")
  private static DataStructureConverter<?, ?> createFieldConverter(DataType fieldType) {
    DataStructureConverter<?, ?> fieldConverter = null;
    LogicalType logicalType = fieldType.getLogicalType();
    switch (logicalType.getTypeRoot()) {
      case ARRAY:
        CollectionDataType arrayDataType = (CollectionDataType) fieldType;
        DataType elementType = arrayDataType.getElementDataType();
        if (String.class.isAssignableFrom(elementType.getConversionClass())) {
          fieldConverter =
              new DataStructureConverter<ArrayData, String[]>() {
                @Override
                public ArrayData toInternal(String[] external) {
                  StringData[] strings = new StringData[external.length];
                  for (int i = 0; i < external.length; ++i) {
                    strings[i] = StringData.fromString(external[i]);
                  }
                  return new GenericArrayData(strings);
                }

                @Override
                public String[] toExternal(ArrayData internal) {
                  String[] strs = new String[internal.size()];
                  for (int i = 0; i < internal.size(); ++i) {
                    strs[i] = internal.getString(i).toString();
                  }
                  return strs;
                }
              };
        } else if (Row.class.isAssignableFrom(elementType.getConversionClass())) {
          final RowRowConverter elementConverter = create(elementType);
          final int numFields = ((FieldsDataType) elementType).getChildren().size();
          fieldConverter =
              new DataStructureConverter<ArrayData, Row[]>() {
                @Override
                public ArrayData toInternal(Row[] external) {
                  RowData[] rowDatas = new RowData[external.length];
                  for (int i = 0; i < external.length; ++i) {
                    rowDatas[i] = elementConverter.toInternal(external[i]);
                  }
                  return new GenericArrayData(rowDatas);
                }

                @Override
                public Row[] toExternal(ArrayData internal) {
                  Row[] rows = new Row[internal.size()];
                  for (int i = 0; i < internal.size(); ++i) {
                    rows[i] = elementConverter.toExternal(internal.getRow(i, numFields));
                  }
                  return rows;
                }
              };
        } else if (Object[].class.isAssignableFrom(elementType.getConversionClass())) {
          final DataStructureConverter<ArrayData, Object> elementConverter =
              (DataStructureConverter<ArrayData, Object>) createFieldConverter(elementType);
          fieldConverter =
              new DataStructureConverter<ArrayData, Object[]>() {
                @Override
                public ArrayData toInternal(Object[] external) {
                  ArrayData[] arrays = new ArrayData[external.length];
                  for (int i = 0; i < external.length; ++i) {
                    arrays[i] = (ArrayData) elementConverter.toInternal(external[i]);
                  }
                  return new GenericArrayData(arrays);
                }

                @Override
                public Object[] toExternal(ArrayData internal) {
                  Object[] arrays = new Object[internal.size()];
                  for (int i = 0; i < internal.size(); ++i) {
                    ArrayData subArray = internal.getArray(i);
                    arrays[i] = elementConverter.toExternal(subArray);
                  }
                  return arrays;
                }
              };
        } else if (Map.class.isAssignableFrom(elementType.getConversionClass())) {
          final DataStructureConverter<MapData, Map> elementConverter =
              (DataStructureConverter<MapData, Map>) createFieldConverter(elementType);
          fieldConverter =
              new DataStructureConverter<ArrayData, Map[]>() {

                @Override
                public ArrayData toInternal(Map[] external) {
                  MapData[] mapDatas = new MapData[external.length];
                  for (int i = 0; i < external.length; ++i) {
                    mapDatas[i] = (MapData) elementConverter.toInternal(external[i]);
                  }
                  return new GenericArrayData(mapDatas);
                }

                @Override
                public Map[] toExternal(ArrayData internal) {
                  Map[] maps = new Map[internal.size()];
                  for (int i = 0; i < internal.size(); ++i) {
                    maps[i] = (Map) elementConverter.toExternal(internal.getMap(i));
                  }
                  return maps;
                }
              };
        } else {
          fieldConverter = DataStructureConverters.getConverter(fieldType);
        }
        break;
      case MAP:
        KeyValueDataType keyValueDataType = (KeyValueDataType) fieldType;
        DataType keyType = keyValueDataType.getKeyDataType();
        DataType valueType = keyValueDataType.getValueDataType();
        final DataStructureConverter<Object, Object> keyConverter =
            (DataStructureConverter<Object, Object>) createFieldConverter(keyType);
        final DataStructureConverter<Object, Object> valueConverter =
            (DataStructureConverter<Object, Object>) createFieldConverter(valueType);
        fieldConverter =
            new DataStructureConverter<MapData, Map>() {
              @Override
              public MapData toInternal(Map external) {
                Map<Object, Object> kvs = new LinkedHashMap<>();
                for (Object key : external.keySet()) {
                  Object newKey = keyConverter.toInternal(key);
                  Object newValue = valueConverter.toInternal(external.get(key));
                  kvs.put(newKey, newValue);
                }
                return new GenericMapData(kvs);
              }

              @Override
              public Map toExternal(MapData internal) {
                Map<Object, Object> map = new LinkedHashMap<>();
                Object[] keys = ((GenericArrayData) internal.keyArray()).toObjectArray();
                Object[] values = ((GenericArrayData) internal.valueArray()).toObjectArray();
                for (int i = 0; i < keys.length; ++i) {
                  map.put(keys[i], values[i]);
                }
                return map;
              }
            };
        break;
      default:
        fieldConverter = DataStructureConverters.getConverter(fieldType);
    }
    return fieldConverter;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static RowRowConverter create(DataType dataType) {
    final List<DataType> fields = dataType.getChildren();
    final List<DataStructureConverter<Object, Object>> fieldConverters = new ArrayList<>();
    for (DataType fieldType : fields) {
      DataStructureConverter fieldConverter = createFieldConverter(fieldType);
      if (fieldConverter != null) {
        fieldConverters.add(fieldConverter);
      } else {
        throw new FlinkRuntimeException(
            "Failed to create converter for data type:" + fieldType.toString());
      }
    }
    final RowData.FieldGetter[] fieldGetters =
        IntStream.range(0, fields.size())
            .mapToObj(pos -> RowData.createFieldGetter(fields.get(pos).getLogicalType(), pos))
            .toArray(RowData.FieldGetter[]::new);
    final List<String> fieldNames = ((RowType) dataType.getLogicalType()).getFieldNames();
    final LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>();
    for (int i = 0; i < fieldNames.size(); i++) {
      positionByName.put(fieldNames.get(i), i);
    }
    return (RowRowConverter)
        ReflectUtils.invokeConstructor(
            RowRowConverter.class,
            new Class<?>[] {
              DataStructureConverter[].class, RowData.FieldGetter[].class, LinkedHashMap.class
            },
            new Object[] {
              fieldConverters.toArray(DataStructureConverter[]::new), fieldGetters, positionByName
            });
  }
}
