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
package org.apache.gluten.connector.write;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.DecimalType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartitionDataJson implements StructLike {
  private static final String PARTITION_VALUES_FIELD = "partitionValues";
  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER =
      new ObjectMapper(FACTORY).configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

  private final Object[] partitionValues;

  public PartitionDataJson(Object[] partitionValues) {
    this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
  }

  @Override
  public int size() {
    return partitionValues.length;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    Object value = partitionValues[pos];

    if (javaClass == ByteBuffer.class && value instanceof byte[]) {
      value = ByteBuffer.wrap((byte[]) value);
    }

    if (value == null || javaClass.isInstance(value)) {
      return javaClass.cast(value);
    }

    throw new IllegalArgumentException(
        format(
            "Wrong class [%s] for object class [%s]",
            javaClass.getName(), value.getClass().getName()));
  }

  @Override
  public <T> void set(int pos, T value) {
    partitionValues[pos] = value;
  }

  public static PartitionDataJson fromJson(
      String partitionDataAsJson, PartitionSpec partitionSpec) {
    if (partitionDataAsJson == null) {
      return null;
    }

    JsonNode jsonNode;
    try {
      jsonNode = MAPPER.readTree(partitionDataAsJson);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Conversion from JSON failed for PartitionData: " + partitionDataAsJson, e);
    }
    if (jsonNode.isNull()) {
      return null;
    }

    JsonNode partitionValues = jsonNode.get(PARTITION_VALUES_FIELD);
    Object[] objects = new Object[partitionSpec.fields().size()];
    int index = 0;
    for (JsonNode partitionValue : partitionValues) {
      Type partionType = partitionSpec.partitionType().fields().get(index).type();
      objects[index] = getValue(partitionValue, partionType);
      index++;
    }
    return new PartitionDataJson(objects);
  }

  public static Object getValue(JsonNode partitionValue, Type type) {
    if (partitionValue.isNull()) {
      return null;
    }
    switch (type.typeId()) {
      case BOOLEAN:
        return partitionValue.asBoolean();
      case INTEGER:
      case DATE:
        return partitionValue.asInt();
      case LONG:
      case TIMESTAMP:
      case TIME:
        return partitionValue.asLong();
      case FLOAT:
        if (partitionValue.asText().equalsIgnoreCase("NaN")) {
          return Float.NaN;
        }
        if (partitionValue.asText().equalsIgnoreCase("Infinity")) {
          return Float.POSITIVE_INFINITY;
        }
        if (partitionValue.asText().equalsIgnoreCase("-Infinity")) {
          return Float.NEGATIVE_INFINITY;
        }
        return partitionValue.floatValue();
      case DOUBLE:
        if (partitionValue.asText().equalsIgnoreCase("NaN")) {
          return Double.NaN;
        }
        if (partitionValue.asText().equalsIgnoreCase("Infinity")) {
          return Double.POSITIVE_INFINITY;
        }
        if (partitionValue.asText().equalsIgnoreCase("-Infinity")) {
          return Double.NEGATIVE_INFINITY;
        }
        return partitionValue.doubleValue();
      case STRING:
        return partitionValue.asText();
      case FIXED:
      case BINARY:
        try {
          return partitionValue.binaryValue();
        } catch (IOException e) {
          throw new UncheckedIOException("Failed during JSON conversion of " + partitionValue, e);
        }
      case DECIMAL:
        return partitionValue.decimalValue().setScale(((DecimalType) type).scale());
    }
    throw new UnsupportedOperationException("Type not supported as partition column: " + type);
  }
}
