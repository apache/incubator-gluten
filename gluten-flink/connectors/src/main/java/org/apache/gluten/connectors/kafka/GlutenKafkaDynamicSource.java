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

package org.apache.gluten.connectors.kafka;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.config.BoundedMode;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.formats.raw.RawFormatDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlutenKafkaDynamicSource extends KafkaDynamicSource{

    private static final Logger LOG = LoggerFactory.getLogger(GlutenKafkaDynamicSource.class);

    public GlutenKafkaDynamicSource(DataType physicalDataType,
                                    DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
                                    DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
                                    int[] keyProjection,
                                    int[] valueProjection,
                                    String keyPrefix,
                                    List<String> topics,
                                    Pattern topicPattern,
                                    Properties properties,
                                    StartupMode startupMode,
                                    Map<KafkaTopicPartition, Long> specificStartupOffsets,
                                    long startupTimestampMillis,
                                    BoundedMode boundedMode,
                                    Map<KafkaTopicPartition, Long> specificBoundedOffsets,
                                    long boundedTimestampMillis,
                                    boolean upsertMode,
                                    String tableIdentifier) {
        super(physicalDataType, keyDecodingFormat, valueDecodingFormat, keyProjection, valueProjection, keyPrefix, topics,
                topicPattern, properties, startupMode, specificStartupOffsets, startupTimestampMillis, boundedMode,
                specificBoundedOffsets, boundedTimestampMillis, upsertMode, tableIdentifier);
    }

    public static GlutenKafkaDynamicSource copyFrom(KafkaDynamicSource source) throws Exception {
        final GlutenKafkaDynamicSource copy =
                new GlutenKafkaDynamicSource(
                        getFieldFromKafkaSource(source, "physicalDataType", DataType.class),
                        getFieldFromKafkaSource(source, "keyDecodingFormat", DecodingFormat.class),
                        getFieldFromKafkaSource(source, "valueDecodingFormat", DecodingFormat.class),
                        getFieldFromKafkaSource(source, "keyProjection", int[].class),
                        getFieldFromKafkaSource(source, "valueProjection", int[].class),
                        getFieldFromKafkaSource(source, "keyPrefix", String.class),
                        getFieldFromKafkaSource(source, "topics", List.class),
                        getFieldFromKafkaSource(source, "topicPattern", Pattern.class),
                        getFieldFromKafkaSource(source, "properties", Properties.class),
                        getFieldFromKafkaSource(source, "startupMode", StartupMode.class),
                        getFieldFromKafkaSource(source, "specificStartupOffsets", Map.class),
                        getFieldFromKafkaSource(source, "startupTimestampMillis", long.class),
                        getFieldFromKafkaSource(source, "boundedMode", BoundedMode.class),
                        getFieldFromKafkaSource(source, "specificBoundedOffsets", Map.class),
                        getFieldFromKafkaSource(source, "boundedTimestampMillis", long.class),
                        getFieldFromKafkaSource(source, "upsertMode", boolean.class),
                        getFieldFromKafkaSource(source, "tableIdentifier", String.class));
        copy.producedDataType = getFieldFromKafkaSource(source, "producedDataType", DataType.class);
        copy.metadataKeys = getFieldFromKafkaSource(source, "metadataKeys", List.class);
        // copy.watermarkStrategy = getFieldFromKafkaSource(source, "watermarkStrategy", WatermarkStrategy.class);
        copy.watermarkStrategy = WatermarkStrategy.noWatermarks();
        return copy;
    }

    @Override
    public DynamicTableSource copy() {
        try {
            final DynamicTableSource source = super.copy();
            return copyFrom((KafkaDynamicSource) source);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> T getFieldValue(Object source, Class<?> sourceClazz, String fieldName, Class<T> fieldClazz) {
        try {
            Field f = sourceClazz.getDeclaredField(fieldName);
            f.setAccessible(true);
            Class<?> fClazz = f.getType();
            if (fClazz.equals(fieldClazz)) {
                return (T) f.get(source);
            } else {
                String errMsg = String.format("Faield to get field %s from KafkaDynamimcSource,  the field class {} not match with given class: {}",
                        fieldName, fClazz.getName(), fieldClazz.getName());
                throw new RuntimeException(errMsg);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> T getFieldFromKafkaSource(KafkaDynamicSource source, String fieldName, Class<T> fieldClazz) {
        return getFieldValue(source, KafkaDynamicSource.class, fieldName, fieldClazz);
    }

    private static <T> T getFieldFromKafkaSource(KafkaSource<?> source, String fieldName, Class<T> fieldClazz) {
        return getFieldValue(source, KafkaSource.class, fieldName, fieldClazz);
    }

    private static <T> T invokeMethodFromKafkaSource(KafkaDynamicSource source, String methodName, Class<?>[] argClazzs, Object[] args) {
        try {
            Method m = KafkaDynamicSource.class.getDeclaredMethod(methodName, argClazzs);
            if (m != null) {
                m.setAccessible(true);
                return (T) m.invoke(source, args);
            } else {
                throw new RuntimeException("Failed to get method name of: " + methodName);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String getFormat(DeserializationSchema<RowData> deserializationSchema) {
        String deserializationSchemaClazzName = deserializationSchema.getClass().getSimpleName();
        if (deserializationSchema instanceof RawFormatDeserializationSchema) {
            return "raw";
        } else if (deserializationSchemaClazzName.equals("JsonParserRowDataDeserializationSchema")) {
            return "json";
        } else {
            throw new RuntimeException("Format not supported:" + deserializationSchema.getClass().getName());
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final DeserializationSchema<RowData> keyDeserialization = invokeMethodFromKafkaSource(this,
                "createDeserialization",
                new Class<?>[] {
                        DynamicTableSource.Context.class,
                        DecodingFormat.class,
                        int[].class,
                        String.class
                },
                new Object[]{context,
                        keyDecodingFormat,
                        keyProjection,
                        keyPrefix});
        final DeserializationSchema<RowData> valueDeserialization = invokeMethodFromKafkaSource(this,
                "createDeserialization",
                new Class<?>[] {
                        DynamicTableSource.Context.class,
                        DecodingFormat.class,
                        int[].class,
                        String.class
                },
                new Object[]{
                        context,
                        valueDecodingFormat,
                        valueProjection,
                        null
                });
        final TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(producedDataType);
        final KafkaRecordDeserializationSchema<RowData> deserializationSchema =
                KafkaRecordDeserializationSchema.of(invokeMethodFromKafkaSource(this,
                        "createKafkaDeserializationSchema",
                        new Class<?>[] {
                                DeserializationSchema.class,
                                DeserializationSchema.class,
                                TypeInformation.class
                        },
                        new Object[] {
                                keyDeserialization,
                                valueDeserialization,
                                producedTypeInfo
                        }));
        final KafkaSource<RowData> kafkaSource = createKafkaSource(keyDeserialization, valueDeserialization, producedTypeInfo);
        KafkaSubscriber subscriber = getFieldFromKafkaSource(kafkaSource, "subscriber", KafkaSubscriber.class);
        OffsetsInitializer startOffsetsInitializer = getFieldFromKafkaSource(kafkaSource,
                "startingOffsetsInitializer", OffsetsInitializer.class);
        OffsetsInitializer stopOffsetsInitializer = getFieldFromKafkaSource(kafkaSource,
                "stoppingOffsetsInitializer", OffsetsInitializer.class);
        final GlutenKafkaSource<RowData> glutenKafkaSource =
                new GlutenKafkaSource<RowData>("",
                        getFormat(valueDeserialization),
                        properties,
                        producedDataType,
                        deserializationSchema,
                        subscriber,
                        startOffsetsInitializer,
                        stopOffsetsInitializer);
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                if (watermarkStrategy == null) {
                   watermarkStrategy = WatermarkStrategy.noWatermarks();
                }
                DataStreamSource<RowData> sourceStream = execEnv.fromSource(glutenKafkaSource, watermarkStrategy, "kafkasource-" + tableIdentifier);
                glutenKafkaSource.setPlanNodeId(String.valueOf(sourceStream.getTransformation().getId()));
                glutenKafkaSource.setEnableAutoCommitOffset(!execEnv.getCheckpointConfig().isCheckpointingEnabled());
                glutenKafkaSource.setStartupMode(startupMode);
                providerContext.generateUid("kafka").ifPresent(sourceStream::uid);
                return sourceStream;
            }

            @Override
            public boolean isBounded() {
                return false;
            }
        };
    }

}
