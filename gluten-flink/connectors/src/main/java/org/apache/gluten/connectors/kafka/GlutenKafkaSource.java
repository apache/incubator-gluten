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

 import java.util.Properties;
 
 import org.apache.flink.api.common.typeinfo.TypeInformation;
 import org.apache.flink.api.connector.source.Boundedness;
 import org.apache.flink.api.connector.source.Source;
 import org.apache.flink.api.connector.source.SourceReader;
 import org.apache.flink.api.connector.source.SourceReaderContext;
 import org.apache.flink.api.connector.source.SplitEnumerator;
 import org.apache.flink.api.connector.source.SplitEnumeratorContext;
 import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
 import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
 import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumStateSerializer;
 import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
 import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
 import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
 import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
 import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
 import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
 import org.apache.flink.core.io.SimpleVersionedSerializer;
 import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
 import org.apache.flink.table.types.DataType;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;
 
 public class GlutenKafkaSource<OUT> implements Source<OUT, KafkaPartitionSplit, KafkaSourceEnumState>, ResultTypeQueryable<OUT> {
 
   private static final Logger LOG = LoggerFactory.getLogger(GlutenKafkaSource.class);
 
   private final Properties properties;
 
   private final String format;
 
   private final DataType outputType;
 
   private final KafkaRecordDeserializationSchema<OUT> deserializationSchema;
 
   private final KafkaSubscriber subscriber;
 
   private final OffsetsInitializer startingOffsetsInitializer;
 
   private final OffsetsInitializer stoppingOffsetsInitializer;
 
   private String planNodeId;
 
   public GlutenKafkaSource(
     String planNodeId,
     String format,
     Properties props,
     DataType outpuType,
     KafkaRecordDeserializationSchema<OUT> deserializationSchema,
     KafkaSubscriber subscriber,
     OffsetsInitializer startOffsetsInitializer,
     OffsetsInitializer stopOffsetsInitializer) {
     this.planNodeId = planNodeId;
     this.format = format;
     this.properties = props;
     this.outputType = outpuType;
     this.deserializationSchema = deserializationSchema;
     this.subscriber = subscriber;
     this.startingOffsetsInitializer = startOffsetsInitializer;
     this.stoppingOffsetsInitializer = stopOffsetsInitializer;
   }
 
   @Override
   public Boundedness getBoundedness() {
       return Boundedness.CONTINUOUS_UNBOUNDED;
   }
 
   @Override
   public SourceReader<OUT, KafkaPartitionSplit> createReader(SourceReaderContext readerContext) throws Exception {
     return new GlutenKafkaSourceReader<OUT>(planNodeId, format, outputType, properties);
   }
 
   @Override
   public TypeInformation<OUT> getProducedType() {
     return deserializationSchema.getProducedType();
   }
 
   public void setPlanNodeId(String planNodeId) {
     this.planNodeId = planNodeId;
   }
 
   public void setEnableAutoCommitOffset(boolean enabled) {
     this.properties.setProperty("enable.auto.commit", String.valueOf(enabled));
   }
 
   public void setStartupMode(StartupMode mode) {
     switch(mode) {
       case GROUP_OFFSETS :
         this.properties.setProperty("scan.startup.mode", "group-offsets");
         break;
       case LATEST:
         this.properties.setProperty("scan.startup.mode", "latest-offsets");
         break;
       case EARLIEST:
         this.properties.setProperty("scan.startup.mode", "earliest-offsets");
         break;
       default:
         this.properties.setProperty("scan.startup.mode", "group-offsets");
         break;
     }
   }
 
   @Override
   public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(
       SplitEnumeratorContext<KafkaPartitionSplit> enumContext) throws Exception {
     return new KafkaSourceEnumerator(
                 subscriber,
                 startingOffsetsInitializer,
                 stoppingOffsetsInitializer,
                 properties,
                 enumContext,
                 getBoundedness());
   }
 
   @Override
   public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(
       SplitEnumeratorContext<KafkaPartitionSplit> enumContext, KafkaSourceEnumState checkpoint) throws Exception {
     return new KafkaSourceEnumerator(
           subscriber,
           startingOffsetsInitializer,
           stoppingOffsetsInitializer,
           properties,
           enumContext,
           getBoundedness(),
           checkpoint);
   }
 
   @Override
   public SimpleVersionedSerializer<KafkaPartitionSplit> getSplitSerializer() {
     return new KafkaPartitionSplitSerializer();
   }
 
   @Override
   public SimpleVersionedSerializer<KafkaSourceEnumState> getEnumeratorCheckpointSerializer() {
     return new KafkaSourceEnumStateSerializer();
   }
 
 }