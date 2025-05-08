/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package org.apache.gluten.connectors.kafka;

 import java.util.concurrent.CompletableFuture;
 import java.util.stream.Collectors;
 
 import org.apache.arrow.memory.BufferAllocator;
 import org.apache.arrow.memory.RootAllocator;
 import org.apache.flink.api.connector.source.ReaderOutput;
 import org.apache.flink.api.connector.source.SourceReader;
 import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
 import org.apache.flink.core.io.InputStatus;
 import org.apache.flink.table.data.RowData;
 import org.apache.flink.table.types.DataType;
 import org.apache.gluten.util.LogicalTypeConverter;
 import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;
 
 import io.github.zhztheplayer.velox4j.Velox4j;
 import io.github.zhztheplayer.velox4j.config.Config;
 import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
 import io.github.zhztheplayer.velox4j.connector.KafkaConnectorSplit;
 import io.github.zhztheplayer.velox4j.connector.KafkaTableHandle;
 import io.github.zhztheplayer.velox4j.data.RowVector;
 import io.github.zhztheplayer.velox4j.iterator.CloseableIterator;
 import io.github.zhztheplayer.velox4j.iterator.UpIterators;
 import io.github.zhztheplayer.velox4j.memory.AllocationListener;
 import io.github.zhztheplayer.velox4j.memory.MemoryManager;
 import io.github.zhztheplayer.velox4j.plan.TableScanNode;
 import io.github.zhztheplayer.velox4j.query.BoundSplit;
 import io.github.zhztheplayer.velox4j.query.Query;
 import io.github.zhztheplayer.velox4j.session.Session;
 
 import java.util.ArrayList;
 import java.util.HashMap;
 import java.util.List;
 import java.util.Map;
 import java.util.Properties;
 import java.util.UUID;
 
 /**
  * Kafka reader to consume kafka messages use native cpp consumer.
  */
 public class GlutenKafkaSourceReader<T> implements SourceReader<T, KafkaPartitionSplit> {
 
   private static final Logger LOG = LoggerFactory.getLogger(GlutenKafkaSourceReader.class);
 
   private static final String CONNECTOR_ID = "connector-kafka";
 
   private static final String KEY_ENABLE_AUTO_COMMIT = "enable.auto.commit";
 
   private static final String KEY_AUTO_OFFSET_RESET = "auto.offset.reset";
 
   private static final String KEY_STARTUP_MODE = "scan.startup.mode";
 
   private final Properties props;
 
   private final String planNodeId;
 
   private final String format;
 
   private final MemoryManager memoryManager;

   private final Session session;

   private final BufferAllocator allocator;
 
   private final DataType outputType;
 
   private final io.github.zhztheplayer.velox4j.type.Type veloxOutputType;
 
   private final List<KafkaPartitionSplit> topicPartitions;
 
   private Query query;
 
   private CloseableIterator<RowVector> result;
 
   private boolean running = false;
 
   public GlutenKafkaSourceReader(
       String planNodeId,
       String format,
       DataType outputType,
       Properties props) {
     this.planNodeId = planNodeId;
     this.format = format;
     this.props = props;
     this.outputType = outputType;
     this.veloxOutputType = LogicalTypeConverter.toVLType(outputType.getLogicalType());
     this.topicPartitions = new ArrayList<>();
     this.memoryManager = MemoryManager.create(AllocationListener.NOOP);
     this.session = Velox4j.newSession(memoryManager);
     this.allocator = new RootAllocator(Long.MAX_VALUE);
   }
 
   private KafkaConnectorSplit getConnectionSplit(Properties props) {
     String bootstrapServers = props.getProperty("bootstrap.servers");
     String groupId = props.getProperty("group.id");
     boolean enableAutoCommit = Boolean.valueOf(props.getProperty(KEY_ENABLE_AUTO_COMMIT, "true"));
     String autoResetOffset = props.getProperty(KEY_AUTO_OFFSET_RESET, "earliest");
     Map<String, List<Integer>> topicPartitionsMap = new HashMap<>();
     if (!topicPartitions.isEmpty()) {
       for (KafkaPartitionSplit tp : topicPartitions) {
         String topic = tp.getTopic();
         List<Integer> partitions = topicPartitionsMap.computeIfAbsent(topic, x -> new ArrayList<>());
         partitions.add(tp.getPartition());
       }
     }
     KafkaConnectorSplit connectorSplit = new KafkaConnectorSplit(
       CONNECTOR_ID, 
       0, 
       false, 
       bootstrapServers,
       groupId,
       format,
       enableAutoCommit,
       autoResetOffset,
       topicPartitionsMap);
     return connectorSplit;
   }
 
   private KafkaTableHandle getTableHandle() {
     if (topicPartitions.isEmpty()) {
       throw new RuntimeException("Failed to get table handle, as the topics is empty.");
     }
     String topic = topicPartitions.get(0).getTopic();
     Map<String, String> tableParams = props.entrySet()
             .stream()
             .collect(Collectors.toMap(e -> (String) e.getKey(), e -> (String) e.getValue()));
     tableParams.put("topic", topic);
     tableParams.put("format", format);
     tableParams.put("client.id", CONNECTOR_ID + "-" + UUID.randomUUID());
     tableParams.put(KEY_ENABLE_AUTO_COMMIT, props.getProperty(KEY_ENABLE_AUTO_COMMIT, "true"));
     tableParams.put(KEY_AUTO_OFFSET_RESET, props.getProperty(KEY_AUTO_OFFSET_RESET, "latest"));
     tableParams.put(KEY_STARTUP_MODE, props.getProperty(KEY_STARTUP_MODE, "group-offsets"));
     return new KafkaTableHandle(
       CONNECTOR_ID,
       topic, 
       (io.github.zhztheplayer.velox4j.type.RowType) veloxOutputType,
       tableParams);
   }
 
   /**
    * Pull the kafka record from the queue, and emit it to the next operator. 
    */
   @Override
   public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
     if (running && result != null && result.hasNext()) {
       RowVector rowVector = result.next();
       List<RowData> rows = FlinkRowToVLVectorConvertor.toRowData(rowVector, allocator, 
         (io.github.zhztheplayer.velox4j.type.RowType) veloxOutputType);
       for (RowData row : rows) {
         output.collect((T) row);
       }
       rowVector.close();
     }
     return running ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
   }
 
   /**
    * Create a record fetch task, and submit to the thread pool.
    * consume the kafka record from kafka partition and put it into a queue.
    */
   @Override
   public void addSplits(List<KafkaPartitionSplit> splits) {
     LOG.info("Add kafka partitons to consume: {}", splits.toString());
     topicPartitions.addAll(splits);
     KafkaConnectorSplit kafkaConnectorSplit = getConnectionSplit(props);
     List<BoundSplit> veloxSplits = List.of(new BoundSplit(planNodeId, -1, kafkaConnectorSplit));
     TableScanNode kafkaScan = new TableScanNode(planNodeId,
         LogicalTypeConverter.toVLType(outputType.getLogicalType()), getTableHandle(), new ArrayList<>());
     query = new Query(kafkaScan, veloxSplits, Config.empty(), ConnectorConfig.empty());
     result = UpIterators.asJavaIterator(session.queryOps().execute(query));
   }
 
   @Override
   public List<KafkaPartitionSplit> snapshotState(long checkpointId) {
     throw new RuntimeException("Not implemented");
   }
 
   @Override
   public void notifyCheckpointComplete(long checkpointId) throws Exception {
   }
 
   @Override
   public void close() throws Exception {
     running = false;
     if (result != null) {
       result.close();
     }
     if (session != null) {
       session.close();
     }
     if (memoryManager != null) {
      memoryManager.close();
     }
     if (topicPartitions != null) {
       topicPartitions.clear();
     }
   }
 
   @Override
   public void start() {
     this.running = true;
   }
 
   @Override
   public CompletableFuture<Void> isAvailable() {
     return running ? CompletableFuture.completedFuture(null) : new CompletableFuture<>();
   }
 
   @Override
   public void notifyNoMoreSplits() {
     LOG.info("Reader received NoMoreSplits event.");
   }
 }
 