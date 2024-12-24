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

#pragma once


#include <Processors/ISource.h>
#include <Storages/Kafka/StorageKafka.h>

namespace local_engine
{
using namespace DB;

class GlutenKafkaSource : public ISource
{
public:
    GlutenKafkaSource(
        const Block & result_header_,
        const ContextPtr & context_,
        const Names & topics_,
        const size_t & partition_,
        const String & brokers_,
        const String & group_,
        const size_t & poll_timeout_ms_,
        const size_t & start_offset_,
        const size_t & end_offset_,
        const std::shared_ptr<KafkaSettings> & kafka_settings_);

    ~GlutenKafkaSource() override;

    struct TopicPartition
    {
        String topic;
        size_t partition;

        bool operator==(const TopicPartition & other) const { return topic == other.topic && partition == other.partition; }

        TopicPartition(const String & topic, size_t partition) : topic(topic), partition(partition) { }
    };

    String getName() const override { return "GlutenKafkaSource"; }

protected:
    Chunk generate() override;

private:
    Chunk generateImpl();
    void initConsumer();

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;

    LoggerPtr log;
    ContextPtr context;
    UInt64 max_block_size;
    KafkaConsumerPtr consumer;

    Block result_header;
    Block virtual_header;
    Block non_virtual_header;
    std::shared_ptr<KafkaSettings> kafka_settings;

    const Names topics;
    const size_t partition;
    const String brokers;
    const String group;
    const String client_id = "123";
    const size_t poll_timeout_ms;
    const size_t start_offset;
    const size_t end_offset;
    bool finished = false;
};

}

namespace std
{
template <>
struct hash<local_engine::GlutenKafkaSource::TopicPartition>
{
    std::size_t operator()(const local_engine::GlutenKafkaSource::TopicPartition & tp) const noexcept
    {
        std::size_t h1 = std::hash<std::string>{}(tp.topic);
        std::size_t h2 = std::hash<size_t>{}(tp.partition);
        return h1 ^ (h2 << 1); // Combine the two hash values
    }
};
}

namespace local_engine
{
static std::mutex consumer_mutex;
static std::unordered_map<GlutenKafkaSource::TopicPartition, std::vector<KafkaConsumerPtr>> consumers_in_memory;
}