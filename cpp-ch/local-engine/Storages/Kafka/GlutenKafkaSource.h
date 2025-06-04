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


#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>
#include <Storages/Kafka/KafkaConsumer.h>
#include <Storages/Kafka/KafkaSettings.h>

namespace local_engine
{

class GlutenKafkaSource : public DB::ISource
{
public:
    GlutenKafkaSource(
        const DB::Block & result_header_,
        const DB::ContextPtr & context_,
        const DB::Names & topics_,
        const size_t & partition_,
        const String & brokers_,
        const String & group_,
        const size_t & poll_timeout_ms_,
        const size_t & start_offset_,
        const size_t & end_offset_,
        const std::shared_ptr<DB::KafkaSettings> & kafka_settings_);

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
    DB::Chunk generate() override;

private:
    DB::Chunk generateImpl();
    void initConsumer();

    size_t getPollMaxBatchSize() const;
    size_t getMaxBlockSize() const;
    size_t getPollTimeoutMillisecond() const;

    LoggerPtr log;
    DB::ContextPtr context;
    UInt64 max_block_size;
    std::shared_ptr<DB::KafkaConsumer> consumer;

    DB::Block result_header;
    DB::Block virtual_header;
    DB::Block non_virtual_header;
    std::shared_ptr<DB::KafkaSettings> kafka_settings;

    const DB::Names topics;
    const size_t partition;
    const String brokers;
    const String group;
    const size_t poll_timeout_ms;
    const size_t start_offset;
    const size_t end_offset;
    String client_id;
    bool finished = false;
    size_t total_rows = 0;
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
static std::unordered_map<GlutenKafkaSource::TopicPartition, std::vector<std::shared_ptr<DB::KafkaConsumer>>> consumers_in_memory;
static const std::atomic<bool> is_stopped{false}; // for kafka progress, it always false
}