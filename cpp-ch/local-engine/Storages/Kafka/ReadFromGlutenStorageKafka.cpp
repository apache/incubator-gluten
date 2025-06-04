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
#include "GlutenKafkaSource.h"

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/Kafka/ReadFromGlutenStorageKafka.h>

namespace DB
{
namespace Setting
{
extern const SettingsBool stream_like_engine_allow_direct_select;
}
}


namespace local_engine
{
using namespace DB;

ReadFromGlutenStorageKafka::ReadFromGlutenStorageKafka(
    const Names & column_names_,
    Header output_header_,
    // std::shared_ptr<const StorageLimitsList> storage_limits_,
    ContextPtr context_,
    Names & topics,
    size_t partition,
    size_t start_offset,
    size_t end_offset,
    size_t poll_timeout_ms,
    String group_id,
    String brokers)
    : ISourceStep{output_header_}
    , WithContext{context_}
    ,
    // storage_limits{std::move(storage_limits_)},
    column_names(column_names_)
    , output_header(output_header_)
    , topics(topics)
    , partition(partition)
    , start_offset(start_offset)
    , end_offset(end_offset)
    , poll_timeout_ms(poll_timeout_ms)
    , group_id(group_id)
    , brokers(brokers)
{
}

void ReadFromGlutenStorageKafka::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    /// Add storage limits.
    // for (const auto & processor : pipe.getProcessors())
    //     processor->setStorageLimits(storage_limits);

    /// Add to processors to get processor info through explain pipeline statement.
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

Pipe ReadFromGlutenStorageKafka::makePipe()
{
    auto kafka_settings = createKafkaSettings();
    Pipes pipes;
    pipes.reserve(1);
    auto modified_context = Context::createCopy(getContext());
    // modified_context->applySettingsChanges(kafka_storage.settings_adjustments);
    pipes.emplace_back(std::make_shared<GlutenKafkaSource>(
        output_header, modified_context, topics, partition, brokers, group_id, poll_timeout_ms, start_offset, end_offset, kafka_settings));

    // LOG_DEBUG(kafka_storage.log, "Starting reading kafka batch stream");
    return Pipe::unitePipes(std::move(pipes));
}

std::shared_ptr<KafkaSettings> ReadFromGlutenStorageKafka::createKafkaSettings()
{
    // TODO: add more configuration
    return std::make_shared<DB::KafkaSettings>();
}

}