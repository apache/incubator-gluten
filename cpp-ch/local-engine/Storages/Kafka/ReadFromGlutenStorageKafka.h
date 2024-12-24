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

#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Storages/Kafka/KafkaSettings.h>

namespace local_engine
{
using namespace DB;
class ReadFromGlutenStorageKafka : public ISourceStep, protected WithContext
{
public:
    ReadFromGlutenStorageKafka(
        const Names & column_names_,
        Header output_header_,
        ContextPtr context_,
        Names & topics,
        size_t partition,
        size_t start_offset,
        size_t end_offset,
        size_t poll_timeout_ms,
        String group_id,
        String brokers);

    String getName() const override { return "ReadFromGlutenStorageKafka"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/) final;

private:
    Pipe makePipe();
    std::shared_ptr<KafkaSettings> createKafkaSettings();

protected:
    // std::shared_ptr<const StorageLimitsList> storage_limits;
    const Names & column_names;
    Header output_header;

    Names topics;
    size_t partition;
    size_t start_offset;
    size_t end_offset;
    size_t poll_timeout_ms;
    String group_id;
    String brokers;
};


}
