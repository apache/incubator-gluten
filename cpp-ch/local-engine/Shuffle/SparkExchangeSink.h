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
#include <Processors/ISink.h>
#include <Shuffle/SelectorBuilder.h>
#include <Shuffle/ShuffleCommon.h>
#include <jni.h>
#include <Shuffle/PartitionWriter.h>

namespace DB
{
    class QueryPipelineBuilder;
}

namespace local_engine
{
class CelebornClient;
class PartitionWriter;

class SparkExchangeSink : public DB::ISink
{
    friend class SparkExchangeManager;
public:
    SparkExchangeSink(const DB::Block& header, std::unique_ptr<SelectorBuilder> partitioner_,
                      std::shared_ptr<PartitionWriter> partition_writer_,
                      const std::vector<size_t>& output_columns_indicies_, bool sort_writer_)
        : DB::ISink(header)
          , partitioner(std::move(partitioner_))
          , partition_writer(partition_writer_)
          , output_columns_indicies(output_columns_indicies_)
          , sort_writer(sort_writer_)
    {
        initOutputHeader(header);
        partition_writer->initialize(&split_result, output_header);
    }

    String getName() const override
    {
        return "SparkExchangeSink";
    }

    const SplitResult& getSplitResult() const
    {
        return split_result;
    }

    const DB::Block& getOutputHeader() const
    {
        return output_header;
    }

protected:
    void consume(DB::Chunk block) override;
    void onFinish() override;

private:
    void initOutputHeader(const DB::Block& block);

    DB::Block output_header;
    std::unique_ptr<SelectorBuilder> partitioner;
    std::shared_ptr<PartitionWriter> partition_writer;
    std::vector<size_t> output_columns_indicies;
    bool sort_writer = false;
    SplitResult split_result;
};

using SelectBuilderPtr = std::unique_ptr<SelectorBuilder>;
using SelectBuilderCreator = std::function<SelectBuilderPtr(const SplitOptions &)>;

class SparkExchangeManager
{
public:
    SparkExchangeManager(const DB::Block& header, const String & short_name, const SplitOptions & options_,  jobject rss_pusher = nullptr);
    void initSinks(size_t num);
    void setSinksToPipeline(DB::QueryPipelineBuilder & pipeline) const;
    void pushBlock(const DB::Block &block);
    void finish();
    [[nodiscard]] SplitResult getSplitResult() const
    {
        return split_result;
    }

private:
    static SelectBuilderPtr createRoundRobinSelectorBuilder(const SplitOptions & options_);
    static SelectBuilderPtr createHashSelectorBuilder(const SplitOptions & options_);
    static SelectBuilderPtr createSingleSelectorBuilder(const SplitOptions & options_);
    static SelectBuilderPtr createRangeSelectorBuilder(const SplitOptions & options_);
    static std::unordered_map<String, SelectBuilderCreator> partitioner_creators;

    void mergeSplitResult();
    std::vector<SpillInfo> gatherAllSpillInfo() const;
    std::vector<UInt64> mergeSpills(DB::WriteBuffer & data_file, const std::vector<SpillInfo>& spill_infos, const std::vector<Spillable::ExtraData> & extra_datas = {});

    DB::Block input_header;
    std::vector<std::shared_ptr<SparkExchangeSink>> sinks;
    std::vector<std::shared_ptr<PartitionWriter>> partition_writers;
    std::unique_ptr<CelebornClient> celeborn_client = nullptr;
    SplitOptions options;
    SelectBuilderCreator partitioner_creator;
    std::vector<size_t> output_columns_indicies;
    bool use_sort_shuffle = false;
    bool use_rss = false;
    SplitResult split_result;
};
}
