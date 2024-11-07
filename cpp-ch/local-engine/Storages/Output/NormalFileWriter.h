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

#include <Core/Block.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/NativeOutputWriter.h>
#include <Storages/Output/OutputFormatFile.h>
#include <Storages/PartitionedSink.h>
#include <base/types.h>
#include <Common/ArenaUtils.h>
#include <Common/BlockTypeUtils.h>
#include <Common/CHUtil.h>

namespace local_engine
{

class NormalFileWriter : public NativeOutputWriter
{
public:
    static std::unique_ptr<NativeOutputWriter> create(
        const DB::ContextPtr & context, const std::string & file_uri, const DB::Block & preferred_schema, const std::string & format_hint);

    NormalFileWriter(const OutputFormatFilePtr & file_, const DB::ContextPtr & context_);
    ~NormalFileWriter() override = default;

    void write(DB::Block & block) override;
    void close() override;

private:
    OutputFormatFilePtr file;
    DB::ContextPtr context;

    OutputFormatFile::OutputFormatPtr output_format;
    std::unique_ptr<DB::QueryPipeline> pipeline;
    std::unique_ptr<DB::PushingPipelineExecutor> writer;
};

OutputFormatFilePtr createOutputFormatFile(
    const DB::ContextPtr & context, const std::string & file_uri, const DB::Block & preferred_schema, const std::string & format_hint);

class WriteStats : public DB::ISimpleTransform
{
    bool all_chunks_processed_ = false; /// flag to determine if we have already processed all chunks
    DB::Arena partition_keys_arena_;
    std::string filename_;

    absl::flat_hash_map<StringRef, size_t> fiel_to_count_;

    static DB::Block statsHeader()
    {
        return makeBlockHeader({{STRING(), "filename"}, {STRING(), "partition_id"}, {BIGINT(), "record_count"}});
    }

    DB::Chunk final_result() const
    {
        ///TODO: improve performance
        auto file_col = STRING()->createColumn();
        auto partition_col = STRING()->createColumn();
        auto countCol = BIGINT()->createColumn();
        UInt64 num_rows = 0;
        for (const auto & [relative_path, rows] : fiel_to_count_)
        {
            if (rows == 0)
                continue;
            file_col->insertData(filename_.c_str(), filename_.size());
            partition_col->insertData(relative_path.data, relative_path.size);
            countCol->insert(rows);
            num_rows++;
        }

        const DB::Columns res_columns{std::move(file_col), std::move(partition_col), std::move(countCol)};
        return DB::Chunk(res_columns, num_rows);
    }

public:
    explicit WriteStats(const DB::Block & input_header_) : ISimpleTransform(input_header_, statsHeader(), true) { }

    Status prepare() override
    {
        if (input.isFinished() && !output.isFinished() && !has_input && !all_chunks_processed_)
        {
            all_chunks_processed_ = true;
            /// return Ready to call transform() for generating filling rows after latest chunk was processed
            return Status::Ready;
        }

        return ISimpleTransform::prepare();
    }

    String getName() const override { return "WriteStats"; }
    void transform(DB::Chunk & chunk) override
    {
        if (all_chunks_processed_)
            chunk = final_result();
        else
            chunk = {};
    }

    void addFilePath(const String & patition_id, const String & filename)
    {
        assert(!filename.empty());

        if (filename_.empty())
            filename_ = filename;

        assert(filename_ == filename);

        if (patition_id.empty())
            return;
        fiel_to_count_.emplace(copyStringInArena(partition_keys_arena_, patition_id), 0);
    }

    void collectStats(const String & file_path, size_t rows)
    {
        if (const auto it = fiel_to_count_.find(file_path); it != fiel_to_count_.end())
        {
            it->second += rows;
            return;
        }
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "File path {} not found in the stats map", file_path);
    }
};

class SubstraitFileSink final : public DB::SinkToStorage
{
    const std::string partition_id_;
    const std::string relative_path_;
    OutputFormatFile::OutputFormatPtr output_format_;
    std::shared_ptr<WriteStats> stats_{nullptr};

    static std::string makeFilename(const std::string & base_path, const std::string & partition_id, const std::string & relative)
    {
        if (partition_id.empty())
            return fmt::format("{}/{}", base_path, relative);
        return fmt::format("{}/{}/{}", base_path, partition_id, relative);
    }

public:
    /// visible for UTs
    static const std::string NO_PARTITION_ID;

    explicit SubstraitFileSink(
        const DB::ContextPtr & context,
        const std::string & base_path,
        const std::string & partition_id,
        const std::string & relative,
        const std::string & format_hint,
        const DB::Block & header)
        : SinkToStorage(header)
        , partition_id_(partition_id.empty() ? NO_PARTITION_ID : partition_id)
        , relative_path_(relative)
        , output_format_(createOutputFormatFile(context, makeFilename(base_path, partition_id, relative), header, format_hint)
                             ->createOutputFormat(header))
    {
    }
    String getName() const override { return "SubstraitFileSink"; }

    ///TODO: remove this function
    void setStats(const std::shared_ptr<WriteStats> & stats)
    {
        stats_ = stats;
        stats_->addFilePath(partition_id_, relative_path_);
    }

protected:
    void consume(DB::Chunk & chunk) override
    {
        const size_t row_count = chunk.getNumRows();
        output_format_->output->write(materializeBlock(getHeader().cloneWithColumns(chunk.detachColumns())));

        if (stats_)
            stats_->collectStats(partition_id_, row_count);
    }
    void onFinish() override
    {
        output_format_->output->finalize();
        output_format_->output->flush();
        output_format_->write_buffer->finalize();
    }
};

class SubstraitPartitionedFileSink final : public DB::PartitionedSink
{
    static const std::string DEFAULT_PARTITION_NAME;

public:
    /// visible for UTs
    static DB::ASTPtr make_partition_expression(const DB::Names & partition_columns)
    {
        /// Parse the following expression into ASTs
        /// cancat('/col_name=', 'toString(col_name)')
        bool add_slash = false;
        DB::ASTs arguments;
        for (const auto & column : partition_columns)
        {
            // partition_column=
            std::string key = add_slash ? fmt::format("/{}=", column) : fmt::format("{}=", column);
            add_slash = true;
            arguments.emplace_back(std::make_shared<DB::ASTLiteral>(key));

            // ifNull(toString(partition_column), DEFAULT_PARTITION_NAME)
            // FIXME if toString(partition_column) is empty
            auto column_ast = std::make_shared<DB::ASTIdentifier>(column);
            DB::ASTs if_null_args{
                makeASTFunction("toString", DB::ASTs{column_ast}), std::make_shared<DB::ASTLiteral>(DEFAULT_PARTITION_NAME)};
            arguments.emplace_back(makeASTFunction("ifNull", std::move(if_null_args)));
        }
        return DB::makeASTFunction("concat", std::move(arguments));
    }

private:
    const std::string base_path_;
    const std::string filenmame_;
    DB::ContextPtr context_;
    const DB::Block sample_block_;
    const std::string format_hint_;
    std::shared_ptr<WriteStats> stats_{nullptr};

public:
    SubstraitPartitionedFileSink(
        const DB::ContextPtr & context,
        const DB::Names & partition_by,
        const DB::Block & input_header,
        const DB::Block & sample_block,
        const std::string & base_path,
        const std::string & filename,
        const std::string & format_hint)
        : PartitionedSink(make_partition_expression(partition_by), context, input_header)
        , base_path_(base_path)
        , filenmame_(filename)
        , context_(context)
        , sample_block_(sample_block)
        , format_hint_(format_hint)
    {
    }
    DB::SinkPtr createSinkForPartition(const String & partition_id) override
    {
        assert(stats_);
        const auto partition_path = fmt::format("{}/{}", partition_id, filenmame_);
        PartitionedSink::validatePartitionKey(partition_path, true);
        auto file_sink = std::make_shared<SubstraitFileSink>(context_, base_path_, partition_id, filenmame_, format_hint_, sample_block_);
        file_sink->setStats(stats_);
        return file_sink;
    }
    String getName() const override { return "SubstraitPartitionedFileSink"; }

    ///TODO: remove this function
    void setStats(const std::shared_ptr<WriteStats> & stats) { stats_ = stats; }
};
}
