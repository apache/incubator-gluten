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

class WriteStatsBase : public DB::ISimpleTransform
{
protected:
    bool all_chunks_processed_ = false; /// flag to determine if we have already processed all chunks
    virtual DB::Chunk final_result() = 0;

public:
    WriteStatsBase(const DB::Block & input_header_, const DB::Block & output_header_)
        : ISimpleTransform(input_header_, output_header_, true)
    {
    }

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

    void transform(DB::Chunk & chunk) override
    {
        if (all_chunks_processed_)
            chunk = final_result();
        else
            chunk = {};
    }
};

class WriteStats : public WriteStatsBase
{
    static DB::Block statsHeader()
    {
        return makeBlockHeader({{STRING(), "filename"}, {STRING(), "partition_id"}, {BIGINT(), "record_count"}});
    }
    DB::Arena partition_keys_arena_;
    std::string filename_;
    absl::flat_hash_map<StringRef, size_t> file_to_count_;

protected:
    DB::Chunk final_result() override
    {
        const size_t size = file_to_count_.size();

        auto file_col = STRING()->createColumn();
        file_col->reserve(size);
        auto partition_col = STRING()->createColumn();
        partition_col->reserve(size);
        auto countCol = BIGINT()->createColumn();
        countCol->reserve(size);
        auto & countColData = static_cast<DB::ColumnVector<Int64> &>(*countCol).getData();

        UInt64 num_rows = 0;
        for (const auto & [relative_path, rows] : file_to_count_)
        {
            if (rows == 0)
                continue;
            file_col->insertData(filename_.c_str(), filename_.size());
            partition_col->insertData(relative_path.data, relative_path.size);
            countColData.emplace_back(rows);
            num_rows++;
        }

        const DB::Columns res_columns{std::move(file_col), std::move(partition_col), std::move(countCol)};
        return DB::Chunk(res_columns, num_rows);
    }

public:
    explicit WriteStats(const DB::Block & input_header_) : WriteStatsBase(input_header_, statsHeader()) { }
    String getName() const override { return "WriteStats"; }
    void addFilePath(const String & partition_id, const String & filename)
    {
        assert(!filename.empty());

        if (filename_.empty())
            filename_ = filename;

        assert(filename_ == filename);

        if (partition_id.empty())
            return;
        file_to_count_.emplace(copyStringInArena(partition_keys_arena_, partition_id), 0);
    }

    void collectStats(const String & file_path, size_t rows)
    {
        if (const auto it = file_to_count_.find(file_path); it != file_to_count_.end())
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
    std::shared_ptr<WriteStats> stats_;

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
        const DB::Block & header,
        const std::shared_ptr<WriteStatsBase> & stats)
        : SinkToStorage(header)
        , partition_id_(partition_id.empty() ? NO_PARTITION_ID : partition_id)
        , relative_path_(relative)
        , output_format_(createOutputFormatFile(context, makeFilename(base_path, partition_id, relative), header, format_hint)
                             ->createOutputFormat(header))
        , stats_(std::dynamic_pointer_cast<WriteStats>(stats))
    {
        stats_->addFilePath(partition_id_, relative_path_);
    }
    String getName() const override { return "SubstraitFileSink"; }

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

class SparkPartitionedBaseSink : public DB::PartitionedSink
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

protected:
    DB::ContextPtr context_;
    std::shared_ptr<WriteStatsBase> stats_;

public:
    SparkPartitionedBaseSink(
        const DB::ContextPtr & context,
        const DB::Names & partition_by,
        const DB::Block & input_header,
        const std::shared_ptr<WriteStatsBase> & stats)
        : PartitionedSink(make_partition_expression(partition_by), context, input_header), context_(context), stats_(stats)
    {
    }
};

class SubstraitPartitionedFileSink final : public SparkPartitionedBaseSink
{
    const std::string base_path_;
    const std::string filename_;
    const DB::Block sample_block_;
    const std::string format_hint_;

public:
    SubstraitPartitionedFileSink(
        const DB::ContextPtr & context,
        const DB::Names & partition_by,
        const DB::Block & input_header,
        const DB::Block & sample_block,
        const std::string & base_path,
        const std::string & filename,
        const std::string & format_hint,
        const std::shared_ptr<WriteStatsBase> & stats)
        : SparkPartitionedBaseSink(context, partition_by, input_header, stats)
        , base_path_(base_path)
        , filename_(filename)
        , sample_block_(sample_block)
        , format_hint_(format_hint)
    {
    }

    DB::SinkPtr createSinkForPartition(const String & partition_id) override
    {
        assert(stats_);
        const auto partition_path = fmt::format("{}/{}", partition_id, filename_);
        validatePartitionKey(partition_path, true);
        return std::make_shared<SubstraitFileSink>(context_, base_path_, partition_id, filename_, format_hint_, sample_block_, stats_);
    }
    String getName() const override { return "SubstraitPartitionedFileSink"; }
};
}
