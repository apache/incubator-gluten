#pragma once
#include <cstdint>
#include <Storages/SubstraitSource/FormatFile.h>
#include <unordered_map>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <base/types.h>
namespace local_engine
{

class FileReaderWrapper
{
public:
    explicit FileReaderWrapper(FormatFilePtr file_) : file(file_) {}
    virtual ~FileReaderWrapper() = default;
    virtual bool pull(DB::Chunk & chunk) = 0;

protected:
    FormatFilePtr file;

    static DB::ColumnPtr createConstColumn(DB::DataTypePtr type, const DB::Field & field, size_t rows);
    static DB::Field buildFieldFromString(const String & value, DB::DataTypePtr type);
};

class NormalFileReader : public FileReaderWrapper
{
public:
    NormalFileReader(FormatFilePtr file_, DB::ContextPtr context_, const DB::Block & to_read_header_, const DB::Block & output_header_);
    ~NormalFileReader() override = default;
    bool pull(DB::Chunk & chunk) override;

private:
    DB::ContextPtr context;
    DB::Block to_read_header;
    DB::Block output_header;

    FormatFile::InputFormatPtr input_format;
    std::unique_ptr<DB::QueryPipeline> pipeline;
    std::unique_ptr<DB::PullingPipelineExecutor> reader;
};

class EmptyFileReader : public FileReaderWrapper
{
public:
    explicit EmptyFileReader(FormatFilePtr file_) : FileReaderWrapper(file_) {}
    ~EmptyFileReader() override = default;
    bool pull(DB::Chunk &) override { return false; }
};

class ConstColumnsFileReader : public FileReaderWrapper
{
public:
    ConstColumnsFileReader(FormatFilePtr file_, DB::ContextPtr context_, const DB::Block & header_, size_t block_size_ = DEFAULT_BLOCK_SIZE);
    ~ConstColumnsFileReader() override = default;
    bool pull(DB::Chunk & chunk);
private:
    DB::ContextPtr context;
    DB::Block header;
    size_t remained_rows;
    size_t block_size;
};

class SubstraitFileSource : public DB::ISource
{
public:
    SubstraitFileSource(DB::ContextPtr context_, const DB::Block & header_, const substrait::ReadRel::LocalFiles & file_infos);
    ~SubstraitFileSource() override = default;

    String getName() const override
    {
        return "SubstraitFileSource";
    }
protected:
    DB::Chunk generate() override;
private:
    DB::ContextPtr context;
    DB::Block output_header;
    DB::Block flatten_output_header; // flatten a struct column into independent field columns recursively
    DB::Block to_read_header; // Not include partition keys
    FormatFiles files;

    UInt32 current_file_index = 0;
    std::unique_ptr<FileReaderWrapper> file_reader;
    ReadBufferBuilderPtr read_buffer_builder;

    bool tryPrepareReader();

    // E.g we have flatten columns correspond to header {a:int, b.x.i: int, b.x.j: string, b.y: string}
    // but we want to fold all the flatten struct columns into one struct column,
    // {a:int, b: {x: {i: int, j: string}, y: string}}
    // Notice, don't support list with named struct. ClickHouse may take advantage of this to support
    // nested table, but not the case in spark.
    static DB::Block foldFlattenColumns(const DB::Columns & cols, const DB::Block & header);
    static DB::ColumnWithTypeAndName
    foldFlattenColumn(DB::DataTypePtr col_type, const std::string & col_name, size_t & pos, const DB::Columns & cols);
};
}
