#pragma once
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Formats/NativeWriter.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/PODArray.h>
#include <Common/PODArray_fwd.h>
#include <Shuffle/SelectorBuilder.h>


namespace local_engine
{
struct SplitOptions
{
    size_t split_size = DEFAULT_BLOCK_SIZE;
    size_t io_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    std::string data_file;
    std::vector<std::string> local_dirs_list;
    int num_sub_dirs;
    int shuffle_id;
    int map_id;
    size_t partition_nums;
    std::string hash_exprs;
    std::string out_exprs;
    // std::vector<std::string> exprs;
    std::string compress_method = "zstd";
    int compress_level;
};

class ColumnsBuffer
{
public:
    explicit ColumnsBuffer(size_t prefer_buffer_size = DEFAULT_BLOCK_SIZE);
    void add(DB::Block & columns, int start, int end);
    void appendSelective(size_t column_idx, const DB::Block & source, const DB::IColumn::Selector & selector, size_t from, size_t length);
    size_t size() const;
    DB::Block releaseColumns();
    DB::Block getHeader();

private:
    DB::MutableColumns accumulated_columns;
    DB::Block header;
    size_t prefer_buffer_size;
};

struct SplitResult
{
    Int64 total_compute_pid_time = 0;
    Int64 total_write_time = 0;
    Int64 total_spill_time = 0;
    Int64 total_bytes_written = 0;
    Int64 total_bytes_spilled = 0;
    std::vector<Int64> partition_length;
    std::vector<Int64> raw_partition_length;
};

class ShuffleSplitter
{
public:
    static const std::vector<std::string> compress_methods;
    using Ptr = std::unique_ptr<ShuffleSplitter>;
    static Ptr create(const std::string & short_name, SplitOptions options_);
    explicit ShuffleSplitter(SplitOptions && options);
    virtual ~ShuffleSplitter()
    {
        if (!stopped)
            stop();
    }
    void split(DB::Block & block);
    virtual void computeAndCountPartitionId(DB::Block &) { }
    std::vector<int64_t> getPartitionLength() const { return split_result.partition_length; }
    void writeIndexFile();
    SplitResult stop();

private:
    void init();
    void splitBlockByPartition(DB::Block & block);
    void spillPartition(size_t partition_id);
    std::string getPartitionTempFile(size_t partition_id);
    void mergePartitionFiles();
    std::unique_ptr<DB::WriteBuffer> getPartitionWriteBuffer(size_t partition_id);

protected:
    bool stopped = false;
    PartitionInfo partition_info;
    std::vector<ColumnsBuffer> partition_buffer;
    std::vector<std::unique_ptr<DB::NativeWriter>> partition_outputs;
    std::vector<std::unique_ptr<DB::WriteBuffer>> partition_write_buffers;
    std::vector<std::unique_ptr<DB::WriteBuffer>> partition_cached_write_buffers;
    std::vector<size_t> output_columns_indicies;
    DB::Block output_header;
    SplitOptions options;
    SplitResult split_result;
};

class RoundRobinSplitter : public ShuffleSplitter
{
public:
    static std::unique_ptr<ShuffleSplitter> create(SplitOptions && options);

    explicit RoundRobinSplitter(SplitOptions options_);

    ~RoundRobinSplitter() override = default;
    void computeAndCountPartitionId(DB::Block & block) override;

private:
    std::unique_ptr<RoundRobinSelectorBuilder> selector_builder;
};

class HashSplitter : public ShuffleSplitter
{
public:
    static std::unique_ptr<ShuffleSplitter> create(SplitOptions && options);

    explicit HashSplitter(SplitOptions options_);

    ~HashSplitter() override = default;
    void computeAndCountPartitionId(DB::Block & block) override;

private:
    std::unique_ptr<HashSelectorBuilder> selector_builder;
};

class RangeSplitter : public ShuffleSplitter
{
public:
    static std::unique_ptr<ShuffleSplitter> create(SplitOptions && options);
    explicit RangeSplitter(SplitOptions options_);
    void computeAndCountPartitionId(DB::Block & block) override;
private:
    std::unique_ptr<RangeSelectorBuilder> selector_builder;
};
struct SplitterHolder
{
    ShuffleSplitter::Ptr splitter;
};


}
