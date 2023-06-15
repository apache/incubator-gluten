#pragma once

#include <generated/parquet_types.h>
#include <IO/SeekableReadBuffer.h>
#include "metadata.h"
#include "Filters.h"

namespace DB
{
struct ColumnReaderContext
{
    size_t next_row = 0;

    void advance(size_t num_rows) { next_row += num_rows; }
};

struct ColumnReaderOptions
{
    std::string timezone;
    bool case_sensitive = false;
    size_t chunk_size = 0;
    SeekableReadBuffer * stream = nullptr;
    parquet::format::RowGroup * row_group_meta = nullptr;
    bool is_numeric_type = false;
    DataTypePtr stats_type;
    ColumnReaderContext * context = nullptr;
};


struct ParquetGroupReaderParam {
    struct Column {
        size_t col_idx_in_parquet;
        size_t col_idx_in_chunk;
        parquet::format::Type::type col_type_in_parquet;
        DataTypePtr col_type_in_chunk;

    };

    std::vector<Column> read_cols;

    // condition column index in parquet
    std::vector<Column> condition_cols;

    std::vector<Column> output_cols;

    std::string timezone;

    size_t chunk_size = 0;

    std::shared_ptr<PushDownFilter> filter;

    std::shared_ptr<PageFilter> page_filter;

    FileMetaData* file_metadata = nullptr;

    SeekableReadBuffer * file_read_buffer;

    bool case_sensitive = false;
};
}

