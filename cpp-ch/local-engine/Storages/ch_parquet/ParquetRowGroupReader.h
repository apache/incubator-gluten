#pragma once
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <generated/parquet_types.h>
#include <Processors/Chunk.h>
#include "ParquetColumnReader.h"
#include "metadata.h"
#include "param.h"

namespace DB
{


class ParquetGroupReader {
public:
    ParquetGroupReader(ParquetGroupReaderParam& param, int row_group_number);
    ~ParquetGroupReader() = default;

    void init();
    Chunk getNext();
    void close();

private:
    void initColumnReaders();
    void createColumnReader(const ParquetGroupReaderParam::Column& column);
    bool supportDictFilter(int column_idx_in_chunk);

    Chunk read(const std::vector<int>& read_columns, size_t row_count);
    std::pair<bool, ColumnPtr> readColumn(int idx, size_t row_count, bool force_values);
    bool filterPage();

    std::shared_ptr<parquet::format::RowGroup> row_group_metadata;
    std::unordered_map<size_t, std::unique_ptr<ParquetColumnReader>> column_readers;
    /// col_idx_in_chunk
    std::unordered_map<DB::String, size_t> column_name_to_idx;
    std::unordered_map<size_t, DB::String> column_idx_to_name;
    std::vector<ParquetGroupReaderParam::Column> direct_read_columns;
    std::vector<int> active_column_indices;
    ParquetGroupReaderParam& param;
    ColumnReaderOptions column_reader_opts;
    bool end = false;
};
}


