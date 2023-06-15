#pragma once
#include <unordered_set>
#include <Core/Block.h>
#include <DataTypes/DataTypeNested.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Processors/Chunk.h>
#include "Filters.h"
#include "metadata.h"
#include "param.h"

namespace parquet
{
namespace format
{
    class FileMetaData;
    class SchemaElement;
}
}


namespace DB
{

class ReadBufferFromFileBase;
class ParquetGroupReader;
struct ParquetGroupReaderParam;

struct ScanParam
{
    Block header;
    std::unordered_set<int> skip_row_groups;
    std::vector<int> active_columns;
    bool case_sensitive = false;
    std::shared_ptr<PushDownFilter> filter;
    std::shared_ptr<RowGroupFilter> groupFilter;
};

class ParquetFileReader
{
public:
    ParquetFileReader(ReadBufferFromFileBase * file, ScanParam context, size_t chunk_size = 8192);
    void init();
    Chunk getNext();

private:
    // Reads and parses file footer.
    void loadFileMetaData();
    void prepareReadColumns();
    void initGroupReaderParam();
    bool filterRowGroup(size_t id);
    bool
    readMinMaxBlock(const parquet::format::RowGroup & row_group, Block & minBlock, Block & maxBlock) const;
    static const parquet::format::ColumnMetaData * getColumnMeta(const parquet::format::RowGroup & row_group, const std::string & col_name);
    static bool decodeMinMaxColumn(
        const parquet::format::ColumnMetaData & column_meta,
        const parquet::format::ColumnOrder * column_order,
        MutableColumnPtr min_column,
        MutableColumnPtr max_column);
    static bool canUseMinMaxStats(const parquet::format::ColumnMetaData& column_meta,
                                       const parquet::format::ColumnOrder* column_order);
    // statistics.min_value max_value
    static bool canUseStats(const parquet::format::Type::type& type, const parquet::format::ColumnOrder* column_order);
    // statistics.min max
    static bool canUseDeprecatedStats(const parquet::format::Type::type& type, const parquet::format::ColumnOrder* column_order);
    static bool isIntegerType(const parquet::format::Type::type& type);
    std::shared_ptr<ParquetGroupReader> getGroupReader(int id);


    ReadBufferFromFileBase * file;
    size_t file_length;
    FileMetaData metadata;
    std::shared_ptr<ParquetGroupReader> current_group_reader;
    ParquetGroupReaderParam group_reader_param;
    size_t chunk_size;
    ScanParam param;
    std::vector<ParquetGroupReaderParam::Column> read_cols;
    size_t total_row_count = 0;
    size_t scan_row_count = 0;
    size_t row_group_size = 0;
    size_t cur_row_group_idx = 0;
};

} // DB
