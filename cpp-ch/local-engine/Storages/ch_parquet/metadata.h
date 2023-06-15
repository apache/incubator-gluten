#pragma once
#include "schema.h"
#include <generated/parquet_types.h>

namespace DB
{
class FileMetaData
{
    friend class ParquetFileReader;
public:
    FileMetaData() = default;
    ~FileMetaData() = default;

    void init(const parquet::format::FileMetaData & metadata);

    uint64_t numRows() const { return num_rows; }

    std::string debug_string() const;

    const parquet::format::FileMetaData & parquetMetaData() const { return parquet_metadata; }

    const Schema & schema() const { return schema_; }

private:
    parquet::format::FileMetaData parquet_metadata;
    uint64_t num_rows = 0;
    Schema schema_;
};

}
