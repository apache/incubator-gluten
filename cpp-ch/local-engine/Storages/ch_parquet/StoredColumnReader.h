#pragma once

#include <generated/parquet_types.h>
#include "schema.h"
#include "type.h"
#include "ParquetColumnChunkReader.h"
#include <Common/PODArray.h>


namespace DB
{


class StoredColumnReader
{
public:
    static std::unique_ptr<StoredColumnReader>
    create(const ColumnReaderOptions & opts, const ParquetField * field, const parquet::format::ColumnChunk * chunk_metadata);
    explicit StoredColumnReader(const ColumnReaderOptions & opts_) : opts(opts_) { }

    virtual ~StoredColumnReader() = default;

    virtual void reset() = 0;

    virtual size_t readRecords(size_t num_rows, MutableColumnPtr & dst, bool values = true) = 0;

    virtual void getLevels(level_t ** def_levels, level_t ** rep_levels, size_t * num_levels) = 0;

    virtual void getDictValues(const PaddedPODArray<UInt32> & dict_codes, MutableColumnPtr & column)
    {
        reader->getDictValues(dict_codes, column);
    }

    virtual bool canUseMinMaxStatics() {return false;}
    virtual bool currentIsDict() {return reader->isDictColumn();}

    std::pair<ColumnPtr, ColumnPtr> readMinMaxColumn();

    size_t nextPage();

    size_t skipPage();

    void skipRows(size_t rows);
protected:

    std::shared_ptr<ParquetColumnChunkReader> reader;
    size_t num_values_left_in_cur_page = 0;
    const ColumnReaderOptions & opts;
};


class RequiredStoredColumnReader : public StoredColumnReader
{
public:
    explicit RequiredStoredColumnReader(const ColumnReaderOptions & opts_) : StoredColumnReader(opts_) { }
    ~RequiredStoredColumnReader() override = default;

    void init(const ParquetField * field_, const parquet::format::ColumnChunk * chunk_metadata_);

    void reset() override { }

    size_t readRecords(size_t num_rows, MutableColumnPtr & dst, bool values = true) override;

    void getLevels(level_t ** def_levels, level_t ** rep_levels, size_t * num_levels) override
    {
        *def_levels = nullptr;
        *rep_levels = nullptr;
        *num_levels = 0;
    }

    bool canUseMinMaxStatics() override;

private:
    const parquet::format::ColumnChunk chunk_metadata;
    const ParquetField * field = nullptr;
};

class OptionalStoredColumnReader : public StoredColumnReader
{
public:
    explicit OptionalStoredColumnReader(const ColumnReaderOptions& opts_) : StoredColumnReader(opts_) {}
    ~OptionalStoredColumnReader() override = default;

    void init(const ParquetField* field, const parquet::format::ColumnChunk* chunk_metadata);

    // Reset internal state and ready for next read_values
    void reset() override;

    size_t readRecords(size_t num_records, MutableColumnPtr & dst, bool values) override {
//        if (_needs_levels) {
//            return _read_records_and_levels(num_records, dst);
//        } else {
            return _read_records_only(num_records, dst, values);
//        }
    }

//    void set_needs_levels(bool needs_levels) override { _needs_levels = needs_levels; }

    void getLevels(level_t** def_levels_, level_t** rep_levels, size_t* num_levels) override {
        // _needs_levels must be true
//        DCHECK(_needs_levels);

        *def_levels_ = nullptr;
        *rep_levels = nullptr;
        *num_levels = 0;
    }

private:
//    void _decode_levels(size_t num_levels);
    size_t _read_records_only(size_t num_records, MutableColumnPtr & dst, bool values);
//    size_t _read_records_and_levels(size_t num_records, MutableColumnPtr&  dst);

    const ParquetField* _field = nullptr;

    // When the flag is false, the information of levels does not need to be materialized,
    // so that the advantages of RLE encoding can be fully utilized and a lot of overhead
    // can be saved in decoding.
//    bool _needs_levels = false;

    size_t levels_capacity = 0;
    PaddedPODArray<uint8_t> is_nulls;
    PaddedPODArray<level_t> def_levels;
};
}
