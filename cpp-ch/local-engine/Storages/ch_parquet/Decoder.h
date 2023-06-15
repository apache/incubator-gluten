#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>
#include <generated/parquet_types.h>
#include <Common/PODArray.h>
#include "RleEncoding.h"
#include "Utils.h"
#include "type.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename T>
class RleDecoder;
template <typename T>
class RleBatchDecoder;

class Decoder
{
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    virtual void setDict(size_t /*chunk_size*/, size_t /*num_values*/, Decoder & /*decoder*/)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "setDict not supported");
    }

    virtual ColumnPtr getDictValues() { throw Exception(ErrorCodes::LOGICAL_ERROR, "getDictValues not supported"); }

    virtual void getDictValues(MutableColumnPtr & /*column*/) { throw Exception(ErrorCodes::LOGICAL_ERROR, "getDictValues not supported"); }

    virtual void getDictValues(const PaddedPODArray<UInt32> & /*dict_codes*/, MutableColumnPtr &)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "getDictValues not supported");
    }

    virtual void getDictCodes(const std::vector<Slice> & /*dict_values*/, std::vector<int32_t> & /*dict_codes*/)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "getDictCodes not supported");
    }

    virtual size_t skipRows(size_t) { throw Exception(ErrorCodes::LOGICAL_ERROR, "getDictCodes not supported"); }

    // used to set fixed length
    virtual void setTypeLength(int32_t /*type_length*/) { }

    // Set a new page to decoded.
    virtual void setData(const Slice & data) = 0;

    // For history reason, decoder don't known how many elements encoded in one page.
    // Caller must assure that no out-of-bounds access.
    // It will return ERROR if caller wants to read out-of-bound data.
    virtual void nextBatch(size_t count, MutableColumnPtr & dst, bool values = true) = 0;

    virtual void nextBatch(size_t /*count*/, uint8_t * /*dst*/) { throw Exception(ErrorCodes::LOGICAL_ERROR, "nextBatch not supported"); }
};

template <typename T>
class PlainDecoder final : public Decoder
{
public:
    PlainDecoder() = default;
    ~PlainDecoder() override = default;

    static void decode(const std::string & buffer, T * value)
    {
        int byte_size = sizeof(T);
        memcpy(value, buffer.c_str(), byte_size);
    }

    void setData(const Slice & data_) override
    {
        data.data = data_.data;
        data.size = data_.size;
        offset = 0;
    }

    void nextBatch(size_t count, MutableColumnPtr & dst, bool) override
    {
        chassert((count + offset) * SIZE_OF_TYPE <= data.size);
        const auto * data_start = reinterpret_cast<const T *>(data.data) + offset;
        if (dst->isNullable())
        {
            auto & nullable_col = static_cast<ColumnNullable &>(*dst);
            nullable_col.getNullMapData().resize_fill(dst->size() + count);
            auto & column_data = static_cast<ColumnVector<T> &>(nullable_col.getNestedColumn()).getData();
            column_data.insert_assume_reserved(data_start, data_start + count);
        }
        else
        {
            auto & column_data = static_cast<ColumnVector<T> &>(*dst).getData();
            column_data.insert_assume_reserved(data_start, data_start + count);
        }
        offset += count;
    }

    size_t skipRows(size_t rows) override
    {
        offset += rows;
        return rows;
    }

    void nextBatch(size_t count, uint8_t * dst) override
    {
        if ((count + offset) * SIZE_OF_TYPE > data.size) [[unlikely]]
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "going to read out-of-bounds data, offset={},count={},size={}", offset, count, data.size);
        }
        memcpy(dst, data.data + offset * SIZE_OF_TYPE, count * SIZE_OF_TYPE);
        offset += count;
    }

private:
    enum
    {
        SIZE_OF_TYPE = sizeof(T)
    };

    Slice data;
    // row number
    size_t offset = 0;
};

template <>
class PlainDecoder<DB::String> final : public Decoder
{
public:
    PlainDecoder() = default;
    ~PlainDecoder() override = default;

    //    static void decode(const std::string & buffer, DB::String * value)
    //    {
    //        int byte_size = sizeof(T);
    //        memcpy(value, buffer.c_str(), byte_size);
    //    }

    void setData(const Slice & data_) override
    {
        data.data = data_.data;
        data.size = data_.size;
        offset = 0;
        row_offset = 0;
        size_list.clear();
        size_list.reserve(8192 * 4);
        char * pointer = data.data + offset;
        while (pointer < data_.data + data.size)
        {
            auto length = decode_fixed32_le(pointer);
            pointer = pointer + sizeof(uint32_t) + length;
            size_list.emplace_back(length);
        }
    }

    void nextBatch(size_t count, MutableColumnPtr & dst, bool) override
    {
        chassert(count + row_offset <= size_list.size());
        size_t total_size = count;
        for (size_t i = row_offset; i < row_offset + count; i++)
        {
            total_size += size_list[i];
        }
        if (dst->isNullable())
        {
            auto & nullable_col = static_cast<ColumnNullable &>(*dst);
            nullable_col.getNullMapColumn().insertMany(0, count);
            chassert(checkColumn<ColumnString>(nullable_col.getNestedColumn()));
            auto & string_col = static_cast<ColumnString &>(nullable_col.getNestedColumn());
            insertDataToStringColumn(string_col, total_size, count);
        }
        else
        {
            chassert(checkColumn<ColumnString>(*dst));
            auto & string_col = static_cast<ColumnString &>(*dst);
            insertDataToStringColumn(string_col, total_size, count);
        }
        row_offset += count;
    }

    size_t skipRows(size_t rows) override
    {
        size_t total_size = rows * sizeof(uint32_t);
        for (size_t i = row_offset; i < row_offset + rows; i++)
        {
            total_size += size_list[i];
        }
        offset += total_size;
        row_offset += rows;
        return rows;
    }

private:
    void insertDataToStringColumn(ColumnString & col, size_t mem_size, size_t row_count)
    {
        PaddedPODArray<UInt8> & column_chars_t = col.getChars();
        PaddedPODArray<UInt64> & column_offsets = col.getOffsets();

        auto initial_size = column_chars_t.size();
        column_chars_t.reserve(initial_size + mem_size);
        column_offsets.reserve(initial_size + row_count);

        for (size_t i = 0; i < row_count; ++i)
        {
            const auto * raw_data = data.data + offset + sizeof(uint32_t);
            column_chars_t.insert_assume_reserved(raw_data, raw_data + size_list[row_offset + i]);
            column_chars_t.emplace_back('\0');
            column_offsets.emplace_back(column_chars_t.size());
            offset = offset + size_list[row_offset + i] + sizeof(uint32_t);
        }
        chassert(initial_size + mem_size == column_offsets.back());
    }

    Slice data;
    size_t offset = 0;
    size_t row_offset = 0;
    std::vector<size_t> size_list;
};

template <typename T>
class DictDecoder final : public Decoder
{
public:
    DictDecoder() = default;
    ~DictDecoder() override = default;

    // initialize dictionary
    void setDict(size_t chunk_size, size_t num_values, Decoder & decoder) override
    {
        dict.resize_fill(num_values);
        indexes.resize_fill(chunk_size);
        decoder.nextBatch(num_values, reinterpret_cast<uint8_t *>(dict.data()));
    }

    void setData(const Slice & data) override
    {
        if (data.size > 0)
        {
            uint8_t bit_width = *data.data;
            indexBatchDecoder
                = RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t *>(data.data) + 1, static_cast<int>(data.size) - 1, bit_width);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "input encoded data size is 0");
        }
    }

    void getDictValues(const PaddedPODArray<UInt32> & dict_codes, MutableColumnPtr & column) override
    {
        if (column->isNullable())
        {
            auto & nullable_col = static_cast<ColumnNullable &>(*column);
            nullable_col.getNullMapData().resize_fill(column->size() + dict_codes.size());
            auto mutable_nested_column = nullable_col.getNestedColumnPtr()->assumeMutable();
            index(dict_codes, mutable_nested_column);
        }
        else
        {
            index(dict_codes, column);
        }
    }

    void nextBatch(size_t count, MutableColumnPtr & dst, bool values) override
    {
        dst->reserve(count);

        if (values)
        {
            indexes.resize_fill(count);
            indexBatchDecoder.GetBatch(indexes.data(), static_cast<int32_t>(count));
            if (dst->isNullable())
            {
                auto & nullable_col = static_cast<ColumnNullable &>(*dst);
                nullable_col.getNullMapData().resize_fill(dst->size() + count);
                auto mutable_nested_column = nullable_col.getNestedColumnPtr()->assumeMutable();
                index(indexes, mutable_nested_column);
            }
            else
            {
                index(indexes, dst);
            }
        }
        else
        {
            ColumnVector<UInt32> * codes;
            if (dst->isNullable())
            {
                auto & nullable_col = static_cast<ColumnNullable &>(*dst);
                codes = static_cast<ColumnVector<UInt32> *>(nullable_col.getNestedColumnPtr()->assumeMutable().get());
            }
            else
            {
                codes = static_cast<ColumnVector<UInt32> *>(dst.get());
            }
            auto old_size = dst->size();
            codes->getData().resize_fill(old_size + count);
            indexBatchDecoder.GetBatch(&codes->getData().data()[old_size], static_cast<int32_t>(count));
        }
    }

    size_t skipRows(size_t rows) override
    {
        indexBatchDecoder.GetBatch(indexes.data(), static_cast<int32_t>(rows));
        return rows;
    }

private:
    void index(const PaddedPODArray<UInt32> & codes, MutableColumnPtr & dst)
    {
        ColumnVector<T> & dst_vector = static_cast<ColumnVector<T> &>(*dst);
        auto & dst_data = dst_vector.getData();
        auto old_size = dst->size();
        dst_data.resize_fill(old_size + codes.size());
        for (size_t i = 0; i < codes.size(); ++i)
        {
            dst_data[old_size + i] = dict[codes[i]];
        }
    }

    RleBatchDecoder<UInt32> indexBatchDecoder;
    PaddedPODArray<T> dict;
    PaddedPODArray<UInt32> indexes;
};


template <>
class DictDecoder<DB::String> final : public Decoder
{
public:
    DictDecoder() = default;
    ~DictDecoder() override = default;

    void setDict(size_t chunk_size, size_t num_values, Decoder & decoder) override
    {
        indexes.reserve(chunk_size);
        slices.resize(chunk_size);
        auto type = std::make_shared<DataTypeString>();
        dict = type->createColumn();
        dict->reserve(num_values);
        decoder.nextBatch(num_values, dict);
    }

    void getDictValues(MutableColumnPtr & column) override { column->insertRangeFrom(*dict, 0, dict->size()); }

    void getDictValues(const PaddedPODArray<UInt32> & dict_codes, MutableColumnPtr & column) override
    {
        if (column->isNullable())
        {
            auto & nullable_col = static_cast<ColumnNullable &>(*column);
            nullable_col.getNullMapData().resize_fill(column->size() + dict_codes.size());
            auto mutable_nested_column = nullable_col.getNestedColumnPtr()->assumeMutable();
            index(dict_codes, mutable_nested_column);
        }
        else
        {
            index(dict_codes, column);
        }
    }

    void setData(const Slice & data) override
    {
        if (data.size > 0)
        {
            uint8_t bit_width = *data.data;
            index_batch_decoder
                = RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t *>(data.data) + 1, static_cast<int>(data.size) - 1, bit_width);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "input encoded data size is 0");
        }
    }

    void nextBatch(size_t count, MutableColumnPtr & dst, bool values) override
    {
        if (values)
        {
            dst->reserve(count);
            indexes.resize_fill(count);
            index_batch_decoder.GetBatch(indexes.data(), static_cast<int32_t>(count));
            if (dst->isNullable())
            {
                auto & nullable_col = static_cast<ColumnNullable &>(*dst);
                nullable_col.getNullMapData().resize_fill(dst->size() + count);
                auto mutable_nested_column = nullable_col.getNestedColumnPtr()->assumeMutable();
                index(indexes, mutable_nested_column);
            }
            else
            {
                index(indexes, dst);
            }
        }
        else
        {
            ColumnVector<UInt32> * codes;
            if (dst->isNullable())
            {
                auto & nullable_col = static_cast<ColumnNullable &>(*dst);
                nullable_col.getNullMapData().resize_fill(dst->size() + count);
                codes = static_cast<ColumnVector<UInt32> *>(nullable_col.getNestedColumnPtr()->assumeMutable().get());
            }
            else
            {
                codes = static_cast<ColumnVector<UInt32> *>(dst.get());
            }
            auto old_size = dst->size();
            codes->getData().resize_fill(old_size + count);
            index_batch_decoder.GetBatch(&codes->getData().data()[old_size], static_cast<int32_t>(count));
        }
    }

    size_t skipRows(size_t rows) override
    {
        indexes.resize_fill(rows);
        index_batch_decoder.GetBatch(indexes.data(), static_cast<int32_t>(rows));
        return rows;
    }

private:
    void index(const PaddedPODArray<UInt32> & codes, MutableColumnPtr & dst)
    {
        size_t count = codes.size();
        if (count == 0)
            return;

        auto * res = static_cast<ColumnString *>(dst.get());
        auto * dict_column = static_cast<ColumnString *>(dict.get());

        auto & res_chars = res->getChars();
        auto & res_offsets = res->getOffsets();

        auto & dict_chars = dict_column->getChars();
        auto & dict_offsets = dict_column->getOffsets();

        size_t new_chars_size = 0;
        for (size_t i = 0; i < count; ++i)
            new_chars_size += (dict_column->byteSizeAt(codes[i]) - sizeof(UInt8));
        auto res_chars_old_size = res_chars.size();
        res_chars.resize_fill(res_chars_old_size + new_chars_size);
        auto res_offsets_old_size = res_offsets.size();
        res_offsets.resize_fill(res_offsets_old_size + count);

        ColumnString::Offset current_new_offset = res_chars_old_size;

        for (size_t i = 0; i < count; ++i)
        {
            size_t j = codes[i];
            size_t string_offset = dict_offsets[j - 1];
            size_t string_size = dict_offsets[j] - string_offset;

            memcpySmallAllowReadWriteOverflow15(&res_chars[current_new_offset], &dict_chars[string_offset], string_size);

            current_new_offset += string_size;
            res_offsets[res_offsets_old_size + i] = current_new_offset;
        }
    }

    RleBatchDecoder<UInt32> index_batch_decoder;
    MutableColumnPtr dict;
    PaddedPODArray<UInt32> indexes;
    std::vector<Slice> slices;
};

class LevelDecoder
{
public:
    void parse(parquet::format::Encoding::type encoding, size_t max_level, uint32_t num_levels, Slice * slice);

    // Try to decode n levels into levels;
    size_t decode_batch(size_t n, level_t * levels);

    size_t nextRepeatedCount() { return rle_decoder->repeatedCount(); }

    size_t get_repeated_value(size_t count);

private:
    parquet::format::Encoding::type encoding;
    size_t bit_width = 0;
    [[maybe_unused]] size_t max_level = 0;
    uint32_t num_levels = 0;
    std::shared_ptr<RleDecoder<level_t>> rle_decoder;
};

class EncodingInfo
{
public:
    static EncodingInfo get(parquet::format::Type::type type, parquet::format::Encoding::type encoding);
    template <typename TypeEncodingTraits>
    explicit EncodingInfo(TypeEncodingTraits traits);

    std::unique_ptr<Decoder> createDecoder() const { return create_decoder_func(); }


    parquet::format::Type::type getType() const { return type; }
    parquet::format::Encoding::type getEncoding() const { return encoding; }

private:
    friend class EncodingInfoResolver;


    using CreateDecoderFunc = std::function<std::unique_ptr<Decoder>()>;
    CreateDecoderFunc create_decoder_func;

    parquet::format::Type::type type;
    parquet::format::Encoding::type encoding;
};
}
