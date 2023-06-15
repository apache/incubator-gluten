#include "ParquetFileReader.h"
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <IO/ReadBufferFromFileBase.h>
#include <generated/parquet_types.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TProtocol.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include "Decoder.h"
#include "ParquetRowGroupReader.h"
#include "thrift/ThriftFileTransport.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void check(bool condition, String msg)
{
    if (!condition)
    {
        throw Exception::createRuntime(ErrorCodes::LOGICAL_ERROR, msg);
    }
}

ParquetFileReader::ParquetFileReader(ReadBufferFromFileBase * file_, ScanParam param_, size_t chunk_size_)
    : file(file_), chunk_size(chunk_size_), param(param_)
{
    file_length = file->getFileSize();
}

std::unique_ptr<apache::thrift::protocol::TProtocol> createThriftProtocol(ReadBufferFromFileBase * file)
{
    auto transport = std::make_shared<ThriftFileTransport>(file);
    return std::make_unique<apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(transport);
}

void ParquetFileReader::loadFileMetaData()
{
    auto file_size = file->getFileSize();
    auto proto = createThriftProtocol(file);
    auto & transport = *reinterpret_cast<ThriftFileTransport *>(proto->getTransport().get());
    PODArray<uint8_t> buf(8);
    transport.setLocation(file_length - 8);
    transport.read(buf.data(), 8);
    if (strncmp(reinterpret_cast<char *>(buf.data()) + 4, "PAR1", 4) != 0)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No magic bytes found at end of file '{}'", file->getFileName());
    }
    auto footer_len = *reinterpret_cast<uint32_t *>(buf.data());
    if (footer_len <= 0 || file_size < 12 + footer_len)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Footer length error in file '{}'", file->getFileName());
    }
    auto metadata_pos = file_size - (footer_len + 8);
    transport.setLocation(metadata_pos);
    parquet::format::FileMetaData meta;
    meta.read(proto.get());
    metadata.init(meta);
}

void ParquetFileReader::init()
{
    if (file_length == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parquet file is empty");
    if (file_length < 12)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parquet file is too small");
    loadFileMetaData();
    prepareReadColumns();
    initGroupReaderParam();
}


void ParquetFileReader::prepareReadColumns()
{
    size_t col_idx = 0;
    for (const auto & column : param.header.getColumnsWithTypeAndName())
    {
        size_t field_index = metadata.schema().getColumnIndex(column.name, param.case_sensitive);

        auto parquet_type = metadata.schema().getStoredColumnByIdx(field_index)->physical_type;
        metadata.schema_.fields[field_index].type = column.type;
        ParquetGroupReaderParam::Column read_col{};
        read_col.col_idx_in_parquet = field_index;
        read_col.col_type_in_parquet = parquet_type;
        read_col.col_idx_in_chunk = col_idx;
        read_col.col_type_in_chunk = column.type;
        read_cols.emplace_back(read_col);
        col_idx++;
    }
}

void ParquetFileReader::initGroupReaderParam()
{
    group_reader_param.read_cols = read_cols;
    group_reader_param.chunk_size = chunk_size;
    group_reader_param.file_metadata = &metadata;
    group_reader_param.case_sensitive = param.case_sensitive;
    group_reader_param.file_read_buffer = file;
    if (param.filter)
    {
        group_reader_param.filter = param.filter;
    }
    for (const auto & item : param.active_columns)
    {
        group_reader_param.output_cols.emplace_back(read_cols[item]);
    }

    // select and create row group readers.
    for (size_t i = 0; i < metadata.parquetMetaData().row_groups.size(); i++)
    {
        if (param.skip_row_groups.contains(i))
            continue;
        total_row_count += metadata.parquetMetaData().row_groups[i].num_rows;
        //        for (const auto & item : metadata.parquetMetaData().row_groups[i].columns)
        //        {
        //            std::cerr << item.meta_data.path_in_schema[0] << ":" << reinterpret_cast<const int64_t *>(item.meta_data.statistics.min_value.data())[0] << "\t" << reinterpret_cast<const int64_t *>(item.meta_data.statistics.max_value.data())[0] << std::endl;
        //            std::cerr << item.meta_data.path_in_schema[0] << ":" << reinterpret_cast<const double *>(item.meta_data.statistics.min_value.data())[0] << "\t" << reinterpret_cast<const double *>(item.meta_data.statistics.max_value.data())[0] << std::endl;
        //            std::cerr << item.meta_data.path_in_schema[0] << ":" << item.meta_data.statistics.min_value << "\t" << item.meta_data.statistics.max_value << std::endl;
        //
        //        }
        //        std::cerr <<std::endl;
    }
    row_group_size = metadata.parquetMetaData().row_groups.size();
}

std::shared_ptr<ParquetGroupReader> ParquetFileReader::getGroupReader(int id)
{
    return std::make_shared<ParquetGroupReader>(group_reader_param, id);
}

Chunk ParquetFileReader::getNext()
{
    while (cur_row_group_idx < row_group_size)
    {
        if (param.skip_row_groups.contains(cur_row_group_idx))
        {
            cur_row_group_idx++;
            continue;
        }
        if (!current_group_reader)
        {
            if (filterRowGroup(cur_row_group_idx))
            {
                cur_row_group_idx++;
                continue;
            }
            current_group_reader = getGroupReader(cur_row_group_idx);
            current_group_reader->init();
        }
        Chunk res = current_group_reader->getNext();
        if (res.hasRows())
        {
            scan_row_count += res.getNumRows();
            return res;
        }
        else
        {
            current_group_reader->close();
            current_group_reader.reset();
            cur_row_group_idx++;
        }
    }
    return {};
}

bool ParquetFileReader::readMinMaxBlock(const parquet::format::RowGroup & row_group, Block & minBlock, Block & maxBlock) const
{
    assertBlocksHaveEqualStructure(minBlock, maxBlock, "parquet read min max block");
    for (const auto & col : minBlock.getNames())
    {
        const auto * column_meta = getColumnMeta(row_group, col);
        if (column_meta == nullptr)
        {
            return false;
        }
        else if (!column_meta->__isset.statistics)
        {
            return false;
        }
        else
        {
            const ParquetField * field = metadata.schema().resolveByName(col);
            const parquet::format::ColumnOrder * column_order = nullptr;
            if (metadata.parquetMetaData().__isset.column_orders)
            {
                const auto & column_orders = metadata.parquetMetaData().column_orders;
                size_t column_idx = field->physical_column_index;
                column_order = column_idx < column_orders.size() ? &column_orders[column_idx] : nullptr;
            }

            bool decode_ok = decodeMinMaxColumn(
                *column_meta,
                column_order,
                minBlock.getByName(col).column->assumeMutable(),
                maxBlock.getByName(col).column->assumeMutable());
            if (!decode_ok)
            {
                return false;
            }
        }
    }
    return true;
}

bool ParquetFileReader::filterRowGroup(size_t id)
{
    if (param.groupFilter)
    {
        auto min_chunk = param.groupFilter->getArguments().cloneEmpty();
        auto max_chunk = param.groupFilter->getArguments().cloneEmpty();

        bool exist = readMinMaxBlock(metadata.parquetMetaData().row_groups[id], min_chunk, max_chunk);
        if (!exist)
        {
            return false;
        }
        auto min_column = param.groupFilter->execute(min_chunk);
        auto max_column = param.groupFilter->execute(max_chunk);
        int64_t min = min_column->get64(0);
        int64_t max = max_column->get64(0);

        if (min == 0 && max == 0)
        {
            return true;
        }
    }
    return false;
}
const parquet::format::ColumnMetaData *
ParquetFileReader::getColumnMeta(const parquet::format::RowGroup & row_group, const std::string & col_name)
{
    for (const auto & column : row_group.columns)
    {
        if (column.meta_data.path_in_schema[0] == col_name)
        {
            return &column.meta_data;
        }
    }
    return nullptr;
}
bool ParquetFileReader::decodeMinMaxColumn(
    const parquet::format::ColumnMetaData & column_meta,
    const parquet::format::ColumnOrder * column_order,
    MutableColumnPtr min_column,
    MutableColumnPtr max_column)
{
    if (!canUseMinMaxStats(column_meta, column_order))
    {
        return false;
    }

    switch (column_meta.type)
    {
        case parquet::format::Type::type::INT32: {
            int32_t min_value = 0;
            int32_t max_value = 0;
            if (column_meta.statistics.__isset.min_value)
            {
                PlainDecoder<int32_t>::decode(column_meta.statistics.min_value, &min_value);
                PlainDecoder<int32_t>::decode(column_meta.statistics.max_value, &max_value);
            }
            else
            {
                PlainDecoder<int32_t>::decode(column_meta.statistics.min, &min_value);
                PlainDecoder<int32_t>::decode(column_meta.statistics.max, &max_value);
            }
            min_column->insert(min_value);
            max_column->insert(max_value);
            break;
        }
        case parquet::format::Type::type::INT64: {
            int64_t min_value = 0;
            int64_t max_value = 0;
            if (column_meta.statistics.__isset.min_value)
            {
                PlainDecoder<int64_t>::decode(column_meta.statistics.min_value, &min_value);
                PlainDecoder<int64_t>::decode(column_meta.statistics.max_value, &max_value);
            }
            else
            {
                PlainDecoder<int64_t>::decode(column_meta.statistics.max, &max_value);
                PlainDecoder<int64_t>::decode(column_meta.statistics.min, &min_value);
            }
            min_column->insert(min_value);
            max_column->insert(max_value);
            break;
        }
        default:
            return false;
    }
    return true;
}
bool ParquetFileReader::canUseStats(const parquet::format::Type::type & type, const parquet::format::ColumnOrder * column_order)
{
    // If column order is not set, only statistics for numeric types can be trusted.
    if (column_order == nullptr)
    {
        // is boolean | is interger | is floating
        return type == parquet::format::Type::type::BOOLEAN || isIntegerType(type) || type == parquet::format::Type::type::DOUBLE;
    }
    // Stats can be used if the column order is TypeDefinedOrder (see parquet.thrift).
    return column_order->__isset.TYPE_ORDER;
}

bool ParquetFileReader::canUseDeprecatedStats(const parquet::format::Type::type & type, const parquet::format::ColumnOrder * column_order)
{
    // If column order is set to something other than TypeDefinedOrder, we shall not use the
    // stats (see parquet.thrift).
    if (column_order != nullptr && !column_order->__isset.TYPE_ORDER)
    {
        return false;
    }
    return type == parquet::format::Type::type::BOOLEAN || isIntegerType(type) || type == parquet::format::Type::type::DOUBLE;
}

bool ParquetFileReader::isIntegerType(const parquet::format::Type::type & type)
{
    return type == parquet::format::Type::type::INT32 || type == parquet::format::Type::type::INT64
        || type == parquet::format::Type::type::INT96;
}

bool ParquetFileReader::canUseMinMaxStats(
    const parquet::format::ColumnMetaData & column_meta, const parquet::format::ColumnOrder * column_order)
{
    if (column_meta.statistics.__isset.min_value && canUseStats(column_meta.type, column_order))
    {
        return true;
    }
    if (column_meta.statistics.__isset.min && canUseDeprecatedStats(column_meta.type, column_order))
    {
        return true;
    }
    return false;
}
}
