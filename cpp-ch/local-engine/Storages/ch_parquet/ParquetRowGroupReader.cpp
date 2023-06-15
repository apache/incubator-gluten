#include "ParquetRowGroupReader.h"
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/FilterDescription.h>

namespace DB
{
ParquetGroupReader::ParquetGroupReader(ParquetGroupReaderParam & param_, int row_group_number) : param(param_)
{
    row_group_metadata = std::make_shared<parquet::format::RowGroup>(param.file_metadata->parquetMetaData().row_groups[row_group_number]);
}

void ParquetGroupReader::init()
{
    initColumnReaders();
    for (const auto & item : param.output_cols)
    {
        active_column_indices.emplace_back(item.col_idx_in_chunk);
    }
}

Chunk ParquetGroupReader::getNext()
{
    return read(active_column_indices, param.chunk_size);
}

void ParquetGroupReader::close()
{
    column_readers.clear();
}

void ParquetGroupReader::initColumnReaders()
{
    ColumnReaderOptions & opts = column_reader_opts;
    opts.timezone = param.timezone;
    opts.case_sensitive = param.case_sensitive;
    opts.chunk_size = param.chunk_size;
    opts.stream = param.file_read_buffer;
    opts.row_group_meta = row_group_metadata.get();

    for (const auto & column : param.read_cols)
    {
        createColumnReader(column);
    }
}

void ParquetGroupReader::createColumnReader(const ParquetGroupReaderParam::Column & column)
{
    const auto * schema_node = param.file_metadata->schema().getStoredColumnByIdx(column.col_idx_in_parquet);
    std::unique_ptr<ParquetColumnReader> column_reader = ParquetColumnReader::create(column_reader_opts, schema_node);
    column_readers[column.col_idx_in_chunk] = std::move(column_reader);
    column_name_to_idx[schema_node->name] = column.col_idx_in_chunk;
    column_idx_to_name[column.col_idx_in_chunk] = schema_node->name;
}

bool ParquetGroupReader::filterPage()
{
    if (!param.page_filter)
        return false;
    auto conditions = param.page_filter->getAllConditions();
    std::vector<DB::String> active_conditions;
    Block min_columns;
    Block max_columns;
    for (const auto & condition : conditions)
    {
        chassert(column_name_to_idx.contains(condition));
        auto * reader = column_readers[column_name_to_idx[condition]].get();
        if (reader->canUseMinMaxStatics())
        {
            active_conditions.emplace_back(condition);
            auto min_max_columns = reader->readMinMaxColumn();
            min_columns.insert({min_max_columns.first, reader->getStatsType(), condition});
            max_columns.insert({min_max_columns.second, reader->getStatsType(), condition});
        }
    }
    if (min_columns.columns() == 0)
        return false;
    auto min_column = param.filter->execute(min_columns);
    auto max_column = param.filter->execute(max_columns);
    int64_t min = min_column->get64(0);
    int64_t max = max_column->get64(0);

    if (min == 0 && max == 0)
    {
        return true;
    }
    return false;
}

std::pair<bool, ColumnPtr> ParquetGroupReader::readColumn(int idx, size_t row_count, bool force_values)
{
    column_readers[idx]->nextPage();
    static DataTypePtr dict_code_type = std::make_shared<DataTypeUInt32>();
    auto column = param.read_cols[idx];
    MutableColumnPtr column_vector;
    bool is_dict = column_readers[idx]->currentIsDict();
    auto values = !is_dict || force_values;
    if (values)
    {
        column_vector = column.col_type_in_chunk->createColumn();
    }
    else
    {
        column_vector = dict_code_type->createColumn();
    }
    try
    {
        column_vector->reserve(row_count);
        while (column_vector->size() < row_count)
        {
            auto old_size = column_vector->size();
            column_readers[idx]->next_batch(row_count - column_vector->size(), column_vector, values);
            if (column_vector->size() - old_size == 0)
            {
                column_readers[idx]->nextPage();
                if (is_dict && !column_readers[idx]->currentIsDict() && !values)
                {
                    // convert dict code to values, because page has different encoding
                    MutableColumnPtr dict_vector = std::move(column_vector);
                    const auto & dict_data = static_cast<const ColumnVector<UInt32> *>(dict_vector.get())->getData();
                    column_vector = column.col_type_in_chunk->createColumn();
                    column_vector->reserve(row_count);
                    column_readers[idx]->getDictValues(dict_data, column_vector);
                    values = true;
                }
            }
        }
    }
    catch (EndOfFile &)
    {
        end = true;
    }
    return std::pair<bool, ColumnPtr>(values, std::move(column_vector));
}

Chunk ParquetGroupReader::read(const std::vector<int> & read_columns, size_t row_count)
{
    if (read_columns.empty())
    {
        return {};
    }
    MutableColumns columns;
    std::vector<int> non_conditional_columns;
    std::unordered_map<int, int> idx_in_chunk_to_result_idx_map;
    int result_idx = 0;
    for (int col_idx : read_columns)
    {
        auto & column = param.read_cols[col_idx];
        if (param.filter && !param.filter->getConditionColumns().contains(column_idx_to_name[column.col_idx_in_chunk]))
        {
            non_conditional_columns.emplace_back(column.col_idx_in_chunk);
        }
        columns.emplace_back(column.col_type_in_chunk->createColumn());
        idx_in_chunk_to_result_idx_map[column.col_idx_in_chunk] = result_idx;
        result_idx ++;
    }
    if (end)
    {
        return Chunk(std::move(columns), 0);
    }
    //            if (filterPage())
    //            {
    //                for (int read_column : read_columns)
    //                {
    //                    auto & column = param.read_cols[read_column];
    //                    column_readers[column.col_idx_in_chunk]->skipPage();
    //                }
    //            }
    if (param.filter)
    {
        Block condition_input;
        for (const auto & item : param.filter->getConditionColumns())
        {
            auto idx_in_chunk = column_name_to_idx[item];
            auto col = readColumn(idx_in_chunk, row_count, true);
            condition_input.insert({col.second, param.read_cols[idx_in_chunk].col_type_in_chunk, item});
        }
        auto selection = param.filter->execute(condition_input);
        const auto & filter = checkAndGetColumn<ColumnVector<UInt8>>(*selection)->getData();
        ssize_t filtered_count = countBytesInFilter(filter);
        bool need_filter = filtered_count != static_cast<ssize_t>(selection->size());
        FilterDescription filterDescription(*selection.get());
        for (const auto & item : condition_input.getColumnsWithTypeAndName())
        {
            if (column_name_to_idx.contains(item.name) && idx_in_chunk_to_result_idx_map.contains(column_name_to_idx[item.name]))
            {
                if (need_filter)
                {
                    auto filtered_column = filterDescription.filter(*item.column, filtered_count);
                    columns[idx_in_chunk_to_result_idx_map[column_name_to_idx[item.name]]] = filtered_column->assumeMutable();
                }
                else
                {
                    columns[idx_in_chunk_to_result_idx_map[column_name_to_idx[item.name]]] = item.column->assumeMutable();
                }
            }
        }
        for (const auto & item : non_conditional_columns)
        {

            if (need_filter)
            {
                auto column = readColumn(item, row_count, false);
                auto filtered_column = filterDescription.filter(*column.second, filtered_count);
                if (column.first)
                {
                    columns[idx_in_chunk_to_result_idx_map[item]] = filtered_column->assumeMutable();
                }
                else
                {
                    auto res = param.read_cols[item].col_type_in_chunk->createColumn();
                    res->reserve(filtered_count);
                    const auto & filtered_data = static_cast<const ColumnVector<UInt32> *>(filtered_column.get())->getData();
                    column_readers[item]->getDictValues(filtered_data, res);
                    columns[idx_in_chunk_to_result_idx_map[item]] = std::move(res);
                }
            }
            else
            {
                auto column = readColumn(item, row_count, true);
                columns[idx_in_chunk_to_result_idx_map[item]] = column.second->assumeMutable();
            }
        }
    }
    else
    {
        columns.clear();
        for (int read_column : read_columns)
        {
            auto col = readColumn(read_column, row_count, true);
            columns.emplace_back(col.second->assumeMutable());
        }
    }
    auto rows = columns.front()->size();
    return Chunk(std::move(columns), rows);
}
}
