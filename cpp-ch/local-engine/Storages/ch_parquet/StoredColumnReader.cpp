#include "StoredColumnReader.h"
#include "ParquetColumnChunkReader.h"

namespace DB
{
size_t RequiredStoredColumnReader::readRecords(size_t num_rows, MutableColumnPtr & dst, bool values)
{
    size_t records_read = 0;
    while (records_read < num_rows)
    {
        if (num_values_left_in_cur_page == 0)
        {
            break;
        }

        size_t records_to_read = std::min(num_rows - records_read, num_values_left_in_cur_page);
        if (records_to_read == 0) [[unlikely]]
        {
            break;
        }
        reader->decode_values(records_to_read, dst, values);
        records_read += records_to_read;
        num_values_left_in_cur_page -= records_to_read;
    }
    return records_read;
}


void RequiredStoredColumnReader::init(const ParquetField * field_, const parquet::format::ColumnChunk * chunk_metadata_)
{
    field = field_;
    reader = std::make_shared<ParquetColumnChunkReader>(field->maxDefLevel(), field->maxRepLevel(), chunk_metadata_, opts);
    reader->init(opts.chunk_size);
}

bool RequiredStoredColumnReader::canUseMinMaxStatics()
{
    return reader->canUseMinMaxStats();
}

std::unique_ptr<StoredColumnReader> StoredColumnReader::create(
    const ColumnReaderOptions & opts, const ParquetField * field, const parquet::format::ColumnChunk * chunk_metadata)
{
    if (field->maxRepLevel() > 0)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RepeatedStoredColumnReader not supported");
    }
    else if (field->maxDefLevel() > 0)
    {
        auto reader = std::make_unique<OptionalStoredColumnReader>(opts);
        reader->init(field, chunk_metadata);
        return reader;
    }
    else
    {
        auto reader = std::make_unique<RequiredStoredColumnReader>(opts);
        reader->init(field, chunk_metadata);
        return reader;
    }
}

size_t StoredColumnReader::nextPage()
{
    if (num_values_left_in_cur_page == 0)
    {
        reader->loadHeader();
        if (reader->currentPageIsDict())
        {
            reader->loadPage();
            reader->skipPage();
            reader->loadHeader();
        }

        reader->loadPage();
        num_values_left_in_cur_page = reader->numValues();
    }
    return num_values_left_in_cur_page;
}
size_t StoredColumnReader::skipPage()
{
    reader->skipPage();
    auto skip_rows = num_values_left_in_cur_page;
    num_values_left_in_cur_page = 0;
    return skip_rows;
}

void StoredColumnReader::skipRows(size_t rows)
{
    auto skipped_rows = std::min(rows, num_values_left_in_cur_page);
    reader->skipRows(skipped_rows);
    num_values_left_in_cur_page = num_values_left_in_cur_page - skipped_rows;
}

std::pair<ColumnPtr, ColumnPtr> StoredColumnReader::readMinMaxColumn()
{
    return reader->readMinMaxColumn();
}

void OptionalStoredColumnReader::init(const ParquetField * field, const parquet::format::ColumnChunk * chunk_metadata)
{
    _field = field;
    reader = std::make_shared<ParquetColumnChunkReader>(_field->maxDefLevel(), _field->maxRepLevel(), chunk_metadata, opts);
    reader->init(opts.chunk_size);
}
size_t OptionalStoredColumnReader::_read_records_only(size_t num_rows, MutableColumnPtr & dst, bool values)
{
    size_t records_read = 0;
    while (records_read < num_rows)
    {
        if (num_values_left_in_cur_page == 0)
        {
            break;
        }
        size_t records_to_read = std::min(num_rows - records_read, num_values_left_in_cur_page);
        size_t repeated_count = reader->getDefLevelDecoder().nextRepeatedCount();
        if (repeated_count > 0)
        {
            records_to_read = std::min(records_to_read, repeated_count);
            level_t def_level = 0;
            def_level = reader->getDefLevelDecoder().get_repeated_value(records_to_read);
            if (def_level >= _field->maxDefLevel())
            {
                reader->decode_values(records_to_read, dst, values);
            }
            else
            {
                dst->insertManyDefaults(records_to_read);
            }
        }
        else
        {
            {
                size_t new_capacity = records_to_read;
                if (new_capacity > levels_capacity)
                {
                    def_levels.resize(new_capacity);

                    levels_capacity = new_capacity;
                }
                reader->decode_def_levels(records_to_read, def_levels.data());
            }

            size_t i = 0;
            while (i < records_to_read)
            {
                size_t j = i;
                bool is_null = def_levels[j] < _field->maxDefLevel();
                j++;
                while (j < records_to_read && is_null == (def_levels[j] < _field->maxDefLevel()))
                {
                    j++;
                }
                if (is_null)
                {
                    dst->insertManyDefaults(j - i);
                }
                else
                {
                    reader->decode_values(j - i, dst, values);
                }
                i = j;
            }
        }
        num_values_left_in_cur_page -= records_to_read;
        records_read += records_to_read;
    }

    return records_read;
}
void OptionalStoredColumnReader::reset()
{
    def_levels.resize_fill(def_levels.size());
}
//size_t OptionalStoredColumnReader::_read_records_and_levels(size_t , MutableColumnPtr & )
//{
//    return 0;
//}
}
