#include "ParquetColumnReader.h"
#include <DataTypes/DataTypeNested.h>


namespace DB
{

std::unique_ptr<ParquetColumnReader> ParquetColumnReader::create(const ColumnReaderOptions& opts, const ParquetField* field) {
    if (isArray(field->type)) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "array not supported");
    } else if (isMap(field->type)) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "map not supported");

    } else if (isNested(field->type)) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "struct not supported");

    } else {
        auto reader = std::make_unique<ScalarColumnReader>(opts);
        reader->init(field, &opts.row_group_meta->columns[field->physical_column_index]);
        return reader;
    }
}
}
