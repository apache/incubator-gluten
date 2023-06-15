#include "metadata.h"

namespace DB
{
void FileMetaData::init(const parquet::format::FileMetaData& metadata) {
    // construct schema from thrift
    schema_.fromThrift(metadata.schema);
    num_rows = metadata.num_rows;
    parquet_metadata = metadata;
}

std::string FileMetaData::debug_string() const {
    std::stringstream ss;
    ss << "schema=" << schema_.debugString();
    return ss.str();
}
}
