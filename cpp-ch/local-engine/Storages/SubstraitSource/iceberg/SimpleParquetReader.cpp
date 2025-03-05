/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "SimpleParquetReader.h"
#include <Poco/URI.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <Formats/FormatFactory.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>

using namespace DB;
namespace local_engine
{

namespace iceberg
{
substraitInputFile fromDeleteFile(const substraitIcebergDeleteFile & deleteFile)
{
    assert(deleteFile.filecontent() == IcebergReadOptions::EQUALITY_DELETES ||
           deleteFile.filecontent() == IcebergReadOptions::POSITION_DELETES);
    assert(deleteFile.has_parquet());
    substraitInputFile file;
    file.set_uri_file(deleteFile.filepath());
    file.set_start(0);
    file.set_length(deleteFile.filesize());
    return file;
}
}

SimpleParquetReader::SimpleParquetReader(const ContextPtr & context, const substraitInputFile & file_info)
{
    const Poco::URI file_uri{file_info.uri_file()};
    ReadBufferBuilderPtr read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context);
    read_buffer_ = read_buffer_builder->build(file_info);
    FormatSettings format_settings = getFormatSettings(context);
    ParquetMetaBuilder metaBuilder{
        .case_insensitive = format_settings.parquet.case_insensitive_column_matching,
        .allow_missing_columns = false,
        .collectPageIndex = true,
        .collectSchema = true};
    metaBuilder.build(*read_buffer_);
    std::atomic<int> is_stopped{0};
    auto arrow_file = asArrowFile(*read_buffer_, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);

    // TODO: set min_bytes_for_seek
    ParquetReader::Settings settings{
        .arrow_properties = parquet::ArrowReaderProperties(),
        .reader_properties = parquet::ReaderProperties(ArrowMemoryPool::instance()),
        .format_settings = format_settings};

    auto * seekable = dynamic_cast<SeekableReadBuffer *>(read_buffer_.get());
    assert(seekable != nullptr);
    seekable->seek(0, SEEK_SET);
    reader_ = std::make_shared<ParquetReader>(
        std::move(metaBuilder.fileHeader),
        *seekable,
        nullptr,
        settings,
        std::vector<int>{},
        metaBuilder.fileMetaData);
}

SimpleParquetReader::SimpleParquetReader(const DB::ContextPtr & context,
    const substraitIcebergDeleteFile & file_info): SimpleParquetReader(context, iceberg::fromDeleteFile(file_info))
{
}

SimpleParquetReader::~SimpleParquetReader() = default;

Block SimpleParquetReader::next() const
{
    return reader_->read();
}

}