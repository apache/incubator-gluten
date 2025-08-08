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
#include <IO/ReadBufferFromFile.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReader.h>
#include <Processors/Formats/Impl/Parquet/ParquetLeafColReader.h>
#include <Processors/Formats/Impl/Parquet/ParquetRecordReader.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <gtest/gtest.h>
#include <tests/utils/gluten_test_util.h>


using namespace DB;
using namespace local_engine;

TEST(ParquetColumnReader, ParquetFileReaderExt)
{
    const std::string sample(test::gtest_data("sample.parquet"));
    const FormatSettings format_settings{};
    ReadBufferFromFile in(sample);
    auto arrow_file = test::asArrowFileForParquet(in, format_settings);

    ParquetMetaBuilder metaBuilder{.collectPageIndex = true};
    Block blockHeader({{DOUBLE(), "b"}, {BIGINT(), "a"}});
    metaBuilder.build(in, blockHeader);
    ColumnIndexRowRangesProvider provider{metaBuilder};

    ParquetFileReaderExt test{
        arrow_file, parquet::ParquetFileReader::Open(arrow_file, parquet::default_reader_properties()), provider, format_settings};

    auto & file_reader = *test.fileReader();

    constexpr int col_a = 0;
    constexpr int rowGroup = 0;

    auto cur_row_group_reader = file_reader.RowGroup(rowGroup);
    std::unique_ptr<parquet::ColumnChunkMetaData> meta = cur_row_group_reader->metadata()->ColumnChunk(col_a);
    const parquet::ColumnDescriptor & col_descriptor = *file_reader.metadata()->schema()->Column(col_a);
    auto readState = test.nextRowGroup(rowGroup, col_a, "___");
    ASSERT_TRUE(readState.has_value());

    auto reader = ParquetRecordReaderFactory::makeReader(col_descriptor, BIGINT(), std::move(meta), std::move(readState.value().first));
    EXPECT_TRUE(reader);

    reader->reserve(10000);

    /// Note ParquetColumnReader requires that the number of records read should not
    /// exceed the number of records in the row group
    constexpr int skip_records = 2;
    reader->skipRecords(skip_records);
    EXPECT_EQ(reader->readRecords(2), 2);
    reader->skipRecords(skip_records);
    EXPECT_EQ(reader->readRecords(4), 4);
    auto x = reader->resetColumn("a");
    const auto & _a = *x.column;
    for (size_t i = 0; i < _a.size(); i++)
        EXPECT_EQ(_a.get64(i) - skip_records, i + 1);
}

TEST(ParquetColumnReader, SkipInterface)
{
    const std::string sample(GLUTEN_SOURCE_TPCH_DIR("lineitem/part-00000-d08071cb-0dfa-42dc-9198-83cb334ccda3-c000.snappy.parquet"));
    ReadBufferFromFile in(sample);
    const FormatSettings format_settings{};
    auto arrow_file = test::asArrowFileForParquet(in, format_settings);
    auto sampleBlock = toSampleBlock(test::readParquetSchema(sample));

    constexpr int rowGroup = 0;

    ParquetMetaBuilder metaBuilder{.collectPageIndex = true};
    metaBuilder.build(in, sampleBlock);
    ColumnIndexRowRangesProvider provider{metaBuilder};

    ParquetFileReaderExt test{
        arrow_file, parquet::ParquetFileReader::Open(arrow_file, parquet::default_reader_properties()), provider, format_settings};
    auto & file_reader = *test.fileReader();
    auto cur_row_group_reader = file_reader.RowGroup(rowGroup);


    for (size_t i = 0; i < sampleBlock.columns(); i++)
    {
        const ColumnWithTypeAndName & col_with_name = sampleBlock.getByPosition(i);
        const parquet::ColumnDescriptor & col_descriptor = *file_reader.metadata()->schema()->Column(i);
        std::unique_ptr<parquet::ColumnChunkMetaData> meta = cur_row_group_reader->metadata()->ColumnChunk(i);
        auto readState = test.nextRowGroup(rowGroup, i, "___");
        ASSERT_TRUE(readState.has_value());
        auto reader = ParquetRecordReaderFactory::makeReader(col_descriptor, col_with_name.type, std::move(meta), std::move(readState.value().first));
        EXPECT_TRUE(reader);
        reader->skipRecords(1);
    }

}