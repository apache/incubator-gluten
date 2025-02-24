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

#include <filesystem>

#include <Core/Block.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Storages/Output/NormalFileWriter.h>
#include <Storages/SubstraitSource/FileReader.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Storages/SubstraitSource/ParquetFormatFile.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SubstraitSource/iceberg/EqualityDeleteFileReader.h>
#include <gtest/gtest.h>
#include <tests/utils/ReaderTestBase.h>
#include <tests/utils/TempFilePath.h>
#include <utils/QueryAssertions.h>
#include <utils/gluten_test_util.h>
#include <Common/QueryContext.h>

namespace local_engine
{
class ParquetFormatFile;
}

namespace DB::Setting
{
extern const SettingsUInt64 interactive_delay;
}

namespace local_engine::test
{

class IcebergTest : public ReaderTestBase
{
public:
    using substraitIcebergDeleteFile = substrait::ReadRel::LocalFiles::FileOrFiles::IcebergReadOptions::DeleteFile;

    static constexpr int rowCount = 20000;
private:

    std::string makeNotInList(const std::vector<int64_t>& deletePositionVector) {
        if (deletePositionVector.empty()) {
            return "";
        }

        return std::accumulate(
            deletePositionVector.begin() + 1,
            deletePositionVector.end(),
            std::to_string(deletePositionVector[0]),
            [](const std::string& a, int64_t b) {
              return a + ", " + std::to_string(b);
            });
    }

    std::string makePredicates(
      const std::vector<std::vector<int64_t>>& equalityDeleteVector,
      const std::vector<int32_t>& equalityFieldIds)
    {
        std::string predicates("");
        int32_t numDataColumns =
            *std::max_element(equalityFieldIds.begin(), equalityFieldIds.end());

        EXPECT_GT(numDataColumns, 0);
        EXPECT_GE(numDataColumns, equalityDeleteVector.size());
        EXPECT_GT(equalityDeleteVector.size(), 0);

        auto numDeletedValues = equalityDeleteVector[0].size();

        if (numDeletedValues == 0) {
            return predicates;
        }

        // If all values for a column are deleted, just return an always-false
        // predicate
        for (auto i = 0; i < equalityDeleteVector.size(); i++) {
            auto equalityFieldId = equalityFieldIds[i];
            auto deleteValues = equalityDeleteVector[i];

            auto lastIter = std::unique(deleteValues.begin(), deleteValues.end());
            auto numDistinctValues = lastIter - deleteValues.begin();
            auto minValue = 1;
            auto maxValue = *std::max_element(deleteValues.begin(), lastIter);
            if (maxValue - minValue + 1 == numDistinctValues &&
                maxValue == (rowCount - 1) / equalityFieldId) {
                return "1 = 0";
                }
        }

        if (equalityDeleteVector.size() == 1) {
            std::string name = fmt::format("c{}", equalityFieldIds[0] - 1);
            predicates = fmt::format(
                "{} NOT IN ({})", name, makeNotInList({equalityDeleteVector[0]}));
        } else {
            for (int i = 0; i < numDeletedValues; i++) {
                std::string oneRow("");
                for (int j = 0; j < equalityFieldIds.size(); j++) {
                    std::string name = fmt::format("c{}", equalityFieldIds[j] - 1);
                    std::string predicate =
                        fmt::format("({} <> {})", name, equalityDeleteVector[j][i]);

                    oneRow = oneRow == "" ? predicate
                                          : fmt::format("({} OR {})", oneRow, predicate);
                }

                predicates = predicates == ""
                    ? oneRow
                    : fmt::format("{} AND {}", predicates, oneRow);
            }
        }
        return predicates;
    }

    void assertEqualityDeletes(NormalFileReader & reader, const std::string& duckDbSql) const
    {
        auto msg = fmt::format("\nExpected result from running Clickhouse sql: {}", duckDbSql);
        EXPECT_TRUE(assertEqualResults(collectResult( reader), runClickhouseSQL(duckDbSql), msg));
    }

protected:
    std::unique_ptr<NormalFileReader> makeIcebergSplit(
        const std::string& dataFilePath,
        const DB::Block & sampleBlock,
        const std::vector<substraitIcebergDeleteFile>& deleteFiles = {})
    {
        substraitInputFile file_info = makeInputFile(dataFilePath, deleteFiles);
        const Poco::URI file_uri{file_info.uri_file()};

        ReadBufferBuilderPtr read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context_);
        auto format_file = FormatFileUtil::createFile(context_, read_buffer_builder, file_info);

        return NormalFileReader::create(format_file, sampleBlock, sampleBlock);
    }

    std::unique_ptr<NormalFileReader> makeIcebergSplit(
        const std::string& dataFilePath,
        const std::vector<substraitIcebergDeleteFile>& deleteFiles = {})
    {
        return makeIcebergSplit(dataFilePath, toSampleBlock(readParquetSchema(dataFilePath, getFormatSettings(context_))), deleteFiles);
    }

    substraitIcebergDeleteFile makeDeleteFile(const std::string & _path, uint64_t _recordCount, uint64_t _fileSizeInBytes)
    {
        substraitIcebergDeleteFile deleteFile;
        deleteFile.set_filecontent(substrait::ReadRel_LocalFiles_FileOrFiles_IcebergReadOptions_FileContent_EQUALITY_DELETES);
        deleteFile.set_filepath("file://" + _path);
        deleteFile.set_recordcount(_recordCount);
        deleteFile.set_filesize(_fileSizeInBytes);

        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        deleteFile.mutable_parquet()->CopyFrom(parquet_format);

        return deleteFile;
    }

    substraitInputFile makeInputFile(const std::string & _path, const std::vector<substraitIcebergDeleteFile> & _deleteFiles)
    {
        substraitInputFile file;
        file.set_uri_file("file://" + _path);
        file.set_start(0);
        file.set_length(std::filesystem::file_size(_path));

        substrait::ReadRel::LocalFiles::FileOrFiles::IcebergReadOptions iceberg_read_options;

        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        iceberg_read_options.mutable_parquet()->CopyFrom(parquet_format);

        iceberg_read_options.mutable_delete_files()->Reserve(_deleteFiles.size());

        for (const auto& del_file : _deleteFiles)
            iceberg_read_options.add_delete_files()->CopyFrom(del_file);

        file.mutable_iceberg()->CopyFrom(iceberg_read_options);

        return file;
    }
    void createDuckDbTable(const std::vector<DB::Block> & blocks)
    {
        createMemoryTableIfNotExists("IcebergTest", "tmp", blocks);
    }
public:

    void SetUp() override
    {
        ReaderTestBase::SetUp();
        /// we know all datas are not nullable
        context_->setSetting("schema_inference_make_columns_nullable", DB::Field("0"));
    }
    std::vector<std::shared_ptr<TempFilePath>> writeDataFiles(
      uint64_t numRows,
      int32_t numColumns = 1,
      int32_t splitCount = 1)
    {
        auto dataVectors = makeVectors(splitCount, numRows, numColumns);
        EXPECT_EQ(dataVectors.size(), splitCount);

        std::vector<std::shared_ptr<TempFilePath>> dataFilePaths;
        dataFilePaths.reserve(splitCount);

        for (auto i = 0; i < splitCount; i++) {
            dataFilePaths.emplace_back(TempFilePath::tmp("parquet"));
            writeToFile(dataFilePaths.back()->string(), dataVectors[i]);
        }

        createDuckDbTable(dataVectors);
        return dataFilePaths;
    }

    // TODO: rename duckDbSql => chDbSql
    void assertEqualityDeletes(
      const std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>&
          equalityDeleteVectorMap,
      const std::unordered_map<int8_t, std::vector<int32_t>>&
          equalityFieldIdsMap,
      std::string duckDbSql = "") {

        EXPECT_EQ(equalityDeleteVectorMap.size(), equalityFieldIdsMap.size());

        // We will create data vectors with numColumns number of columns that is the
        // max field Id in equalityFieldIds
        int32_t numDataColumns = 0;

        for (auto it = equalityFieldIdsMap.begin(); it != equalityFieldIdsMap.end(); ++it) {
            auto equalityFieldIds = it->second;
            auto currentMax =
                *std::max_element(equalityFieldIds.begin(), equalityFieldIds.end());
            numDataColumns = std::max(numDataColumns, currentMax);
        }

        EXPECT_GT(numDataColumns, 0);
        EXPECT_GE(numDataColumns, equalityDeleteVectorMap.size());
        EXPECT_GT(equalityDeleteVectorMap.size(), 0);

        EXPECT_LE(equalityFieldIdsMap.size(), numDataColumns);

        std::shared_ptr<TempFilePath> dataFilePath =
            writeDataFiles(rowCount, numDataColumns)[0];

        std::vector<substraitIcebergDeleteFile> deleteFiles;
        std::string predicates = "";
        unsigned long numDeletedValues = 0;

        std::vector<std::shared_ptr<TempFilePath>> deleteFilePaths;
        for (auto it = equalityFieldIdsMap.begin();
             it != equalityFieldIdsMap.end();)
        {
            auto equalityFieldIds = it->second;
            const auto& equalityDeleteVector = equalityDeleteVectorMap.at(it->first);
            EXPECT_GT(equalityDeleteVector.size(), 0);
            numDeletedValues =
                std::max(numDeletedValues, equalityDeleteVector[0].size());

            deleteFilePaths.push_back(writeEqualityDeleteFile(equalityDeleteVector));
            deleteFiles.push_back(
                makeDeleteFile(deleteFilePaths.back()->string(),
                    equalityDeleteVector[0].size(),
                    testing::internal::GetFileSize(std::fopen(deleteFilePaths.back()->string().c_str(), "r"))));
            predicates += makePredicates(equalityDeleteVector, equalityFieldIds);
            ++it;
            if (it != equalityFieldIdsMap.end()) {
                predicates += " AND ";
            }
        }

        auto icebergSplit = makeIcebergSplit(dataFilePath->string(), deleteFiles);

        // If the caller passed in a query, use that.
        if (duckDbSql == "") {
            // Select all columns
            duckDbSql = "SELECT * FROM IcebergTest.tmp ";
            if (numDeletedValues > 0) {
                duckDbSql += fmt::format("WHERE {}", predicates);
            }
        }

        assertEqualityDeletes(*icebergSplit, duckDbSql);
    }

    std::shared_ptr<TempFilePath> writeEqualityDeleteFile(
      const std::vector<std::vector<int64_t>>& equalityDeleteVector)
    {
        DB::ColumnsWithTypeAndName columns;

        for (int i = 0; i < equalityDeleteVector.size(); i++)
            columns.emplace_back(createColumn(equalityDeleteVector[i], fmt::format("c{}", i)));


        auto deleteFilePath = TempFilePath::tmp("parquet");
        writeToFile(deleteFilePath->string(), DB::Block(columns));
        return deleteFilePath;
    }

    DB::ColumnPtr makeSequenceValues(int32_t numRows, int8_t repeat = 1)
    {
        EXPECT_GT(repeat, 0);
        auto maxValue = std::ceil(static_cast<double>(numRows) / repeat);
        auto column = DB::ColumnInt64::create();
        column->reserve(numRows);

        DB::ColumnInt64::Container & values = column->getData();
        EXPECT_EQ(values.size(), 0);

        int32_t inserted = 0;
        for (int32_t i = 0; i < maxValue && inserted < numRows; i++) {
            for (int8_t j = 0; j < repeat && inserted < numRows; j++, inserted++)
                values.push_back(i);
        }

        EXPECT_EQ(column->size(), numRows);
        return column;
    }

    std::vector<DB::Block> makeVectors(int32_t count, int32_t rowsPerBlock, int32_t numColumns = 1)
    {

        std::vector<DB::Block> rowVectors;
        for (int i = 0; i < count; i++)
        {
            DB::ColumnsWithTypeAndName columns;
            for (int j = 0; j < numColumns; j++)
            {
                std::string name = fmt::format("c{}", j);

                // Create the column values like below:
                // c0 c1 c2
                //  0  0  0
                //  1  0  0
                //  2  1  0
                //  3  1  1
                //  4  2  1
                //  5  2  1
                //  6  3  2
                // ...
                // In the first column c0, the values are continuously increasing and not
                // repeating. In the second column c1, the values are continuously
                // increasing and each value repeats once. And so on.
                auto data = makeSequenceValues(rowsPerBlock, j + 1);
                columns.emplace_back(DB::ColumnWithTypeAndName(data->getPtr(), BIGINT(), name));
            }
            rowVectors.push_back(DB::Block(columns));
        }
        return rowVectors;
    }
};


TEST_F(IcebergTest, tmp2)
{
    std::shared_ptr<TempFilePath> dataFilePath =
        writeDataFiles(rowCount, 4)[0];

    DB::Block block = runClickhouseSQL("select count(*) from IcebergTest.tmp");
    EXPECT_TRUE(assertEqualResults(block, DB::Block{createColumn<UInt64>({rowCount}, "count()")}));


    auto read = makeIcebergSplit(dataFilePath->string());
    DB::Block actual = collectResult( *read);
    EXPECT_TRUE(assertEqualResults( actual, runClickhouseSQL("select * from IcebergTest.tmp")));
}

TEST_F(IcebergTest, tmp)
{
    std::vector<std::vector<int64_t>> equalityDeleteVector = {{0, 1}, {0, 0}};
    auto tmpFile = writeEqualityDeleteFile(equalityDeleteVector);
    auto deletedFile = makeDeleteFile(tmpFile->string(),
        equalityDeleteVector[0].size(),
        testing::internal::GetFileSize(std::fopen(tmpFile->string().c_str(), "r")));

    auto context = QueryContext::instance().currentQueryContext();
    iceberg::EqualityDeleteFileReader reader(context, deletedFile);

    std::vector<DB::Block> dataBlock {makeVectors(1, rowCount, 2)};
    DB::ASTPtr result = reader.readDeleteValues();


    auto syntax_result = DB::TreeRewriter(context).analyze(result, dataBlock[0].getNamesAndTypesList());
    auto partition_by_expr = DB::ExpressionAnalyzer(result, syntax_result, context).getActions(false);
    auto partition_by_column_name = result->getColumnName();

    auto resultBlock{dataBlock[0]};
    partition_by_expr->execute(resultBlock);
    headBlock(resultBlock, 20 ,100);
}

TEST_F(IcebergTest, equalityDeletesSingleFileMultipleColumns)
{
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({0, {1, 2}});

    // Delete rows 0, 1
    equalityDeleteVectorMap.insert({0, {{0, 1}, {0, 0}}});

    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);
}

}
