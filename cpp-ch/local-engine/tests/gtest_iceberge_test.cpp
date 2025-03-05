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

#include <Interpreters/executeQuery.h>
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
#include <Common/DebugUtils.h>
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

    void assertEqualityDeletes(BaseReader & reader, const std::string& duckDbSql) const
    {
        auto msg = fmt::format("\nExpected result from running Clickhouse sql: {}", duckDbSql);
        EXPECT_TRUE(assertEqualResults(collectResult( reader), runClickhouseSQL(duckDbSql), msg));
    }

protected:
    std::unique_ptr<BaseReader> makeIcebergSplit(
        const std::string& dataFilePath,
        const DB::Block & sampleBlock,
        const std::vector<substraitIcebergDeleteFile>& deleteFiles = {})
    {
        substraitInputFile file_info = makeInputFile(dataFilePath, deleteFiles);
        const Poco::URI file_uri{file_info.uri_file()};

        ReadBufferBuilderPtr read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context_);
        auto format_file = FormatFileUtil::createFile(context_, read_buffer_builder, file_info);

        return BaseReader::create(format_file, sampleBlock, sampleBlock, nullptr, nullptr);
    }

    std::unique_ptr<BaseReader> makeIcebergSplit(
        const std::string& dataFilePath,
        const std::vector<substraitIcebergDeleteFile>& deleteFiles = {})
    {
        return makeIcebergSplit(dataFilePath, toSampleBlock(readParquetSchema(dataFilePath, getFormatSettings(context_))), deleteFiles);
    }

    substraitIcebergDeleteFile makeDeleteFile(
        const std::string & _path,
        uint64_t _recordCount,
        uint64_t _fileSizeInBytes,
        std::vector<int32_t> _equalityFieldIds = {},
        std::unordered_map<int32_t, std::string> _lowerBounds = {},
        std::unordered_map<int32_t, std::string> _upperBounds = {} )
    {
        substraitIcebergDeleteFile deleteFile;
        deleteFile.set_filecontent(substrait::ReadRel_LocalFiles_FileOrFiles_IcebergReadOptions_FileContent_EQUALITY_DELETES);
        deleteFile.set_filepath("file://" + _path);
        deleteFile.set_recordcount(_recordCount);
        deleteFile.set_filesize(_fileSizeInBytes);

        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        deleteFile.mutable_parquet()->CopyFrom(parquet_format);
        for (const auto& fieldId : _equalityFieldIds)
            deleteFile.add_equalityfieldids(fieldId);

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
                    testing::internal::GetFileSize(std::fopen(deleteFilePaths.back()->string().c_str(), "r")),
                    equalityFieldIds));
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

        // TODO: Select a column that's not in the filter columns
        // if (numDataColumns > 1 &&
        //     equalityDeleteVectorMap.at(0).size() < numDataColumns) {
        //     std::string duckDbSql1 = "SELECT c0 FROM IcebergTest.tmp";
        //     if (numDeletedValues > 0) {
        //         duckDbSql += fmt::format(" WHERE {}", predicates);
        //     }
        //
        //     auto icebergSplit1 = makeIcebergSplit(dataFilePath->string(),
        //         DB::Block{DB::ColumnWithTypeAndName(BIGINT(),"c0")},
        //         deleteFiles);
        //
        //     assertEqualityDeletes(*icebergSplit1, duckDbSql1);
        // }
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

    std::vector<int64_t> makeSequenceValues(int32_t numRows, int8_t repeat = 1)
    {
        EXPECT_GT(repeat, 0);

        auto maxValue = std::ceil((double)numRows / repeat);
        std::vector<int64_t> values;
        values.reserve(numRows);
        for (int32_t i = 0; i < maxValue; i++) {
            for (int8_t j = 0; j < repeat; j++) {
                values.push_back(i);
            }
        }
        values.resize(numRows);
        return values;
    }

    std::vector<int64_t> makeRandomDeleteValues(int32_t maxRowNumber) {
        std::mt19937 gen{0};
        std::vector<int64_t> deleteRows;
        for (int i = 0; i < maxRowNumber; i++) {
            if (std::uniform_int_distribution<uint32_t>(0, 9)(gen) > 8) {
                deleteRows.push_back(i);
            }
        }
        return deleteRows;
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
                columns.emplace_back(createColumn(data, name));
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

    runClickhouseSQL(fmt::format("select count(*) from file('{}')", dataFilePath->string()));
    DB::Block block = runClickhouseSQL("select count(*) from IcebergTest.tmp");
    EXPECT_TRUE(assertEqualResults(block, DB::Block{createColumn<UInt64>({rowCount}, "count()")}));


    auto read = makeIcebergSplit(dataFilePath->string());
    DB::Block actual = collectResult( *read);
    EXPECT_TRUE(assertEqualResults( actual, runClickhouseSQL("select * from IcebergTest.tmp")));
}

TEST_F(IcebergTest, EqualityDeleteActionBuilder)
{
    std::vector<DB::Block> dataBlock {makeVectors(1, rowCount, 3)};
    DB::Block & resultBlock = dataBlock[0];

    iceberg::EqualityDeleteActionBuilder actions{context_, resultBlock.getNamesAndTypesList()};
    actions.notIn(DB::Block{createColumn<int64_t>({0, 1}, "c0")});
    actions.notIn(DB::Block{createColumn<int64_t>({4, 5}, "c0")});
    actions.notEquals(DB::Block{
        createColumn<int64_t>({0, 1}, "c0"),
        createColumn<int64_t>({0, 0}, "c1"),
        createColumn<int64_t>({0, 0}, "c2")
        });
    auto x = actions.finish();
    LOG_INFO(test_logger, "\n{}", debug::dumpActionsDAG(x->getActionsDAG()));
    x->execute(resultBlock);
    headBlock(resultBlock, 20 ,100);
}

// Delete values from a single column file
TEST_F(IcebergTest, equalityDeletesSingleFileColumn1)
{
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({0, {1}});

    // Delete row 0, 1, 2, 3 from the first batch out of two.
    equalityDeleteVectorMap.insert({0, {{0, 1, 2, 3}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete the first and last row in each batch (10000 rows per batch)
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{0, 9999, 10000, 19999}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete several rows in the second batch (10000 rows per batch)
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{10000, 10002, 19999}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete random rows
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {makeRandomDeleteValues(rowCount)}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete 0 rows
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete all rows
    equalityDeleteVectorMap.insert({0, {makeSequenceValues(rowCount)}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete rows that don't exist
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{20000, 29999}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);
}

// Delete values from the second column in a 2-column file
//
//    c1    c2
//    0     0
//    1     0
//    2     1
//    3     1
//    4     2
//  ...    ...
//  19999 9999
TEST_F(IcebergTest, equalityDeletesSingleFileColumn2) {

    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({0, {2}});

    // Delete values 0, 1, 2, 3 from the second column
    equalityDeleteVectorMap.insert({0, {{0, 1, 2, 3}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete the smallest value 0 and the largest value 9999 from the second
    // column, which has the range [0, 9999]
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{0, 9999}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete non-existent values from the second column
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{10000, 10002, 19999}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete random rows from the second column
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {makeSequenceValues(rowCount)}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    //     Delete 0 values
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete all values
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {makeSequenceValues(rowCount / 2)}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);
}

// Delete values from 2 columns with the following data:
//
//    c1    c2
//    0     0
//    1     0
//    2     1
//    3     1
//    4     2
//  ...    ...
//  19999 9999
TEST_F(IcebergTest, equalityDeletesSingleFileMultipleColumns)
{
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({0, {1, 2}});

    // Delete rows 0, 1
    equalityDeleteVectorMap.insert({0, {{0, 1}, {0, 0}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete rows 0, 2, 4, 6
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{0, 2, 4, 6}, {0, 1, 2, 3}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    //   Delete the last row
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{19999}, {9999}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete non-existent values
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{20000, 30000}, {10000, 1500}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete 0 values
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({0, {{}, {}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);


#ifdef NDEBUG
    // Delete all values
    // very slow in debug build
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert(
        {0, {makeSequenceValues(rowCount), makeSequenceValues(rowCount, 2)}});
    assertEqualityDeletes(
        equalityDeleteVectorMap,
        equalityFieldIdsMap,
        "SELECT * FROM IcebergTest.tmp WHERE 1 = 0");
#endif
}

TEST_F(IcebergTest, equalityDeletesMultipleFiles) {
    std::unordered_map<int8_t, std::vector<int32_t>> equalityFieldIdsMap;
    std::unordered_map<int8_t, std::vector<std::vector<int64_t>>>
        equalityDeleteVectorMap;
    equalityFieldIdsMap.insert({{0, {1}}, {1, {2}}});

    // Delete rows {0, 1} from c0, {2, 3} from c1, with two equality delete files
    equalityDeleteVectorMap.insert({{0, {{0, 1}}}, {1, {{2, 3}}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete using 3 equality delete files
    equalityFieldIdsMap.insert({{2, {3}}});
    equalityDeleteVectorMap.insert({{0, {{0, 1}}}, {1, {{2, 3}}}, {2, {{4, 5}}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete 0 values
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert({{0, {{}}}, {1, {{}}}, {2, {{}}}});
    assertEqualityDeletes(equalityDeleteVectorMap, equalityFieldIdsMap);

    // Delete all values
    equalityDeleteVectorMap.clear();
    equalityDeleteVectorMap.insert(
        {{0, {makeSequenceValues(rowCount)}},
         {1, {makeSequenceValues(rowCount)}},
         {2, {makeSequenceValues(rowCount)}}});
    assertEqualityDeletes(
        equalityDeleteVectorMap,
        equalityFieldIdsMap,
        "SELECT * FROM IcebergTest.tmp WHERE 1 = 0");
}

}
