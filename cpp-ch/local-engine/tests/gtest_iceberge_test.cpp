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
#include <Storages/SubstraitSource/Iceberg/EqualityDeleteFileReader.h>
#include <Storages/SubstraitSource/Iceberg/IcebergMetadataColumn.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <gtest/gtest.h>
#include <tests/utils/QueryAssertions.h>
#include <tests/utils/ReaderTestBase.h>
#include <tests/utils/TempFilePath.h>
#include <tests/utils/gluten_test_util.h>
#include <Common/DebugUtils.h>

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
    static constexpr int rowCount = 20000;

    std::shared_ptr<iceberg::IcebergMetadataColumn> pathColumn_ =
    iceberg::IcebergMetadataColumn::icebergDeleteFilePathColumn();
    std::shared_ptr<iceberg::IcebergMetadataColumn> posColumn_ =
        iceberg::IcebergMetadataColumn::icebergDeletePosColumn();

protected:
    /// Input is like <"deleteFile1", <"dataFile1", {pos_RG1, pos_RG2,..}>,
    /// <"dataFile2", {pos_RG1, pos_RG2,..}>
    std::unordered_map<std::string, std::pair<int64_t, std::shared_ptr<TempFilePath>>>
    writePositionDeleteFiles(
      const std::unordered_map<
          std::string, // delete file name
          std::multimap<
              std::string,
              std::vector<int64_t>>>&
          deleteFilesForBaseDatafiles, // <base file name, delete position
                                       // vector for all RowGroups>
      std::map<std::string, std::shared_ptr<TempFilePath>> baseFilePaths)
    {
        std::unordered_map<std::string, std::pair<int64_t, std::shared_ptr<TempFilePath>>> deleteFilePaths;
        deleteFilePaths.reserve(deleteFilesForBaseDatafiles.size());

        for (const auto& deleteFile : deleteFilesForBaseDatafiles)
        {
            auto deleteFileName = deleteFile.first;
            auto deleteFileContent = deleteFile.second;
            auto deleteFilePath = TempFilePath::tmp("parquet");

            std::vector<DB::Block> deleteFileVectors;
            int64_t totalPositionsInDeleteFile = 0;
            for (auto& deleteFileRowGroup : deleteFileContent)
            {
                auto baseFileName = deleteFileRowGroup.first;
                // TODO: check baseFilePath using URI format
                auto baseFilePath = "file://" + baseFilePaths[baseFileName]->string();
                auto positionsInRowGroup = deleteFileRowGroup.second;

                auto filePathVector = createColumn<std::string>(positionsInRowGroup.size(),
                    [&](size_t /*row*/) { return baseFilePath; });
                auto deletePosVector = createColumn<int64_t>(positionsInRowGroup);

                DB::Block deleteFileVector {
                    {filePathVector, pathColumn_->type, pathColumn_->name},
                    {deletePosVector, posColumn_->type, posColumn_->name}
                };

                deleteFileVectors.push_back(deleteFileVector);
                totalPositionsInDeleteFile += positionsInRowGroup.size();
            }

            writeToFile(deleteFilePath->string(),deleteFileVectors);

            deleteFilePaths[deleteFileName] = std::make_pair(totalPositionsInDeleteFile, deleteFilePath);
        }
        return deleteFilePaths;
    }

    std::string getDuckDBQuery(
      const std::map<std::string, std::vector<int64_t>>& rowGroupSizesForFiles,
      const std::unordered_map<
          std::string,
          std::multimap<std::string, std::vector<int64_t>>>&
          deleteFilesForBaseDatafiles)
    {
        int64_t totalNumRowsInAllBaseFiles = 0;
        std::map<std::string, int64_t> baseFileSizes;
        for (auto rowGroupSizesInFile : rowGroupSizesForFiles)
        {
            // Sum up the row counts in all RowGroups in each base file
            baseFileSizes[rowGroupSizesInFile.first] += std::accumulate(
                rowGroupSizesInFile.second.begin(),
                rowGroupSizesInFile.second.end(),
                0LL);
            totalNumRowsInAllBaseFiles += baseFileSizes[rowGroupSizesInFile.first];
        }

        // Group the delete vectors by baseFileName
        std::map<std::string, std::vector<std::vector<int64_t>>> deletePosVectorsForAllBaseFiles;

        for (auto deleteFile : deleteFilesForBaseDatafiles)
        {
            auto deleteFileContent = deleteFile.second;
            for (auto rowGroup : deleteFileContent)
            {
                auto baseFileName = rowGroup.first;
                deletePosVectorsForAllBaseFiles[baseFileName].push_back(rowGroup.second);
            }
        }

        // Flatten and deduplicate the delete position vectors in
        // deletePosVectorsForAllBaseFiles from previous step, and count the total
        // number of distinct delete positions for all base files
        std::map<std::string, std::vector<int64_t>>
            flattenedDeletePosVectorsForAllBaseFiles;
        int64_t totalNumDeletePositions = 0;
        for (auto deleteVectorsForBaseFile : deletePosVectorsForAllBaseFiles)
        {
            auto baseFileName = deleteVectorsForBaseFile.first;
            auto deletePositionVectors = deleteVectorsForBaseFile.second;
            std::vector<int64_t> deletePositionVector =
                flattenAndDedup(deletePositionVectors, baseFileSizes[baseFileName]);
            flattenedDeletePosVectorsForAllBaseFiles[baseFileName] =
                deletePositionVector;
            totalNumDeletePositions += deletePositionVector.size();
        }

        // Now build the DuckDB queries
        if (totalNumDeletePositions == 0)
        {
            return "SELECT * FROM IcebergTest.tmp";
        }
        else if (totalNumDeletePositions >= totalNumRowsInAllBaseFiles)
        {
            return "SELECT * FROM IcebergTest.tmp WHERE 1 = 0";
        }
        else
        {
            // Convert the delete positions in all base files into column values
            std::vector<int64_t> allDeleteValues;

            int64_t numRowsInPreviousBaseFiles = 0;
            for (auto baseFileSize : baseFileSizes)
            {
                auto deletePositions =
                    flattenedDeletePosVectorsForAllBaseFiles[baseFileSize.first];

                if (numRowsInPreviousBaseFiles > 0)
                {
                    for (int64_t& deleteValue : deletePositions)
                    {
                        deleteValue += numRowsInPreviousBaseFiles;
                    }
                }

                allDeleteValues.insert(
                    allDeleteValues.end(),
                    deletePositions.begin(),
                    deletePositions.end());

                numRowsInPreviousBaseFiles += baseFileSize.second;
            }

            return fmt::format(
                "SELECT * FROM IcebergTest.tmp WHERE c0 NOT IN ({})",
                makeNotInList(allDeleteValues));
        }
    }


    std::vector<int64_t> flattenAndDedup(
        const std::vector<std::vector<int64_t>>& deletePositionVectors,
        int64_t baseFileSize) {
        std::vector<int64_t> deletePositionVector;
        for (auto vec : deletePositionVectors) {
            for (auto pos : vec) {
                if (pos >= 0 && pos < baseFileSize) {
                    deletePositionVector.push_back(pos);
                }
            }
        }

        std::sort(deletePositionVector.begin(), deletePositionVector.end());
        auto last =
            std::unique(deletePositionVector.begin(), deletePositionVector.end());
        deletePositionVector.erase(last, deletePositionVector.end());

        return deletePositionVector;
    }

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

    void assertQuery(std::vector<std::unique_ptr<BaseReader>> readers, const std::string& duckDbSql) const
    {
        BaseReaders base_readers {
            .readers = readers,
        };

        auto msg = fmt::format("\nExpected result from running Clickhouse sql: {}", duckDbSql);
        auto  actual = collectResult( base_readers);
        auto  expected = runClickhouseSQL(duckDbSql);
        // headBlock(actual);
        // headBlock(expected);
        EXPECT_TRUE(assertEqualResults(actual, expected, msg));
    }


protected:
    std::unique_ptr<BaseReader> makeIcebergSplit(
        const std::string& dataFilePath,
        const DB::Block & sampleBlock,
        const std::vector<SubstraitIcebergDeleteFile>& deleteFiles = {})
    {
        SubstraitInputFile file_info = makeInputFile(dataFilePath, deleteFiles);
        const Poco::URI file_uri{file_info.uri_file()};

        ReadBufferBuilderPtr read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context_);
        auto format_file = FormatFileUtil::createFile(context_, read_buffer_builder, file_info);

        return BaseReader::create(format_file, sampleBlock, sampleBlock, nullptr, nullptr);
    }

    std::unique_ptr<BaseReader> makeIcebergSplit(
        const std::string& dataFilePath,
        const std::vector<SubstraitIcebergDeleteFile>& deleteFiles = {})
    {
        return makeIcebergSplit(dataFilePath, toSampleBlock(readParquetSchema(dataFilePath, getFormatSettings(context_))), deleteFiles);
    }

    SubstraitIcebergDeleteFile makeDeleteFile(
        FileContent file_content,
        const std::string & _path,
        uint64_t _recordCount,
        uint64_t _fileSizeInBytes,
        std::vector<int32_t> _equalityFieldIds = {},
        std::unordered_map<int32_t, std::string> _lowerBounds = {},
        std::unordered_map<int32_t, std::string> _upperBounds = {} )
    {
        SubstraitIcebergDeleteFile deleteFile;
        deleteFile.set_filecontent(file_content);
        deleteFile.set_filepath("file://" + _path);
        deleteFile.set_recordcount(_recordCount);
        deleteFile.set_filesize(_fileSizeInBytes);

        substrait::ReadRel::LocalFiles::FileOrFiles::ParquetReadOptions parquet_format;
        deleteFile.mutable_parquet()->CopyFrom(parquet_format);
        for (const auto& fieldId : _equalityFieldIds)
            deleteFile.add_equalityfieldids(fieldId);

        return deleteFile;
    }

    SubstraitInputFile makeInputFile(const std::string & _path, const std::vector<SubstraitIcebergDeleteFile> & _deleteFiles)
    {
        SubstraitInputFile file;
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

        /// for big query
        context_->setSetting("max_query_size", DB::Field(524288));
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

    //TODO: write multiple groups
    std::map<std::string, std::shared_ptr<TempFilePath>>
    writeDataFiles(const std::map<std::string, std::vector<int64_t>> & rowGroupSizesForFiles)
    {
        std::map<std::string, std::shared_ptr<TempFilePath>> dataFilePaths;

        std::vector<DB::Block> dataVectorsJoined;
        dataVectorsJoined.reserve(rowGroupSizesForFiles.size());

        int64_t startingValue = 0;
        for (auto& dataFile : rowGroupSizesForFiles) {
            dataFilePaths[dataFile.first] = TempFilePath::tmp("parquet");

            // We make the values are continuously increasing even across base data
            // files. This is to make constructing DuckDB queries easier
            std::vector<DB::Block> dataVectors =
                makeVectors(dataFile.second, startingValue);
            writeToFile(
                dataFilePaths[dataFile.first]->string(),
                dataVectors,
                true);

            for (int i = 0; i < dataVectors.size(); i++) {
                dataVectorsJoined.push_back(dataVectors[i]);
            }
        }
        createDuckDbTable(dataVectorsJoined);
        return dataFilePaths;
    }

    /// @rowGroupSizesForFiles The key is the file name, and the value is a vector
    /// of RowGroup sizes
    /// @deleteFilesForBaseDatafiles The key is the delete file name, and the
    /// value contains the information about the content of this delete file.
    /// e.g. {
    ///         "delete_file_1",
    ///         {
    ///             {"data_file_1", {1, 2, 3}},
    ///             {"data_file_1", {4, 5, 6}},
    ///             {"data_file_2", {0, 2, 4}}
    ///         }
    ///     }
    /// represents one delete file called delete_file_1, which contains delete
    /// positions for data_file_1 and data_file_2. THere are 3 RowGroups in this
    /// delete file, the first two contain positions for data_file_1, and the last
    /// contain positions for data_file_2
    void assertPositionalDeletes(
        const std::map<std::string, std::vector<int64_t>>& rowGroupSizesForFiles,
        const std::unordered_map<
            std::string,
            std::multimap<std::string, std::vector<int64_t>>>&
            deleteFilesForBaseDatafiles,
        int32_t numPrefetchSplits = 0)
    {
        // Keep the reference to the deleteFilePath, otherwise the corresponding
        // file will be deleted.
        std::map<std::string, std::shared_ptr<TempFilePath>> dataFilePaths =
            writeDataFiles(rowGroupSizesForFiles);
        std::unordered_map<std::string, std::pair<int64_t, std::shared_ptr<TempFilePath>>>
        deleteFilePaths = writePositionDeleteFiles(
            deleteFilesForBaseDatafiles, dataFilePaths);

        std::vector<std::unique_ptr<BaseReader>> splits;

        for (const auto& dataFile : dataFilePaths) {
            std::string baseFileName = dataFile.first;
            std::string baseFilePath = dataFile.second->string();

            std::vector<SubstraitIcebergDeleteFile> deleteFiles;

            for (auto const& deleteFile : deleteFilesForBaseDatafiles) {
                std::string deleteFileName = deleteFile.first;
                std::multimap<std::string, std::vector<int64_t>> deleteFileContent =
                    deleteFile.second;

                if (deleteFileContent.count(baseFileName) != 0) {
                    // If this delete file contains rows for the target base file, then
                    // add it to the split
                    auto deleteFilePath =
                        deleteFilePaths[deleteFileName].second->string();

                    SubstraitIcebergDeleteFile icebergDeleteFile = makeDeleteFile(
                        IcebergReadOptions::POSITION_DELETES,
                        deleteFilePath,
                        deleteFilePaths[deleteFileName].first,
                        testing::internal::GetFileSize(
                            std::fopen(deleteFilePath.c_str(), "r")));
                    deleteFiles.push_back(icebergDeleteFile);
                }
            }

            splits.emplace_back(makeIcebergSplit(baseFilePath, deleteFiles));
        }

        std::string duckdbSql =
            getDuckDBQuery(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

        assertQuery(std::move(splits), duckdbSql);
    }

    /// Create 1 base data file data_file_1 with 2 RowGroups of 10000 rows each.
    /// Also create 1 delete file delete_file_1 which contains delete positions
    /// for data_file_1.
    void assertSingleBaseFileSingleDeleteFile(
        const std::vector<int64_t>& deletePositionsVec) {
        std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles = {
            {"data_file_1", {10000, 10000}}};
        std::unordered_map<
            std::string,
            std::multimap<std::string, std::vector<int64_t>>>
            deleteFilesForBaseDatafiles = {
            {"delete_file_1", {{"data_file_1", deletePositionsVec}}}};

        assertPositionalDeletes(
            rowGroupSizesForFiles, deleteFilesForBaseDatafiles, 0);
    }

    /// Create 3 base data files, where the first file data_file_0 has 500 rows,
    /// the second file data_file_1 contains 2 RowGroups of 10000 rows each, and
    /// the third file data_file_2 contains 500 rows. It creates 1 positional
    /// delete file delete_file_1, which contains delete positions for
    /// data_file_1.
    void assertMultipleBaseFileSingleDeleteFile(
        const std::vector<int64_t>& deletePositionsVec) {
        int64_t previousFileRowCount = 500;
        int64_t afterFileRowCount = 500;

        assertPositionalDeletes(
            {
                {"data_file_0", {previousFileRowCount}},
                {"data_file_1", {10000, 10000}},
                {"data_file_2", {afterFileRowCount}},
            },
            {{"delete_file_1", {{"data_file_1", deletePositionsVec}}}},
            0);
    }

    /// Create 1 base data file data_file_1 with 2 RowGroups of 10000 rows each.
    /// Create multiple delete files with name data_file_1, data_file_2, and so on
    void assertSingleBaseFileMultipleDeleteFiles(
        const std::vector<std::vector<int64_t>>& deletePositionsVecs) {
        std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles = {
            {"data_file_1", {10000, 10000}}};

        std::unordered_map<
            std::string,
            std::multimap<std::string, std::vector<int64_t>>>
            deleteFilesForBaseDatafiles;
        for (int i = 0; i < deletePositionsVecs.size(); i++) {
            std::string deleteFileName = fmt::format("delete_file_{}", i);
            deleteFilesForBaseDatafiles[deleteFileName] = {
                {"data_file_1", deletePositionsVecs[i]}};
        }
        assertPositionalDeletes(
            rowGroupSizesForFiles, deleteFilesForBaseDatafiles, 0);
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

        std::vector<SubstraitIcebergDeleteFile> deleteFiles;
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
                makeDeleteFile(IcebergReadOptions::EQUALITY_DELETES,
                    deleteFilePaths.back()->string(),
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
        if (numDataColumns > 1 &&
            equalityDeleteVectorMap.at(0).size() < numDataColumns) {
            std::string duckDbSql1 = "SELECT c0 FROM IcebergTest.tmp";
            if (numDeletedValues > 0) {
                duckDbSql1 += fmt::format(" WHERE {}", predicates);
            }

            auto icebergSplit1 = makeIcebergSplit(dataFilePath->string(),
                DB::Block{DB::ColumnWithTypeAndName(BIGINT(),"c0")},
                deleteFiles);

            assertEqualityDeletes(*icebergSplit1, duckDbSql1);
        }
    }

    void assertMultipleSplits(
        const std::vector<int64_t>& deletePositions,
        int32_t splitCount,
        int32_t numPrefetchSplits) {

        std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles;
        for (int32_t i = 0; i < splitCount; i++) {
            std::string dataFileName = fmt::format("data_file_{}", i);
            rowGroupSizesForFiles[dataFileName] = {rowCount};
        }

        std::unordered_map<
            std::string,
            std::multimap<std::string, std::vector<int64_t>>>
            deleteFilesForBaseDatafiles;
        for (int i = 0; i < splitCount; i++) {
            std::string deleteFileName = fmt::format("delete_file_{}", i);
            deleteFilesForBaseDatafiles[deleteFileName] = {
                {fmt::format("data_file_{}", i), deletePositions}};
        }

        assertPositionalDeletes(
            rowGroupSizesForFiles, deleteFilesForBaseDatafiles, numPrefetchSplits);
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

    std::vector<int64_t> makeRandomIncreasingValues(int64_t begin, int64_t end) {
        EXPECT_TRUE(begin < end);

        std::mt19937 gen{0};
        std::vector<int64_t> values;
        values.reserve(end - begin);
        for (int i = begin; i < end; i++) {
            if (std::uniform_int_distribution<uint32_t>(0, 9)(gen) > 8){
                values.push_back(i);
            }
        }
        return values;
    }

    std::vector<int64_t> makeContinuousIncreasingValues(
        int64_t begin,
        int64_t end)
    {
        std::vector<int64_t> values;
        values.resize(end - begin);
        std::iota(values.begin(), values.end(), begin);
        return values;
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

    std::vector<DB::Block> makeVectors(const std::vector<int64_t> & vectorSizes, int64_t& startingValue)
    {
        std::vector<DB::Block> vectors;
        vectors.reserve(vectorSizes.size());

        for (int j = 0; j < vectorSizes.size(); j++) {
            auto data = makeContinuousIncreasingValues(
                startingValue, startingValue + vectorSizes[j]);
            vectors.emplace_back(DB::Block{createColumn(data, "c0")});
            startingValue += vectorSizes[j];
        }

        return vectors;
    }
};

/// This test creates one single data file and one delete file. The parameter
/// passed to assertSingleBaseFileSingleDeleteFile is the delete positions.
TEST_F(IcebergTest, singleBaseFileSinglePositionalDeleteFile)
{
    assertSingleBaseFileSingleDeleteFile({{0, 1, 2, 3}});
    // Delete the first and last row in each batch (10000 rows per batch)
    assertSingleBaseFileSingleDeleteFile({{0, 9999, 10000, 19999}});
    // Delete several rows in the second batch (10000 rows per batch)
    assertSingleBaseFileSingleDeleteFile({{10000, 10002, 19999}});
    // Delete random rows
    assertSingleBaseFileSingleDeleteFile({makeRandomIncreasingValues(0, 20000)});
    // Delete 0 rows
    assertSingleBaseFileSingleDeleteFile({});
    // Delete all rows
    assertSingleBaseFileSingleDeleteFile(
        {makeContinuousIncreasingValues(0, 20000)});
    // Delete rows that don't exist
    assertSingleBaseFileSingleDeleteFile({{20000, 29999}});
}

/// This test creates 3 base data files, only the middle one has corresponding
/// delete positions. The parameter passed to
/// assertSingleBaseFileSingleDeleteFile is the delete positions.for the middle
/// base file.
TEST_F(IcebergTest, MultipleBaseFilesSinglePositionalDeleteFile) {

    assertMultipleBaseFileSingleDeleteFile({0, 1, 2, 3});
    assertMultipleBaseFileSingleDeleteFile({0, 9999, 10000, 19999});
    assertMultipleBaseFileSingleDeleteFile({10000, 10002, 19999});
    assertMultipleBaseFileSingleDeleteFile({10000, 10002, 19999});
    assertMultipleBaseFileSingleDeleteFile(
        makeRandomIncreasingValues(0, rowCount));
    assertMultipleBaseFileSingleDeleteFile({});
    assertMultipleBaseFileSingleDeleteFile(
        makeContinuousIncreasingValues(0, rowCount));
}

/// This test creates one base data file/split with multiple delete files. The
/// parameter passed to assertSingleBaseFileMultipleDeleteFiles is the vector of
/// delete files. Each leaf vector represents the delete positions in that
/// delete file.
TEST_F(IcebergTest, singleBaseFileMultiplePositionalDeleteFiles) {

    // Delete row 0, 1, 2, 3 from the first batch out of two.
    assertSingleBaseFileMultipleDeleteFiles({{1}, {2}, {3}, {4}});
    // Delete the first and last row in each batch (10000 rows per batch).
    assertSingleBaseFileMultipleDeleteFiles({{0}, {9999}, {10000}, {19999}});

    assertSingleBaseFileMultipleDeleteFiles({{500, 21000}});

    assertSingleBaseFileMultipleDeleteFiles(
        {makeRandomIncreasingValues(0, 10000),
         makeRandomIncreasingValues(10000, 20000),
         makeRandomIncreasingValues(5000, 15000)});

    assertSingleBaseFileMultipleDeleteFiles(
        {makeContinuousIncreasingValues(0, 10000),
         makeContinuousIncreasingValues(10000, 20000)});

    assertSingleBaseFileMultipleDeleteFiles(
        {makeContinuousIncreasingValues(0, 10000),
         makeContinuousIncreasingValues(10000, 20000),
         makeRandomIncreasingValues(5000, 15000)});

    assertSingleBaseFileMultipleDeleteFiles(
        {makeContinuousIncreasingValues(0, 20000),
         makeContinuousIncreasingValues(0, 20000)});

    assertSingleBaseFileMultipleDeleteFiles(
        {makeRandomIncreasingValues(0, 20000),
         {},
         makeRandomIncreasingValues(5000, 15000)});

    assertSingleBaseFileMultipleDeleteFiles({{}, {}});
}

/// This test creates 2 base data files, and 1 or 2 delete files, with unaligned
/// RowGroup boundaries
TEST_F(IcebergTest, multipleBaseFileMultiplePositionalDeleteFiles) {

  std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles;
  std::unordered_map<
      std::string,
      std::multimap<std::string, std::vector<int64_t>>>
      deleteFilesForBaseDatafiles;

  // Create two data files, each with two RowGroups
  rowGroupSizesForFiles["data_file_1"] = {100, 85};
  rowGroupSizesForFiles["data_file_2"] = {99, 1};

  // Delete 3 rows from the first RowGroup in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {{"data_file_1", {0, 1, 99}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete 3 rows from the second RowGroup in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {100, 101, 184}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete random rows from the both RowGroups in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeRandomIncreasingValues(0, 185)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete all rows in data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeContinuousIncreasingValues(0, 185)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);
  //
  // Delete non-existent rows from data_file_1
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeRandomIncreasingValues(186, 300)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Delete several rows from both RowGroups in both data files
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {0, 100, 102, 184}}, {"data_file_2", {1, 98, 99}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // The delete file delete_file_1 contains 3 RowGroups itself, with the first 3
  // deleting some repeating rows in data_file_1, and the last 2 RowGroups
  // deleting some  repeating rows in data_file_2
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {0, 1, 2, 3}},
      {"data_file_1", {1, 2, 3, 4}},
      {"data_file_1", makeRandomIncreasingValues(0, 185)},
      {"data_file_2", {1, 3, 5, 7}},
      {"data_file_2", makeRandomIncreasingValues(0, 100)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // delete_file_2 contains non-overlapping delete rows for each data files in
  // each RowGroup
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", {0, 1, 2, 3}}, {"data_file_2", {1, 3, 5, 7}}};
  deleteFilesForBaseDatafiles["delete_file_2"] = {
      {"data_file_1", {1, 2, 3, 4}},
      {"data_file_1", {98, 99, 100, 101, 184}},
      {"data_file_2", {3, 5, 7, 9}},
      {"data_file_2", {98, 99, 100}}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);

  // Two delete files each containing overlapping delete rows for both data
  // files
  deleteFilesForBaseDatafiles.clear();
  deleteFilesForBaseDatafiles["delete_file_1"] = {
      {"data_file_1", makeRandomIncreasingValues(0, 185)},
      {"data_file_2", makeRandomIncreasingValues(0, 100)}};
  deleteFilesForBaseDatafiles["delete_file_2"] = {
      {"data_file_1", makeRandomIncreasingValues(10, 120)},
      {"data_file_2", makeRandomIncreasingValues(50, 100)}};
  assertPositionalDeletes(rowGroupSizesForFiles, deleteFilesForBaseDatafiles);
}

TEST_F(IcebergTest, positionalDeletesMultipleSplits)
{

    assertMultipleSplits({1, 2, 3, 4}, 10, 5);
    assertMultipleSplits({1, 2, 3, 4}, 10, 0);
    assertMultipleSplits({1, 2, 3, 4}, 10, 10);
    assertMultipleSplits({0, 9999, 10000, 19999}, 10, 3);
    assertMultipleSplits(makeRandomIncreasingValues(0, 20000), 10, 3);
    assertMultipleSplits(makeContinuousIncreasingValues(0, 20000), 10, 3);
    assertMultipleSplits({}, 10, 3);
}

TEST_F(IcebergTest, basic_utils_test)
{

    {
        context_->setSetting("input_format_parquet_use_native_reader_with_filter_push_down", true);
        std::map<std::string, std::vector<int64_t>> rowGroupSizesForFiles;
        // Create two data files, each with two RowGroups
        rowGroupSizesForFiles["data_file_1"] = {100, 85};
        rowGroupSizesForFiles["data_file_2"] = {99, 1};

        std::unordered_map<std::string, std::multimap<std::string, std::vector<int64_t>>> deleteFilesForBaseDatafiles;

        deleteFilesForBaseDatafiles["delete_file_1"] = {
            {"data_file_1", {0, 100, 102, 184}}, {"data_file_2", {1, 98, 99}}};

        std::map<std::string, std::shared_ptr<TempFilePath>> dataFilePaths =
            writeDataFiles(rowGroupSizesForFiles);

        std::unordered_map<std::string, std::pair<int64_t, std::shared_ptr<TempFilePath>>>
        deleteFilePaths = writePositionDeleteFiles( deleteFilesForBaseDatafiles, dataFilePaths);
        assert(deleteFilePaths.size() == 1);

        auto x = runClickhouseSQL(fmt::format("select pos from file('{}') where file_path = 'file://{}'",
            deleteFilePaths["delete_file_1"].second->string(), dataFilePaths["data_file_2"]->string()));
        // auto y = runClickhouseSQL(fmt::format("select * from file('{}')",
        //     deleteFilePaths["delete_file_1"].second->string()));
        headBlock(x, 100 , 100);

        context_->setSetting("input_format_parquet_use_native_reader_with_filter_push_down", DB::Field(false));
    }

    {
        std::shared_ptr<TempFilePath> dataFilePath = writeDataFiles(rowCount, 4)[0];

        runClickhouseSQL(fmt::format("select count(*) from file('{}')", dataFilePath->string()));
        DB::Block block = runClickhouseSQL("select count(*) from IcebergTest.tmp");
        EXPECT_TRUE(assertEqualResults(block, DB::Block{createColumn<UInt64>({rowCount}, "count()")}));


        auto read = makeIcebergSplit(dataFilePath->string());
        DB::Block actual = collectResult( *read);
        EXPECT_TRUE(assertEqualResults( actual, runClickhouseSQL("select * from IcebergTest.tmp")));
    }
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
