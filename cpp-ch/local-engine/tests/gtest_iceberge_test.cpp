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
#include <Storages/SubstraitSource/icerberg/IcebergDeleteFile.h>
#include <Core/Block.h>
#include <Storages/Output/NormalFileWriter.h>
#include <gtest/gtest.h>
#include <tests/utils/ReaderTestBase.h>
#include <tests/utils/TempFilePath.h>
#include <Poco/URI.h>
#include <Common/QueryContext.h>
#include <Interpreters/ExpressionActions.h>


class ConnectorSplit {};
namespace local_engine::test
{
class IcebergTest : public ReaderTestBase
{
public:
    static constexpr int rowCount = 20000;
private:
    std::shared_ptr<ConnectorSplit> makeIcebergSplit(
      const std::string& dataFilePath,
      const std::vector<iceberg::IcebergDeleteFile>& deleteFiles = {})
    {
        return nullptr;
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

    void assertEqualityDeletes(std::shared_ptr<ConnectorSplit> split, const std::string& duckDbSql) {}

public:
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

        // TODO: createDuckDbTable(dataVectors);
        return dataFilePaths;
    }

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

        std::vector<iceberg::IcebergDeleteFile> deleteFiles;
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
            iceberg::IcebergDeleteFile deleteFile(
                iceberg::FileContent::kEqualityDeletes,
                deleteFilePaths.back()->string(),
                equalityDeleteVector[0].size(),
                testing::internal::GetFileSize(
                    std::fopen(deleteFilePaths.back()->string().c_str(), "r")),
                equalityFieldIds);
            deleteFiles.push_back(deleteFile);
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
            duckDbSql = "SELECT * FROM tmp ";
            if (numDeletedValues > 0) {
                duckDbSql += fmt::format("WHERE {}", predicates);
            }
        }

        assertEqualityDeletes(icebergSplit, duckDbSql);
    }

    std::shared_ptr<TempFilePath> writeEqualityDeleteFile(
      const std::vector<std::vector<int64_t>>& equalityDeleteVector)
    {
        DB::ColumnsWithTypeAndName columns;

        for (int i = 0; i < equalityDeleteVector.size(); i++) {
            const auto column = DB::ColumnInt64::create(equalityDeleteVector[i].size());
            DB::ColumnInt64::Container & vec = column->getData();
            memcpy(vec.data(), equalityDeleteVector[i].data(), equalityDeleteVector[i].size() * sizeof(int64_t));
            columns.emplace_back(DB::ColumnWithTypeAndName(column->getPtr(), BIGINT(), fmt::format("c{}", i)));
        }

        auto deleteFilePath = TempFilePath::tmp("parquet");
        writeToFile(deleteFilePath->string(), DB::Block(columns));
        return deleteFilePath;
    }

    DB::ColumnPtr makeSequenceValues(int32_t numRows, int8_t repeat = 1)
    {
        EXPECT_GT(repeat, 0);
        auto maxValue = std::ceil(static_cast<double>(numRows) / repeat);
        auto column = DB::ColumnInt64::create(numRows);
        DB::ColumnInt64::Container & values = column->getData();
        Int64 * pos = values.data();
        for (int32_t i = 0; i < maxValue; i++) {
            for (int8_t j = 0; j < repeat; j++) {
                *pos++ = i;
            }
        }
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

TEST_F(IcebergTest, tmp)
{
    auto createDeleteBlock = []() -> DB::Block
    {
        std::vector<std::vector<int64_t>> equalityDeleteVector = {{0, 1}, {0, 0}};
        DB::ColumnsWithTypeAndName columns;

        for (int i = 0; i < equalityDeleteVector.size(); i++) {
            const auto column = DB::ColumnInt64::create(equalityDeleteVector[i].size());
            DB::ColumnInt64::Container & vec = column->getData();
            memcpy(vec.data(), equalityDeleteVector[i].data(), equalityDeleteVector[i].size() * sizeof(int64_t));
            columns.emplace_back(DB::ColumnWithTypeAndName(column->getPtr(), BIGINT(), fmt::format("c{}", i)));
        }
        return DB::Block(columns);
    };
    DB::Block deleteBlock{createDeleteBlock()};
    std::vector<DB::Block> dataBlock {makeVectors(1, rowCount, 2)};

    // headBlock(deleteBlock);
    // headBlock(dataBlock[0]);

    // readMultipleColumnDeleteValues

    auto numDeleteFields = deleteBlock.columns();
    assert(numDeleteFields > 0 && "Iceberg equality delete file should have at least one field.");

    auto numDeletedValues = deleteBlock.rows();
    DB::ASTs expressionInputs;

    for (int i = 0; i < numDeletedValues; i++)
    {
        DB::ASTs arguments;
        for (int j = 0; j < numDeleteFields; j++)
        {
            auto name = std::make_shared<DB::ASTIdentifier>(deleteBlock.getByPosition(j).name);
            auto value = std::make_shared<DB::ASTLiteral>(deleteBlock.getByPosition(j).column->operator[](i));
            auto isNotEqualExpr = makeASTFunction("notEquals", DB::ASTs{name, value});
            arguments.emplace_back(isNotEqualExpr);
        }
        if (arguments.size() > 1)
            expressionInputs.emplace_back(makeASTFunction("or", arguments));
        else
            expressionInputs.emplace_back(arguments[0]);
    }

    DB::ASTPtr result;
    if (expressionInputs.size() > 1)
    {
        result = DB::makeASTFunction("and", expressionInputs);

    }
    else
    {
        result = expressionInputs[0];
    }


    auto context = QueryContext::instance().currentQueryContext();
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
