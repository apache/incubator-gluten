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
#pragma once
#include <Processors/QueryPlan/ExpressionStep.h>

namespace DB
{
    class Chunk;
}

namespace local_engine
{
class InputFileNameParser
{
public:
    static inline const String & INPUT_FILE_NAME = "input_file_name";
    static inline const String & INPUT_FILE_BLOCK_START = "input_file_block_start";
    static inline const String & INPUT_FILE_BLOCK_LENGTH = "input_file_block_length";
    static inline std::unordered_set INPUT_FILE_COLUMNS_SET = {INPUT_FILE_NAME, INPUT_FILE_BLOCK_START, INPUT_FILE_BLOCK_LENGTH};

    static bool hasInputFileNameColumn(const DB::Block & block);
    static bool hasInputFileBlockStartColumn(const DB::Block & block);
    static bool hasInputFileBlockLengthColumn(const DB::Block & block);
    static bool containsInputFileColumns(const DB::Block & block);
    static DB::Block removeInputFileColumn(const DB::Block & block);
    static void addInputFileColumnsToChunk(
        const DB::Block & header,
        DB::Chunk & chunk,
        const std::optional<String> & file_name,
        const std::optional<Int64> & block_start,
        const std::optional<Int64> & block_length);


    void setFileName(const String & file_name) { this->file_name = file_name; }

    void setBlockStart(const Int64 block_start) { this->block_start = block_start; }

    void setBlockLength(const Int64 block_length) { this->block_length = block_length; }

    [[nodiscard]] std::optional<DB::IQueryPlanStep *> addInputFileProjectStep(DB::QueryPlan & plan);
    void addInputFileColumnsToChunk(const DB::Block & header, DB::Chunk & chunk) const;

private:
    std::optional<String> file_name;
    std::optional<Int64> block_start;
    std::optional<Int64> block_length;
};
} // local_engine
