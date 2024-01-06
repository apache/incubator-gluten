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
#include <config.h>

#if USE_PARQUET
#include <Formats/FormatSettings.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace ch_parquet::arrow
{
class FileReader;
}

namespace arrow
{
class Buffer;
}

namespace DB
{
class OptimizedArrowColumnToCHColumn;

class OptimizedParquetBlockInputFormat : public IInputFormat
{
public:
    OptimizedParquetBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    void resetParser() override;

    String getName() const override { return "OptimizedParquetBlockInputFormat"; }

    const BlockMissingValues & getMissingValues() const override;

private:
    Chunk read() override;

protected:
    void prepareReader();

    void onCancel() override { is_stopped = 1; }

    std::unique_ptr<ch_parquet::arrow::FileReader> file_reader;
    int row_group_total = 0;
    // indices of columns to read from Parquet file
    std::vector<int> column_indices;
    std::vector<String> column_names;
    std::unique_ptr<OptimizedArrowColumnToCHColumn> arrow_column_to_ch_column;
    int row_group_current = 0;
    std::vector<size_t> missing_columns;
    BlockMissingValues block_missing_values;
    const FormatSettings format_settings;

    std::atomic<int> is_stopped{0};
};

class OptimizedParquetSchemaReader : public ISchemaReader
{
public:
    OptimizedParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    const FormatSettings format_settings;
};

}

#endif
