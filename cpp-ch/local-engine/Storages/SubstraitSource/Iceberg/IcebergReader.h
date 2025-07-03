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

#include <Storages/SubstraitSource/FileReader.h>

namespace local_engine
{
class DeltaDVRoaringBitmapArray;
}

namespace local_engine::iceberg
{
class IcebergReader final : public NormalFileReader
{
    DB::ExpressionActionsPtr delete_expr;
    const std::string delete_expr_column_name;
    std::unique_ptr<DeltaDVRoaringBitmapArray> delete_bitmap_array;
    size_t start_remove_index;

public:
    static std::unique_ptr<IcebergReader> create(
        const FormatFilePtr & file_,
        const DB::Block & to_read_header_,
        const DB::Block & output_header_,
        const std::function<FormatFile::InputFormatPtr(const DB::Block &)> & input_format_callback);

    IcebergReader(
        const FormatFilePtr & file_,
        const DB::Block & to_read_header_,
        const DB::Block & output_header_,
        const FormatFile::InputFormatPtr & input_format_,
        const DB::ExpressionActionsPtr & delete_expr_,
        std::unique_ptr<DeltaDVRoaringBitmapArray> delete_bitmap_array_,
        size_t start_remove_index_);

    ~IcebergReader() override;

protected:
    DB::Chunk doPull() override;

    DB::Block applyEqualityDelete(DB::Chunk & chunk) const;
    DB::Block applyPositionDelete(DB::Block block) const;
    DB::Chunk filterRows(DB::Block block) const;
};

}