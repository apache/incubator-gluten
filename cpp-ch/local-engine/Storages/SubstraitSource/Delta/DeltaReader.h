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
#include <Storages/SubstraitSource/Delta/Bitmap/DeltaDVRoaringBitmapArray.h>

namespace local_engine::delta
{

struct DeltaDVBitmapConfig
{
    inline static const String DELTA_ROW_INDEX_FILTER_TYPE = "row_index_filter_type";
    inline static const String DELTA_ROW_INDEX_FILTER_ID_ENCODED = "row_index_filter_id_encoded";
    inline static const String DELTA_ROW_INDEX_FILTER_TYPE_IF_CONTAINED = "IF_CONTAINED";
    inline static const String DELTA_ROW_INDEX_FILTER_TYPE_IF_NOT_CONTAINED = "IF_NOT_CONTAINED";

    String storage_type;
    String path_or_inline_dv;
    Int32 offset = 0;
    Int32 size_in_bytes = 0;
    Int64 cardinality = 0;
    Int64 max_row_index = 0;
};

class DeltaReader final : public NormalFileReader
{
    std::shared_ptr<DeltaDVBitmapConfig> bitmap_config;
    std::unique_ptr<DeltaDVRoaringBitmapArray> bitmap_array;

public:
    static std::unique_ptr<DeltaReader> create(
        const FormatFilePtr & file_,
        const DB::Block & to_read_header_,
        const DB::Block & output_header_,
        const FormatFile::InputFormatPtr & input_format_,
        const String & row_index_ids_encoded,
        const String & row_index_filter_type);

    DeltaReader(
        const FormatFilePtr & file_,
        const DB::Block & to_read_header_,
        const DB::Block & output_header_,
        const FormatFile::InputFormatPtr & input_format_,
        const std::shared_ptr<DeltaDVBitmapConfig> & bitmap_config_ = nullptr);

protected:
    DB::Chunk doPull() override;

    void deleteRowsByDV(DB::Chunk & chunk) const;
};

}