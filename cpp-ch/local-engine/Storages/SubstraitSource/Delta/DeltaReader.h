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
#include <Storages/SubstraitSource/Delta/DeltaMeta.h>

namespace local_engine::delta
{

class DeltaReader final : public NormalFileReader
{
    std::shared_ptr<DeltaVirtualMeta::DeltaDVBitmapConfig> bitmap_config;
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
        const std::shared_ptr<DeltaVirtualMeta::DeltaDVBitmapConfig> & bitmap_config_ = nullptr);

protected:
    DB::Chunk doPull() override;

    void deleteRowsByDV(DB::Chunk & chunk) const;
};

}