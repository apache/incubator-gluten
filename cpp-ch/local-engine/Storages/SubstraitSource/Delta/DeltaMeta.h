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

#include <rapidjson/document.h>
#include <Core/Block.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
namespace DeltaVirtualMeta
{
inline constexpr auto DELTA_INTERNAL_IS_ROW_DELETED = "__delta_internal_is_row_deleted";
inline bool hasMetaColumns(const DB::Block & header)
{
    return header.findByName(std::string_view{DELTA_INTERNAL_IS_ROW_DELETED}) != nullptr;
}
inline DB::DataTypePtr getMetaColumnType(const DB::Block & header)
{
    return header.findByName(std::string_view{DELTA_INTERNAL_IS_ROW_DELETED})->type;
}
inline DB::Block removeMetaColumns(const DB::Block & header)
{
    DB::Block new_header;
    for (const auto & col : header)
        if (col.name != DELTA_INTERNAL_IS_ROW_DELETED)
            new_header.insert(col);
    return new_header;
}

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

    static std::shared_ptr<DeltaDVBitmapConfig> parse_config(const String & row_index_ids_encoded)
    {
        std::shared_ptr<DeltaDVBitmapConfig> bitmap_config_ = std::make_shared<DeltaDVBitmapConfig>();
        rapidjson::Document doc;
        doc.Parse(row_index_ids_encoded.c_str());
        if (doc.HasParseError())
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid JSON in row_index_ids_encoded: {}", doc.GetParseError());
        if (doc.HasMember("storageType") && doc["storageType"].IsString())
            bitmap_config_->storage_type = doc["storageType"].GetString();
        if (doc.HasMember("pathOrInlineDv") && doc["pathOrInlineDv"].IsString())
            bitmap_config_->path_or_inline_dv = doc["pathOrInlineDv"].GetString();
        if (doc.HasMember("offset") && doc["offset"].IsInt())
            bitmap_config_->offset = doc["offset"].GetInt();
        if (doc.HasMember("sizeInBytes") && doc["sizeInBytes"].IsInt())
            bitmap_config_->size_in_bytes = doc["sizeInBytes"].GetInt();
        if (doc.HasMember("cardinality") && doc["cardinality"].IsInt64())
            bitmap_config_->cardinality = doc["cardinality"].GetInt64();
        if (doc.HasMember("maxRowIndex") && doc["maxRowIndex"].IsInt64())
            bitmap_config_->max_row_index = doc["maxRowIndex"].GetInt64();

        return bitmap_config_;
    }
};

}

}