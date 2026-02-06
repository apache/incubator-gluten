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

#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/SubstraitSource/Delta/Bitmap/DeltaDVRoaringBitmapArray.h>

namespace local_engine::delta
{
static constexpr const char * DELETION_VECTOR_FILE_NAME_CORE = "deletion_vector";

struct DeletionVectorDescriptor
{
    static constexpr String PATH_DV_MARKER = "p";
    static constexpr String INLINE_DV_MARKER = "i";
    static constexpr String UUID_DV_MARKER = "u";

    static DeletionVectorDescriptor deserializeFromBase64(const String & encoded);
    DB::Tuple createDeletionVectorDescriptorField();

    Int64 cardinality;
    Int32 size_in_bytes;
    String storage_type;
    Int32 offset;
    String path_or_inline_dv;
};


class DeltaWriter
{
public:
    explicit DeltaWriter(
        const DB::ContextPtr & context_,
        const String & table_path_,
        const size_t & prefix_length_,
        const size_t & packing_target_size_,
        const String & dv_file_name_prefix_);
    void
    writeDeletionVector(const DB::Block & block);

    DB::Block * finalize();

private:
    std::unique_ptr<DB::WriteBuffer> createWriteBuffer(const String & table_path, const String & prefix) const;
    DeltaDVRoaringBitmapArray
    deserializeExistingBitmap(const DeletionVectorDescriptor & deletion_vector_descriptor, const String & table_path) const;
    String assembleDeletionVectorPath(const String & table_path, const String & prefix, const String & uuid) const;
    void initBinPackage();

    DB::ContextPtr context;
    const String table_path;
    const size_t prefix_length;
    const size_t packing_target_size;
    const String dv_file_name_prefix;

    DB::MutableColumnPtr file_path_column;
    DB::MutableColumnPtr dv_descriptor_column;
    DB::MutableColumnPtr matched_row_count_col;
    std::unique_ptr<DB::WriteBuffer> write_buffer;

    size_t offset = 0;
    size_t size_of_current_bin = 0;
    String prefix;
    DB::UUID uuid;
};


}
