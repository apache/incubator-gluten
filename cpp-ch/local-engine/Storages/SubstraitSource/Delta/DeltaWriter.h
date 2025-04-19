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

struct DeletionVectorDescriptor
{
};

class DeltaWriter
{
    static constexpr String UUID_DV_MARKER = "u";
    static constexpr String DELETION_VECTOR_FILE_NAME_CORE = "deletion_vector";

public:
    explicit DeltaWriter(
        const DB::ContextPtr & context_, const String & table_path_, const size_t & prefix_length_, const size_t & packing_target_size_)
        : context(context_), table_path(table_path_), prefix_length(prefix_length_), packing_target_size(packing_target_size_)
    {
        file_path_column = DB::ColumnString::create();
        dv_descriptor_column = createDeletionVectorDescriptorColumn();
        matched_row_count_col = DB::ColumnInt64::create();
    }
    void
    writeDeletionVector(const DB::Block & block);

    DB::Block * finalize();

private:
    DB::ColumnTuple::MutablePtr createDeletionVectorDescriptorColumn();
    String assembleDeletionVectorPath(const String & table_path, const String & prefix, const String & uuid) const;
    std::unique_ptr<DB::WriteBuffer> createWriteBuffer(const String & table_path, const String & prefix, const String & uuid) const;
    DeltaDVRoaringBitmapArray deserializeExistingBitmap(
        const String & existing_path_or_inline_dv,
        const Int32 & existing_offset,
        const Int32 & existing_size_in_bytes,
        const String & table_path) const;
    DB::Tuple createDeletionVectorDescriptorField(
        const String & path_or_inline_dv, const Int32 & offset, const Int32 & size_in_bytes, const Int64 & cardinality);

    void initBinPackage();

    DB::ContextPtr context;
    const String table_path;
    const size_t prefix_length;
    const size_t packing_target_size;

    DB::MutableColumnPtr file_path_column;
    DB::MutableColumnPtr dv_descriptor_column;
    DB::MutableColumnPtr matched_row_count_col;
    std::unique_ptr<DB::WriteBuffer> write_buffer;

    size_t offset = 0;
    size_t size_of_current_bin = 0;
    String prefix;
    String uuid;
};


}
