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
#include <Interpreters/Context_fwd.h>
#include <Storages/SubstraitSource/substrait_fwd.h>

namespace local_engine
{
class DeltaDVRoaringBitmapArray;
}
namespace local_engine::iceberg
{


/**
 * @brief Creates a bitmap expression for positional delete files.
 *
 * This function processes the provided positional delete files and constructs a bitmap
 * to represent the deleted positions. It reads the delete files, applies a filter, and
 * adds the positions to a DeltaDVRoaringBitmapArray.
 *
 * @param context The execution context.
 * @param data_file_header The header of the data file (unused).
 * @param file_ The input file.
 * @param delete_files A list of delete files.
 * @param position_delete_files Indices of the delete files that are positional deletes.
 * @param reader_header The block header for the reader, which may be modified if it doesn't contain row index.
 * @return A unique pointer to a DeltaDVRoaringBitmapArray containing the deleted positions,
 *         or nullptr if no positions are deleted.
 */
std::unique_ptr<DeltaDVRoaringBitmapArray> createBitmapExpr(
    const DB::ContextPtr & context,
    const DB::Block & data_file_header,
    const SubstraitInputFile & file_,
    const google::protobuf::RepeatedPtrField<SubstraitIcebergDeleteFile> & delete_files,
    const std::vector<int> & position_delete_files,
    DB::Block & reader_header);

}