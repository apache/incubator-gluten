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

#include "DeltaWriter.h"

#include <zlib.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block_fwd.h>
#include <Core/Range.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/JSONUtils.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Storages/Output/WriteBufferBuilder.h>
#include <Storages/SubstraitSource/Delta/DeltaUtil.h>
#include <rapidjson/document.h>
#include <Poco/URI.h>

namespace local_engine::delta
{

String getRandomPrefix(const size_t & length)
{
    static const char alphanum[] = "0123456789"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "abcdefghijklmnopqrstuvwxyz";

    String res;
    for (size_t i = 0; i < length; ++i)
        res += alphanum[rand() % (sizeof(alphanum) - 1)];
    return res;
}

DB::DataTypePtr getDeletionVectorType()
{
    DB::DataTypes dv_descriptor_types;
    auto storageType = std::make_shared<DB::DataTypeString>();
    auto pathOrInlineDv = std::make_shared<DB::DataTypeString>();
    auto offset = std::make_shared<DB::DataTypeInt32>();
    auto offset_nullable = std::make_shared<DB::DataTypeNullable>(offset);
    auto sizeInBytes = std::make_shared<DB::DataTypeInt32>();
    auto cardinality = std::make_shared<DB::DataTypeInt64>();
    auto maxRowIndex = std::make_shared<DB::DataTypeInt64>();
    auto maxRowIndex_nullable = std::make_shared<DB::DataTypeNullable>(maxRowIndex);

    dv_descriptor_types.emplace_back(storageType);
    dv_descriptor_types.emplace_back(pathOrInlineDv);
    dv_descriptor_types.emplace_back(offset_nullable);
    dv_descriptor_types.emplace_back(sizeInBytes);
    dv_descriptor_types.emplace_back(cardinality);
    dv_descriptor_types.emplace_back(maxRowIndex_nullable);

    DB::Strings dv_descriptor_names;
    dv_descriptor_names.emplace_back("storageType");
    dv_descriptor_names.emplace_back("pathOrInlineDv");
    dv_descriptor_names.emplace_back("offset");
    dv_descriptor_names.emplace_back("sizeInBytes");
    dv_descriptor_names.emplace_back("cardinality");
    dv_descriptor_names.emplace_back("maxRowIndex");

    return std::make_shared<DB::DataTypeTuple>(dv_descriptor_types, dv_descriptor_names);
}


void DeltaWriter::writeDeletionVector(const DB::Block & block)
{
    const auto & file_path_columns = block.getByPosition(0);
    const auto & deletion_vector_id_columns = block.getByPosition(1);
    const auto & bitmap_columns = block.getByPosition(2);
    const auto & cardinality_src_columns = block.getByPosition(3);

    for (size_t row_idx = 0; row_idx < block.rows(); row_idx++)
    {
        const auto file_path = file_path_columns.column->getDataAt(row_idx);
        auto bitmap = bitmap_columns.column->getDataAt(row_idx).toString();
        auto cardinality = cardinality_src_columns.column->get64(row_idx); // alisa deletedRowIndexCount

        if (size_of_current_bin > 0 && bitmap.length() + size_of_current_bin > packing_target_size)
        {
            write_buffer->finalize();
            write_buffer = nullptr;
        }

        if (!deletion_vector_id_columns.column->isNullAt(row_idx))
        {
            DB::Field deletion_vector_id_field;
            deletion_vector_id_columns.column->get(row_idx, deletion_vector_id_field);
            auto existing_deletion_vector_id = deletion_vector_id_field.safeGet<String>();

            if (!existing_deletion_vector_id.empty())
            {
                rapidjson::Document existingDvDescriptor;
                existingDvDescriptor.Parse(existing_deletion_vector_id.c_str());

                String existing_path_or_inline_dv = existingDvDescriptor["pathOrInlineDv"].GetString();
                Int32 existing_offset = existingDvDescriptor["offset"].GetInt();
                Int32 existing_size_in_bytes = existingDvDescriptor["sizeInBytes"].GetInt();
                Int64 existing_cardinality = existingDvDescriptor["cardinality"].GetInt64();

                if (cardinality > 0)
                {
                    DeltaDVRoaringBitmapArray existing_bitmap
                        = deserializeExistingBitmap(existing_path_or_inline_dv, existing_offset, existing_size_in_bytes, table_path);
                    existing_bitmap.merge(bitmap);
                    bitmap = existing_bitmap.serialize();
                    cardinality = existing_bitmap.cardinality();
                }
                else
                {
                    // use already existing deletion vector
                    auto dv_descriptor_field = createDeletionVectorDescriptorField(
                        existing_path_or_inline_dv, existing_offset, existing_size_in_bytes, existing_cardinality);
                    file_path_column->insert(file_path.data);
                    dv_descriptor_column->insert(dv_descriptor_field);
                    matched_row_count_col->insert(cardinality);
                    continue;
                }
            }
        }

        if (!write_buffer)
            initBinPackage();

        Int32 bitmap_size = static_cast<Int32>(bitmap.length());
        size_of_current_bin = size_of_current_bin + bitmap.length();

        DB::writeBinaryBigEndian(bitmap_size, *write_buffer);

        write_buffer->write(bitmap.c_str(), bitmap_size);
        Int32 checksum_value = static_cast<Int32>(crc32_z(0L, reinterpret_cast<const unsigned char *>(bitmap.c_str()), bitmap_size));
        DB::writeBinaryBigEndian(checksum_value, *write_buffer);

        auto dv_descriptor_field
            = createDeletionVectorDescriptorField(DeltaUtil::encodeUUID(uuid, prefix), offset, bitmap_size, cardinality);

        file_path_column->insert(file_path.data);
        dv_descriptor_column->insert(dv_descriptor_field);
        matched_row_count_col->insert(cardinality);

        offset = sizeof(bitmap_size) + bitmap_size + sizeof(checksum_value) + offset;
    }
}

DB::Block * DeltaWriter::finalize()
{
    if (size_of_current_bin > 0)
        write_buffer->finalize();

    DB::Block * res = new DB::Block(
        {DB::ColumnWithTypeAndName(std::move(file_path_column), std::make_shared<DB::DataTypeString>(), "filePath"),
         DB::ColumnWithTypeAndName(std::move(dv_descriptor_column), getDeletionVectorType(), "deletionVector"),
         DB::ColumnWithTypeAndName(std::move(matched_row_count_col), std::make_shared<DB::DataTypeInt64>(), "matchedRowCount")});

    return res;
}


DB::ColumnTuple::MutablePtr DeltaWriter::createDeletionVectorDescriptorColumn()
{
    DB::MutableColumns dv_descriptor_mutable_columns;
    dv_descriptor_mutable_columns.emplace_back(DB::ColumnString::create()); // storageType
    dv_descriptor_mutable_columns.emplace_back(DB::ColumnString::create()); // pathOrInlineDv
    dv_descriptor_mutable_columns.emplace_back(DB::ColumnNullable::create(DB::ColumnInt32::create(), DB::ColumnUInt8::create())); // offset
    dv_descriptor_mutable_columns.emplace_back(DB::ColumnInt32::create()); // sizeInBytes
    dv_descriptor_mutable_columns.emplace_back(DB::ColumnInt64::create()); // cardinality
    dv_descriptor_mutable_columns.emplace_back(
        DB::ColumnNullable::create(DB::ColumnInt64::create(), DB::ColumnUInt8::create())); // maxRowIndex

    return DB::ColumnTuple::create(std::move(dv_descriptor_mutable_columns));
}

String DeltaWriter::assembleDeletionVectorPath(const String & table_path, const String & prefix, const String & uuid) const
{
    String path = table_path + "/";
    if (!prefix.empty())
        path += prefix + "/";

    path += DELETION_VECTOR_FILE_NAME_CORE + "_" + uuid + ".bin";
    return path;
}

std::unique_ptr<DB::WriteBuffer> DeltaWriter::createWriteBuffer(const String & table_path, const String & prefix, const String & uuid) const
{
    String dv_file = assembleDeletionVectorPath(table_path, prefix, uuid);

    std::string encoded;
    Poco::URI::encode(dv_file, "", encoded);
    const Poco::URI poco_uri(encoded);
    const auto write_buffer_builder = WriteBufferBuilderFactory::instance().createBuilder(poco_uri.getScheme(), context);
    return write_buffer_builder->build(poco_uri.toString());
}

DeltaDVRoaringBitmapArray DeltaWriter::deserializeExistingBitmap(
    const String & existing_path_or_inline_dv,
    const Int32 & existing_offset,
    const Int32 & existing_size_in_bytes,
    const String & table_path) const
{
    const auto random_prefix_length = existing_path_or_inline_dv.length() - Codec::Base85Codec::ENCODED_UUID_LENGTH;
    const auto randomPrefix = existing_path_or_inline_dv.substr(0, random_prefix_length);
    const auto encoded_uuid = existing_path_or_inline_dv.substr(random_prefix_length);
    const auto existing_decode_uuid = DeltaUtil::decodeUUID(encoded_uuid);
    const String existing_dv_file = assembleDeletionVectorPath(table_path, randomPrefix, existing_decode_uuid);
    DeltaDVRoaringBitmapArray existing_bitmap;
    existing_bitmap.rb_read(existing_dv_file, existing_offset, existing_size_in_bytes, context);
    return existing_bitmap;
}

DB::Tuple DeltaWriter::createDeletionVectorDescriptorField(
    const String & path_or_inline_dv, const Int32 & offset, const Int32 & size_in_bytes, const Int64 & cardinality)
{
    DB::Tuple tuple;
    tuple.emplace_back(UUID_DV_MARKER); // storageType
    tuple.emplace_back(path_or_inline_dv); // pathOrInlineDv
    tuple.emplace_back(offset); // offset
    tuple.emplace_back(size_in_bytes); // sizeInBytes
    tuple.emplace_back(cardinality); // cardinality
    tuple.emplace_back(DB::Field{}); // maxRowIndex
    return tuple;
}

void DeltaWriter::initBinPackage()
{
    offset = 0;
    size_of_current_bin = 0;
    prefix = getRandomPrefix(prefix_length);
    uuid = DB::toString(DB::UUIDHelpers::generateV4());
    write_buffer = createWriteBuffer(table_path, prefix, uuid);
    DB::writeIntBinary(DV_FILE_FORMAT_VERSION_ID_V1, *write_buffer);
    offset++;
}


}