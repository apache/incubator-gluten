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
#include <string>
#include <Common/BlockTypeUtils.h>

namespace local_engine::iceberg
{
struct IcebergMetadataColumn
{
    int id;
    std::string name;
    DB::DataTypePtr type;
    std::string doc;

    IcebergMetadataColumn(int _id, const std::string & _name, const DB::DataTypePtr & _type, const std::string & _doc)
        : id(_id), name(_name), type(_type), doc(_doc)
    {
    }

    static std::shared_ptr<IcebergMetadataColumn> icebergDeleteFilePathColumn()
    {
        static auto file_path
            = std::make_shared<IcebergMetadataColumn>(2147483546, "file_path", STRING(), "Path of a file in which a deleted row is stored");
        return file_path;
    }

    static std::shared_ptr<IcebergMetadataColumn> icebergDeletePosColumn()
    {
        static auto pos
            = std::make_shared<IcebergMetadataColumn>(2147483545, "pos", BIGINT(), "Ordinal position of a deleted row in the data file");
        return pos;
    }

    static DB::NamesAndTypesList & getNamesAndTypesList()
    {
        static DB::NamesAndTypesList names_and_types_list{
            {icebergDeleteFilePathColumn()->name, icebergDeleteFilePathColumn()->type},
            {icebergDeletePosColumn()->name, icebergDeletePosColumn()->type}};
        return names_and_types_list;
    }
};
}