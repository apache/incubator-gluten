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

#include "PositionalDeleteFileReader.h"

#include <Functions/FunctionFactory.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/SubstraitSource/Delta/Bitmap/DeltaDVRoaringBitmapArray.h>
#include <Storages/SubstraitSource/Iceberg/IcebergMetadataColumn.h>
#include <Storages/SubstraitSource/Iceberg/SimpleParquetReader.h>
#include <Common/BlockTypeUtils.h>

using namespace DB;

namespace local_engine::iceberg
{

using namespace google::protobuf;

std::unique_ptr<DeltaDVRoaringBitmapArray> createBitmapExpr(
    const ContextPtr & context,
    const Block & /*data_file_header*/,
    const SubstraitInputFile & file_,
    const RepeatedPtrField<SubstraitIcebergDeleteFile> & delete_files,
    const std::vector<int> & position_delete_files,
    Block & reader_header)
{
    assert(!position_delete_files.empty());

    std::unique_ptr<DeltaDVRoaringBitmapArray> result = std::make_unique<DeltaDVRoaringBitmapArray>();

    for (auto deleteIndex : position_delete_files)
    {
        const auto & delete_file = delete_files[deleteIndex];
        assert(delete_file.filecontent() == IcebergReadOptions::POSITION_DELETES);
        if (delete_file.recordcount() == 0)
            continue;

        ActionsDAG actions_dag{IcebergMetadataColumn::getNamesAndTypesList()};
        ActionsDAG::NodeRawConstPtrs filter_node;
        {
            ActionsDAG & actions = actions_dag;
            const std::string Equal{"equals"};
            auto equalBuilder = FunctionFactory::instance().get(Equal, context);

            ActionsDAG::NodeRawConstPtrs args;
            args.push_back(&actions.findInOutputs(IcebergMetadataColumn::icebergDeleteFilePathColumn()->name));
            args.push_back(&actions.addColumn(createColumnConst<std::string>(1, file_.uri_file(), "_")));
            filter_node.push_back(&actions.addFunction(equalBuilder, std::move(args), "__"));
        }

        auto filter = ActionsDAG::buildFilterActionsDAG(filter_node);

        // Block header{{{IcebergMetadataColumn::icebergDeletePosColumn()->type, IcebergMetadataColumn::icebergDeletePosColumn()->name}}};
        Block header{
            {{IcebergMetadataColumn::icebergDeleteFilePathColumn()->type, IcebergMetadataColumn::icebergDeleteFilePathColumn()->name},
             {IcebergMetadataColumn::icebergDeletePosColumn()->type, IcebergMetadataColumn::icebergDeletePosColumn()->name}}};

        SimpleParquetReader reader{context, delete_file, std::move(header), filter};
        Block deleteBlock = reader.next();

        while (deleteBlock.rows() > 0)
        {
            assert(deleteBlock.columns() == 2);
            const auto * pos_column = typeid_cast<const ColumnInt64 *>(deleteBlock.getByPosition(1).column.get());
            if (pos_column == nullptr)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ColumnInt64 for position deletes");

            const ColumnInt64::Container & vec = pos_column->getData();
            const Int64 * pos = vec.data();
            for (int i = 0; i < deleteBlock.rows(); i++)
                result->rb_add(pos[i]);

            deleteBlock = reader.next();
        }
    }

    if (result->rb_is_empty())
        return nullptr;

    if (!ParquetVirtualMeta::hasMetaColumns(reader_header))
        reader_header.insert({BIGINT(), ParquetVirtualMeta::TMP_ROWINDEX});
    return std::move(result);
}

}