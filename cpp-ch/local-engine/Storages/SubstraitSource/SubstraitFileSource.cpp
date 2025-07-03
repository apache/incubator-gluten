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

#include "SubstraitFileSource.h"
#include <Storages/Parquet/ColumnIndexFilter.h>
#include <Storages/SubstraitSource/FileReader.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Poco/URI.h>
#include <Common/CHUtil.h>

namespace local_engine
{

static std::vector<FormatFilePtr> initializeFiles(const substrait::ReadRel::LocalFiles & file_infos, const DB::ContextPtr & context)
{
    if (file_infos.items().empty())
        return {};
    std::vector<FormatFilePtr> files;
    const Poco::URI file_uri(file_infos.items().Get(0).uri_file());
    ReadBufferBuilderPtr read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context);
    for (const auto & item : file_infos.items())
        files.emplace_back(FormatFileUtil::createFile(context, read_buffer_builder, item));
    return files;
}

static DB::Block initReadHeader(const DB::Block & block, const FormatFiles & files)
{
    if (files.empty())
        return block;
    const auto & partitions = files[0]->getFilePartitionValues();
    const auto & fileMetaColumns = files[0]->fileMetaColumns();
    DB::ColumnsWithTypeAndName result_columns;
    std::ranges::copy_if(
        block.getColumnsWithTypeAndName(),
        std::back_inserter(result_columns),
        [&partitions, &fileMetaColumns](const auto & column)
        { return !partitions.contains(column.name) && !fileMetaColumns.virtualColumn(column.name); });
    return result_columns;
}

SubstraitFileSource::SubstraitFileSource(
    const DB::ContextPtr & context_, const DB::Block & outputHeader_, const substrait::ReadRel::LocalFiles & file_infos)
    : DB::SourceWithKeyCondition(BaseReader::buildRowCountHeader(outputHeader_), false)
    , files(initializeFiles(file_infos, context_))
    , outputHeader(outputHeader_)
    , readHeader(initReadHeader(outputHeader, files))
{
}

SubstraitFileSource::~SubstraitFileSource() = default;

void SubstraitFileSource::setKeyCondition(const std::optional<DB::ActionsDAG> & filter_actions_dag, DB::ContextPtr context_)
{
    setKeyConditionImpl(filter_actions_dag, context_, readHeader);
    if (filter_actions_dag)
        column_index_filter = std::make_shared<ColumnIndexFilter>(filter_actions_dag.value(), context_);
}

DB::Chunk SubstraitFileSource::generate()
{
    while (true)
    {
        if (!tryPrepareReader())
        {
            /// all files finished
            return {};
        }

        DB::Chunk chunk;
        if (file_reader->pull(chunk))
            return chunk;

        /// try to read from next file
        file_reader.reset();
    }
}

bool SubstraitFileSource::tryPrepareReader()
{
    if (isCancelled())
        return false;

    if (file_reader)
        return true;

    while (current_file_index < files.size())
    {
        auto current_file = files[current_file_index];
        current_file_index += 1;
        /// For the files do not support split strategy, the task with not 0 offset will generate empty data
        if (!current_file->supportSplit() && current_file->getStartOffset())
            continue;

        file_reader = BaseReader::create(current_file, readHeader, outputHeader, key_condition, column_index_filter);
        if (file_reader)
            return true;
    }
    return false;
}


void SubstraitFileSource::onCancel() noexcept
{
    if (file_reader)
        file_reader->cancel();
}

}
