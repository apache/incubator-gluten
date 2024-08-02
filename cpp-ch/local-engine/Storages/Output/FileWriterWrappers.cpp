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
#include "FileWriterWrappers.h"

namespace local_engine
{

const std::string SubstraitFileSink::NO_PARTITION_ID{"__NO_PARTITION_ID__"};
const std::string SubstraitPartitionedFileSink::DEFAULT_PARTITION_NAME{"__HIVE_DEFAULT_PARTITION__"};

NormalFileWriter::NormalFileWriter(const OutputFormatFilePtr & file_, const DB::ContextPtr & context_)
    : FileWriterWrapper(file_), context(context_)
{
}

void NormalFileWriter::consume(DB::Block & block)
{
    if (!writer) [[unlikely]]
    {
        // init the writer at first block
        output_format = file->createOutputFormat(block.cloneEmpty());
        pipeline = std::make_unique<DB::QueryPipeline>(output_format->output);
        writer = std::make_unique<DB::PushingPipelineExecutor>(*pipeline);
    }

    /// In case input block didn't have the same types as the preferred schema, we cast the input block to the preferred schema.
    /// Notice that preferred_schema is the actual file schema, which is also the data schema of current inserted table.
    /// Refer to issue: https://github.com/apache/incubator-gluten/issues/6588
    size_t index = 0;
    const auto & preferred_schema = file->getPreferredSchema();
    for (auto & column : block)
    {
        if (column.name.starts_with("__bucket_value__"))
            continue;

        const auto & preferred_column = preferred_schema.getByPosition(index++);
        column.column = DB::castColumn(column, preferred_column.type);
        column.name = preferred_column.name;
        column.type = preferred_column.type;
    }

    /// Although gluten will append MaterializingTransform to the end of the pipeline before native insert in most cases, there are some cases in which MaterializingTransform won't be appended.
    /// e.g. https://github.com/oap-project/gluten/issues/2900
    /// So we need to do materialize here again to make sure all blocks passed to native writer are all materialized.
    /// Note: duplicate materialization on block doesn't has any side affect.
    writer->push(materializeBlock(block));
}

void NormalFileWriter::close()
{
    /// When insert into a table with empty dataset, NormalFileWriter::consume would be never called.
    /// So we need to skip when writer is nullptr.
    if (writer)
        writer->finish();
}

OutputFormatFilePtr createOutputFormatFile(
    const DB::ContextPtr & context, const std::string & file_uri, const DB::Block & preferred_schema, const std::string & format_hint)
{
    // the passed in file_uri is exactly what is expected to see in the output folder
    // e.g /xxx/中文/timestamp_field=2023-07-13 03%3A00%3A17.622/abc.parquet
    LOG_INFO(&Poco::Logger::get("FileWriterWrappers"), "Create native writer, format_hint: {}, file: {}", format_hint, file_uri);
    std::string encoded;
    Poco::URI::encode(file_uri, "", encoded); // encode the space and % seen in the file_uri
    Poco::URI poco_uri(encoded);
    auto write_buffer_builder = WriteBufferBuilderFactory::instance().createBuilder(poco_uri.getScheme(), context);
    return OutputFormatFileUtil::createFile(context, write_buffer_builder, encoded, preferred_schema, format_hint);
}

std::unique_ptr<FileWriterWrapper> createFileWriterWrapper(
    const DB::ContextPtr & context, const std::string & file_uri, const DB::Block & preferred_schema, const std::string & format_hint)
{
    return std::make_unique<NormalFileWriter>(createOutputFormatFile(context, file_uri, preferred_schema, format_hint), context);
}

}
