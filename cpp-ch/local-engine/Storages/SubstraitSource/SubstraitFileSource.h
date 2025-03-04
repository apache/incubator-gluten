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

#include <memory>
#include <Processors/SourceWithKeyCondition.h>
#include <substrait/algebra.pb.h>

namespace local_engine
{
class ColumnIndexFilter;
using ColumnIndexFilterPtr = std::shared_ptr<ColumnIndexFilter>;
class BaseReader;
class FormatFile;
using FormatFilePtr = std::shared_ptr<FormatFile>;
using FormatFiles = std::vector<FormatFilePtr>;

class SubstraitFileSource : public DB::SourceWithKeyCondition
{
public:
    SubstraitFileSource(const DB::ContextPtr & context_, const DB::Block & header_, const substrait::ReadRel::LocalFiles & file_infos);
    ~SubstraitFileSource() override;

    String getName() const override { return "SubstraitFileSource"; }

    void setKeyCondition(const std::optional<DB::ActionsDAG> & filter_actions_dag, DB::ContextPtr context_) override;

protected:
    DB::Chunk generate() override;

private:
    bool tryPrepareReader();
    void onCancel() noexcept override;
    FormatFiles files;

    DB::Block outputHeader; /// Sample header may contain partitions columns and file meta-columns
    DB::Block readHeader; /// Sample header doesn't include partition columns and file meta-columns

    UInt32 current_file_index = 0;

    std::unique_ptr<BaseReader> file_reader;
    ColumnIndexFilterPtr column_index_filter;
};
}
