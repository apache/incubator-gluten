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

#include "ReaderTestBase.h"
#include <Storages/Output/NormalFileWriter.h>
#include <base/demangle.h>
#include <Poco/URI.h>
#include <Common/QueryContext.h>
#include <Common/logger_useful.h>
#include <Common/DebugUtils.h>

using namespace DB;

namespace local_engine::test
{


void ReaderTestBase::writeToFile(const std::string & filePath, const DB::Block & block)
{
    const auto context = QueryContext::instance().currentQueryContext();
    const Poco::Path file{filePath};
    const Poco::URI fileUri{file};
    const auto writer = NormalFileWriter::create(context, fileUri.toString(), block, file.getExtension());
    writer->write(block);
    writer->close();
}

void ReaderTestBase::SetUp()
{
    EXPECT_EQ(query_id_, 0) << "query_id_ should be 0 at the beginning of the test";
    query_id_ = QueryContext::instance().initializeQuery(demangle(typeid(*this).name()));
}

void ReaderTestBase::TearDown()
{
    EXPECT_NE(query_id_, 0) << "query_id_ should not be 0 at the end of the test";
    QueryContext::instance().finalizeQuery(query_id_);
    query_id_ = 0;
}

void ReaderTestBase::headBlock(const DB::Block & block, size_t count, size_t truncate) const
{
    LOG_INFO(test_logger, "\n{}", debug::showString(block, count, truncate));
}

void ReaderTestBase::headColumn(const DB::ColumnPtr & column, size_t count, size_t truncate) const
{
    LOG_INFO(test_logger, "\n{}", debug::showString(column, count, truncate));

}

}
