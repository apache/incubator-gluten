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
#include <fstream>
#include <iostream>
#include <Builder/SerializedPlanBuilder.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/SparkRowToCHColumn.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/CustomMergeTreeSink.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <gtest/gtest.h>
#include <substrait/plan.pb.h>
#include <Common/CHUtil.h>

using namespace local_engine;
using namespace dbms;

TEST(TESTUtil, TestByteToLong)
{
    Int64 expected = 0xf085460ccf7f0000l;
    char * arr = new char[8];
    arr[0] = -16;
    arr[1] = -123;
    arr[2] = 70;
    arr[3] = 12;
    arr[4] = -49;
    arr[5] = 127;
    arr[6] = 0;
    arr[7] = 0;
    std::reverse(arr, arr + 8);
    Int64 result = reinterpret_cast<Int64 *>(arr)[0];
    std::cout << std::to_string(result);

    ASSERT_EQ(expected, result);
}

TEST(ReadBufferFromFile, seekBackwards)
{
    static constexpr size_t N = 256;
    static constexpr size_t BUF_SIZE = 64;

    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path());
        for (size_t i = 0; i < N; ++i)
            writeIntBinary(i, out);
    }

    ReadBufferFromFile in(tmp_file->path(), BUF_SIZE);
    size_t x;

    /// Read something to initialize the buffer.
    in.seek(BUF_SIZE * 10, SEEK_SET);
    readIntBinary(x, in);

    /// Check 2 consecutive  seek calls without reading.
    in.seek(BUF_SIZE * 2, SEEK_SET);
    //    readIntBinary(x, in);
    in.seek(BUF_SIZE, SEEK_SET);

    readIntBinary(x, in);
    ASSERT_EQ(x, 8);
}

int main(int argc, char ** argv)
{
    auto * init = new String("{\"advancedExtensions\":{\"enhancement\":{\"@type\":\"type.googleapis.com/substrait.Expression\",\"literal\":{\"map\":{\"keyValues\":[{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level\"},\"value\":{\"string\":\"trace\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_settings.max_bytes_before_external_sort\"},\"value\":{\"string\":\"5368709120\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.endpoint\"},\"value\":{\"string\":\"localhost:9000\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.velox.IOThreads\"},\"value\":{\"string\":\"0\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.input_read_timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_settings.query_plan_enable_optimizations\"},\"value\":{\"string\":\"false\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.worker.id\"},\"value\":{\"string\":\"1\"}},{\"key\":{\"string\":\"spark.memory.offHeap.enabled\"},\"value\":{\"string\":\"true\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.iam.role.session.name\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.input_connect_timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.shuffle.codec\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.local_engine.settings.log_processors_profiles\"},\"value\":{\"string\":\"true\"}},{\"key\":{\"string\":\"spark.gluten.memory.offHeap.size.in.bytes\"},\"value\":{\"string\":\"10737418240\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.shuffle.codecBackend\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.sql.orc.compression.codec\"},\"value\":{\"string\":\"snappy\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_settings.max_bytes_before_external_group_by\"},\"value\":{\"string\":\"5368709120\"}},{\"key\":{\"string\":\"spark.hadoop.input.write.timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.secret.key\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.access.key\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.dfs_client_log_severity\"},\"value\":{\"string\":\"INFO\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.path.style.access\"},\"value\":{\"string\":\"true\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.timezone\"},\"value\":{\"string\":\"Asia/Shanghai\"}},{\"key\":{\"string\":\"spark.hadoop.input.read.timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.use.instance.credentials\"},\"value\":{\"string\":\"false\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_settings.output_format_orc_compression_method\"},\"value\":{\"string\":\"snappy\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.iam.role\"},\"value\":{\"string\":\"\"}},{\"key\":{\"string\":\"spark.gluten.memory.task.offHeap.size.in.bytes\"},\"value\":{\"string\":\"10737418240\"}},{\"key\":{\"string\":\"spark.hadoop.input.connect.timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.hadoop.dfs.client.log.severity\"},\"value\":{\"string\":\"INFO\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver\"},\"value\":{\"string\":\"2\"}},{\"key\":{\"string\":\"spark.gluten.sql.columnar.backend.ch.runtime_config.hdfs.input_write_timeout\"},\"value\":{\"string\":\"180000\"}},{\"key\":{\"string\":\"spark.hadoop.fs.s3a.connection.ssl.enabled\"},\"value\":{\"string\":\"false\"}}]}}}}}");

    BackendInitializerUtil::init_json(std::move(init));
    SCOPE_EXIT({ BackendFinalizerUtil::finalizeGlobally(); });

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
