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

#include <Processors/ISimpleTransform.h>

namespace local_engine
{
class PartitionColumnFillingTransform : public DB::ISimpleTransform
{
public:
    PartitionColumnFillingTransform(
        const DB::Block & input_, const DB::Block & output_, const String & partition_col_name_, const String & partition_col_value_);

    String getName() const override { return "PartitionColumnFillingTransform"; }

    void transform(DB::Chunk & chunk) override;

private:
    DB::ColumnPtr createPartitionColumn();

    DB::DataTypePtr partition_col_type;
    String partition_col_name;
    String partition_col_value;
    DB::ColumnPtr partition_column;
};

}
