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

#include <Core/Block.h>

namespace local_engine
{
struct BlockStripes
{
    int64_t origin_block_address = 0;
    std::vector<int64_t> block_addresses;
    std::vector<int32_t> heading_row_indice;
    int origin_block_num_columns = 0;
};

class BlockStripeSplitter
{
public:
    static BlockStripes split(const DB::Block & block, const std::vector<size_t> & partition_col_indice, bool has_bucket);
};

}
