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
#include "BlockCoalesceOperator.h"
#include <Core/Block.h>

namespace local_engine
{

void BlockCoalesceOperator::mergeBlock(DB::Block & block)
{
    block_buffer.add(block, 0, static_cast<int>(block.rows()));
}

bool BlockCoalesceOperator::isFull()
{
    return block_buffer.size() >= buf_size;
}

DB::Block * BlockCoalesceOperator::releaseBlock()
{
    clearCache();
    cached_block = new DB::Block(block_buffer.releaseColumns());
    return cached_block;
}

BlockCoalesceOperator::~BlockCoalesceOperator()
{
    clearCache();
}

void BlockCoalesceOperator::clearCache()
{
    if (cached_block)
    {
        delete cached_block;
        cached_block = nullptr;
    }
}
}
