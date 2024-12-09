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
#include "BlockIterator.h"
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
void local_engine::BlockIterator::checkNextValid() const
{
    if (consumed)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Block iterator next should after hasNext");
    }
}

void BlockIterator::produce()
{
    consumed = false;
}

void BlockIterator::consume()
{
    consumed = true;
}

bool BlockIterator::isConsumed() const
{
    return consumed;
}

DB::Block & BlockIterator::currentBlock()
{
    return cached_block;
}

void BlockIterator::setCurrentBlock(DB::Block & block)
{
    cached_block = block;
}

}
