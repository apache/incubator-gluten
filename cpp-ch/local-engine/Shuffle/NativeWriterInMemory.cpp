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
#include "NativeWriterInMemory.h"

using namespace DB;

namespace local_engine
{
NativeWriterInMemory::NativeWriterInMemory()
{
    write_buffer = std::make_unique<WriteBufferFromOwnString>();
}
void NativeWriterInMemory::write(Block & block)
{
    if (block.columns() == 0 || block.rows() == 0)
        return;
    if (!writer)
    {
        writer = std::make_unique<NativeWriter>(*write_buffer, 0, block.cloneEmpty());
    }
    writer->write(block);
}
std::string & NativeWriterInMemory::collect()
{
    return write_buffer->str();
}
}
