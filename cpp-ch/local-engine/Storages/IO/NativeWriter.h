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

#include <base/types.h>
#include <DataTypes/IDataType.h>
#include <Core/Block.h>

namespace DB
{
class WriteBuffer;
class CompressedWriteBuffer;
}

namespace local_engine
{

class NativeWriter
{
public:
    static const String AGG_STATE_SUFFIX;
    NativeWriter(
        DB::WriteBuffer & ostr_, const DB::Block & header_): ostr(ostr_), header(header_)
    {}

    DB::Block getHeader() const { return header; }
    /// Returns the number of bytes written.
    size_t write(const DB::Block & block);
    void flush();


private:
    DB::WriteBuffer & ostr;
    DB::Block header;
};
}
