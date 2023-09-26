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

#include <Common/PODArray.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeAggregateFunction.h>

namespace local_engine
{

class NativeReader
{
public:
    NativeReader(DB::ReadBuffer & istr_) : istr(istr_) {}

    static void readData(const DB::ISerialization & serialization, DB::ColumnPtr & column, DB::ReadBuffer & istr, size_t rows, double avg_value_size_hint);
    static void readAggData(const DB::DataTypeAggregateFunction & data_type, DB::ColumnPtr & column, DB::ReadBuffer & istr, size_t rows);

    DB::Block getHeader() const;

    DB::Block read();

private:
    DB::ReadBuffer & istr;
    DB::Block header;

    DB::PODArray<double> avg_value_size_hints;

    void updateAvgValueSizeHints(const DB::Block & block);
};

}
