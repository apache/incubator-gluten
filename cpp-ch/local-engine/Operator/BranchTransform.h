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
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

namespace local_engine
{

// This is designed for adaptive execution. It has multiple outputs, each indicates for a execution branches.
// It accepts a branch selector, this selector will analysis the input data, and select one of the output port
// as the final only output port.  Other output ports will be closed.
// The output port cannot be changed once it's selected.
class StaticBranchTransform : public DB::IProcessor
{
public:
    using BranchSelector = std::function<size_t(const std::list<DB::Chunk> &)>;
    using Status = DB::IProcessor::Status;
    StaticBranchTransform(const DB::Block & header_, size_t sample_rows_, size_t branches_, BranchSelector selector_);

    String getName() const override { return "StaticBranchTransform"; }

    Status prepare() override;
    void work() override;

private:
    size_t max_sample_rows;
    BranchSelector selector;
    DB::OutputPort * selected_output_port = nullptr;
    std::list<DB::Chunk> sample_chunks;
    size_t sample_rows = 0;
    bool has_input = false;
    bool has_output = false;
    DB::Chunk input_chunk;
    DB::Chunk output_chunk;

    void setupOutputPort();
};

};
