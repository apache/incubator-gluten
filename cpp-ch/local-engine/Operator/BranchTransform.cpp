
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
#include "BranchTransform.h"
#include <iterator>
#include <Processors/IProcessor.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace local_engine
{
static DB::OutputPorts buildOutputPorts(const DB::Block & header, size_t branches)
{
    DB::OutputPorts output_ports;
    for (size_t i = 0; i < branches; ++i)
        output_ports.emplace_back(header);
    return output_ports;
}
StaticBranchTransform::StaticBranchTransform(const DB::Block & header_, size_t sample_rows_, size_t branches_, BranchSelector selector_)
    : DB::IProcessor({header_}, buildOutputPorts(header_, branches_)), max_sample_rows(sample_rows_), selector(selector_)
{
}

static bool existFinishedOutput(const DB::OutputPorts & output_ports)
{
    for (const auto & output_port : output_ports)
        if (output_port.isFinished())
            return true;
    return false;
}

StaticBranchTransform::Status StaticBranchTransform::prepare()
{
    auto & input = inputs.front();
    if ((selected_output_port && selected_output_port->isFinished()) || (!selected_output_port && existFinishedOutput(outputs)))
    {
        input.close();
        return Status::Finished;
    }

    if (has_output)
    {
        assert(selected_output_port != nullptr);
        if (selected_output_port->canPush())
        {
            selected_output_port->push(std::move(output_chunk));
            has_output = false;
        }
        return Status::PortFull;
    }

    if (has_input || (selected_output_port && !sample_chunks.empty()))
    {
        // to clear the pending chunks
        return Status::Ready;
    }

    if (input.isFinished())
    {
        if (!sample_chunks.empty())
        {
            // to clear the pending chunks
            return Status::Ready;
        }
        else
        {
            if (selected_output_port)
                selected_output_port->finish();
            else
                for (auto & output_port : outputs)
                    output_port.finish();
            return Status::Finished;
        }
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    input_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void StaticBranchTransform::work()
{
    if (selected_output_port)
    {
        if (!sample_chunks.empty())
        {
            assert(!has_input);
            has_output = true;
            output_chunk.swap(sample_chunks.front());
            sample_chunks.pop_front();
        }
        else
        {
            assert(has_input);
            has_input = false;
            has_output = true;
            output_chunk.swap(input_chunk);
        }
    }
    else if (has_input)
    {
        sample_rows += input_chunk.getNumRows();
        sample_chunks.emplace_back(std::move(input_chunk));
        if (sample_rows >= max_sample_rows)
            setupOutputPort();
        has_input = false;
    }
    else if (!sample_chunks.empty())
    {
        if (!selected_output_port)
            setupOutputPort();
        output_chunk.swap(sample_chunks.front());
        sample_chunks.pop_front();
        has_output = true;
    }
}

void StaticBranchTransform::setupOutputPort()
{
    size_t branch_index = selector(sample_chunks);
    LOG_DEBUG(getLogger("StaticBranchTransform"), "Select output port: {}", branch_index);
    if (branch_index >= outputs.size())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Branch index {} is out of range(0, {})", branch_index, outputs.size());
    auto it = outputs.begin();
    std::advance(it, branch_index);
    selected_output_port = &(*it);
    // close other output ports
    for (auto oit = outputs.begin(); oit != outputs.end(); ++oit)
        if (oit != it)
            oit->finish();
}
} // namespace local_engine
