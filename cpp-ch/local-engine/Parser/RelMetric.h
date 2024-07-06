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
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <rapidjson/prettywriter.h>

namespace local_engine
{

class RelMetric;
using RelMetricPtr = std::shared_ptr<RelMetric>;

struct RelMetricTimes
{
    // Number of time this processor was executing.
    size_t time;
    // Number of time this processor was waiting for data (from other processor).
    size_t input_wait_elapsed_us;
    // Number of time this processor was waiting because output port was full.
    size_t output_wait_elapsed_us;
};

class RelMetric
{
public:
    explicit RelMetric(size_t id, const String & name, std::vector<DB::IQueryPlanStep *> & steps);
    explicit RelMetric(const String & name, const std::vector<RelMetricPtr> & inputs, std::vector<DB::IQueryPlanStep *> & steps);

    size_t getId() const;
    const String & getName() const;
    const std::vector<DB::IQueryPlanStep *> & getSteps() const;
    const std::vector<RelMetricPtr> & getInputs() const;
    RelMetricTimes getTotalTime() const;
    void serialize(rapidjson::Writer<rapidjson::StringBuffer> & writer, bool summary = true) const;

private:
    size_t id;
    String name;
    // query plan is from query plan
    std::vector<DB::IQueryPlanStep *> steps;
    std::vector<RelMetricPtr> inputs;
};

class RelMetricSerializer
{
public:
    static std::string serializeRelMetric(const RelMetricPtr & rel_metric, bool flatten = true);
};
}
