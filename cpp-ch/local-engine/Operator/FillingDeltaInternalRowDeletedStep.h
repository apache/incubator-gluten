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
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/SubstraitSource/Delta/Bitmap/DeltaDVRoaringBitmapArray.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>

namespace local_engine
{

class FillingDeltaInternalRowDeletedStep : public DB::ITransformingStep
{
public:
    explicit FillingDeltaInternalRowDeletedStep(const DB::Block & input_header, const MergeTreeTableInstance & _merge_tree_table, const DB::ContextPtr _context);
    ~FillingDeltaInternalRowDeletedStep() override = default;

    static DB::Block transformHeader(const DB::Block & input);

    String getName() const override { return "FillingDeltaInternalRowDeletedStep"; }
    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;

private:
    MergeTreeTableInstance merge_tree_table;
    DB::ContextPtr context;
    void updateOutputHeader() override;
};

class FillingDeltaInternalRowDeletedTransform : public DB::ISimpleTransform
{
public:
    FillingDeltaInternalRowDeletedTransform(const DB::Block & input_header_, const MergeTreeTableInstance & merge_tree_table, const DB::ContextPtr context);
    ~FillingDeltaInternalRowDeletedTransform() override = default;

    String getName() const override { return "FillingDeltaInternalRowDeletedTransform"; }
    void transform(DB::Chunk &) override;

private:
    DB::Block read_header;
    std::unordered_map<String, std::unique_ptr<DeltaDVRoaringBitmapArray>> dv_map;
};
}
