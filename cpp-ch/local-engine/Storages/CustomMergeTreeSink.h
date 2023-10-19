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

#include <Processors/ISink.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/StorageInMemoryMetadata.h>
#include "CustomStorageMergeTree.h"

namespace local_engine
{
class CustomMergeTreeSink : public ISink
{
public:
    CustomMergeTreeSink(CustomStorageMergeTree & storage_, const StorageMetadataPtr metadata_snapshot_, ContextPtr context_)
        : ISink(metadata_snapshot_->getSampleBlock()), storage(storage_), metadata_snapshot(metadata_snapshot_), context(context_)
    {
    }

    String getName() const override { return "CustomMergeTreeSink"; }
    void consume(Chunk chunk) override;

private:
    CustomStorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
};

}
