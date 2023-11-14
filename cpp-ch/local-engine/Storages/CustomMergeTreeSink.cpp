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
#include "CustomMergeTreeSink.h"

void local_engine::CustomMergeTreeSink::consume(Chunk chunk)
{
    auto block = metadata_snapshot->getSampleBlock().cloneWithColumns(chunk.detachColumns());
    DB::BlockWithPartition block_with_partition(Block(block), DB::Row{});
    auto part = storage.writer.writeTempPart(block_with_partition, metadata_snapshot, context);
    MergeTreeData::Transaction transaction(storage, NO_TRANSACTION_RAW);
    {
        auto lock = storage.lockParts();
        storage.renameTempPartAndAdd(part.part, transaction, lock);
        transaction.commit(&lock);
    }
}
