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
//std::list<OutputPort> local_engine::CustomMergeTreeSink::getOutputs()
//{
//    return {};
//}
