#pragma once
#include <Interpreters/JoinUtils.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
class TableJoin;
class HashJoin;
using HashJoinPtr = std::shared_ptr<HashJoin>;
}

namespace local_engine
{

class StorageJoinFromReadBuffer
{
public:
    StorageJoinFromReadBuffer(
        std::unique_ptr<DB::ReadBuffer> in_,
        const DB::Names & key_names_,
        bool use_nulls_,
        DB::SizeLimits limits_,
        DB::JoinKind kind_,
        DB::JoinStrictness strictness_,
        const DB::ColumnsDescription & columns_,
        const DB::ConstraintsDescription & constraints_,
        const String & comment,
        bool overwrite_);

    DB::HashJoinPtr getJoinLocked(std::shared_ptr<DB::TableJoin> analyzed_join, DB::ContextPtr context) const;
    DB::Block getRightSampleBlock() const
    {
        DB::Block block = storage_metadata_.getSampleBlock();
        if (use_nulls && isLeftOrFull(kind))
        {
            for (auto & col : block)
            {
                DB::JoinCommon::convertColumnToNullable(col);
            }
        }
        return block;
    }

protected:
    void restore();

private:
    DB::StorageInMemoryMetadata storage_metadata_;
    DB::Block sample_block;
    const DB::Names key_names;
    bool use_nulls;
    DB::SizeLimits limits;
    DB::JoinKind kind; /// LEFT | INNER ...
    DB::JoinStrictness strictness; /// ANY | ALL
    bool overwrite;

    std::shared_ptr<DB::TableJoin> table_join;
    DB::HashJoinPtr join;

    std::unique_ptr<DB::ReadBuffer> in;
};
}
