#include "StorageJoinFromReadBuffer.h"

#include <Formats/NativeReader.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int DEADLOCK_AVOIDED;
}
}

using namespace DB;

namespace local_engine
{

void StorageJoinFromReadBuffer::restore()
{
    if (!in)
    {
        throw std::runtime_error("input reader buffer is not available");
    }
    ContextPtr ctx = nullptr;
    NativeReader block_stream(*in, 0);

    ProfileInfo info;
    {
        while (Block block = block_stream.read())
        {
            auto final_block = sample_block.cloneWithColumns(block.mutateColumns());
            info.update(final_block);
            join->addBlockToJoin(final_block, true);
        }
    }
    in.reset();
}

StorageJoinFromReadBuffer::StorageJoinFromReadBuffer(
    std::unique_ptr<DB::ReadBuffer> in_,
    const Names & key_names_,
    bool use_nulls_,
    DB::SizeLimits limits_,
    DB::JoinKind kind_,
    DB::JoinStrictness strictness_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    const bool overwrite_)
    : key_names(key_names_)
    , use_nulls(use_nulls_)
    , limits(limits_)
    , kind(kind_)
    , strictness(strictness_)
    , overwrite(overwrite_)
    , in(std::move(in_))
{
    storage_metadata_.setColumns(columns_);
    storage_metadata_.setConstraints(constraints_);
    storage_metadata_.setComment(comment);

    sample_block = storage_metadata_.getSampleBlock();
    for (const auto & key : key_names)
        if (!storage_metadata_.getColumns().hasPhysical(key))
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Key column ({}) does not exist in table declaration.", key);

    table_join = std::make_shared<TableJoin>(limits, use_nulls, kind, strictness, key_names);
    join = std::make_shared<HashJoin>(table_join, getRightSampleBlock(), overwrite);
    restore();
}

DB::HashJoinPtr StorageJoinFromReadBuffer::getJoinLocked(std::shared_ptr<DB::TableJoin> analyzed_join, DB::ContextPtr /*context*/) const
{
    if (!analyzed_join->sameStrictnessAndKind(strictness, kind))
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "Table {} has incompatible type of JOIN.", storage_metadata_.comment);

    if ((analyzed_join->forceNullableRight() && !use_nulls)
        || (!analyzed_join->forceNullableRight() && isLeftOrFull(analyzed_join->kind()) && use_nulls))
        throw Exception(
            ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN,
            "Table {} needs the same join_use_nulls setting as present in LEFT or FULL JOIN",
            storage_metadata_.comment);

    /// TODO: check key columns

    /// Set names qualifiers: table.column -> column
    /// It's required because storage join stores non-qualified names
    /// Qualifies will be added by join implementation (HashJoin)
    analyzed_join->setRightKeys(key_names);

    HashJoinPtr join_clone = std::make_shared<HashJoin>(analyzed_join, getRightSampleBlock());
    join_clone->reuseJoinedData(*join);

    return join_clone;
}
}
