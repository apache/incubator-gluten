#include "StorageJoinFromReadBuffer.h"

#include <Compression/CompressedReadBuffer.h>
#include <Formats/NativeReader.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/joinDispatch.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
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
}

template <typename T>
static const char * rawData(T & t)
{
    return reinterpret_cast<const char *>(&t);
}
template <typename T>
static size_t rawSize(T &)
{
    return sizeof(T);
}
template <>
const char * rawData(const StringRef & t)
{
    return t.data;
}
template <>
size_t rawSize(const StringRef & t)
{
    return t.size;
}

class JoinSource : public ISource
{
public:
    JoinSource(HashJoinPtr join_, TableLockHolder lock_holder_, UInt64 max_block_size_, Block sample_block_)
        : ISource(sample_block_)
        , join(join_)
        , lock_holder(lock_holder_)
        , max_block_size(max_block_size_)
        , sample_block(std::move(sample_block_))
    {
        if (!join->getTableJoin().oneDisjunct())
            throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageJoin does not support OR for keys in JOIN ON section");

        column_indices.resize(sample_block.columns());

        auto & saved_block = join->getJoinedData()->sample_block;

        for (size_t i = 0; i < sample_block.columns(); ++i)
        {
            auto & [_, type, name] = sample_block.getByPosition(i);
            if (join->right_table_keys.has(name))
            {
                key_pos = i;
                const auto & column = join->right_table_keys.getByName(name);
                restored_block.insert(column);
            }
            else
            {
                size_t pos = saved_block.getPositionByName(name);
                column_indices[i] = pos;

                const auto & column = saved_block.getByPosition(pos);
                restored_block.insert(column);
            }
        }
    }

    String getName() const override { return "Join"; }

protected:
    Chunk generate() override
    {
        if (join->data->blocks.empty())
            return {};

        Chunk chunk;
        if (!joinDispatch(
                join->kind,
                join->strictness,
                join->data->maps.front(),
                [this, &chunk](auto kind, auto strictness, auto & map) { chunk = createChunk<kind, strictness>(map); }))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: unknown JOIN strictness");
        return chunk;
    }

private:
    HashJoinPtr join;
    TableLockHolder lock_holder;

    UInt64 max_block_size;
    Block sample_block;
    Block restored_block; /// sample_block with parent column types

    ColumnNumbers column_indices;
    std::optional<size_t> key_pos;

    std::unique_ptr<void, std::function<void(void *)>> position; /// type erasure

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename Maps>
    Chunk createChunk(const Maps & maps)
    {
        MutableColumns mut_columns = restored_block.cloneEmpty().mutateColumns();

        size_t rows_added = 0;

        switch (join->data->type)
        {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        rows_added = fillColumns<KIND, STRICTNESS>(*maps.TYPE, mut_columns); \
        break;
            APPLY_FOR_JOIN_VARIANTS_LIMITED(M)
#undef M

            default:
                throw Exception(
                    ErrorCodes::UNSUPPORTED_JOIN_KEYS,
                    "Unsupported JOIN keys in StorageJoin. Type: {}",
                    toString(static_cast<UInt32>(join->data->type)));
        }

        if (!rows_added)
            return {};

        Columns columns;
        columns.reserve(mut_columns.size());
        for (auto & col : mut_columns)
            columns.emplace_back(std::move(col));

        /// Correct nullability and LowCardinality types
        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto & src = restored_block.getByPosition(i);
            const auto & dst = sample_block.getByPosition(i);

            if (!src.type->equals(*dst.type))
            {
                auto arg = src;
                arg.column = std::move(columns[i]);
                columns[i] = castColumn(arg, dst.type);
            }
        }

        UInt64 num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }

    template <JoinKind KIND, JoinStrictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns)
    {
        size_t rows_added = 0;

        if (!position)
            position = decltype(position)(
                static_cast<void *>(new typename Map::const_iterator(map.begin())), //-V572
                [](void * ptr) { delete reinterpret_cast<typename Map::const_iterator *>(ptr); });

        auto & it = *reinterpret_cast<typename Map::const_iterator *>(position.get());
        auto end = map.end();

        for (; it != end; ++it)
        {
            if constexpr (STRICTNESS == JoinStrictness::RightAny)
            {
                fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == JoinStrictness::All)
            {
                fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == JoinStrictness::Any)
            {
                if constexpr (KIND == JoinKind::Left || KIND == JoinKind::Inner)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == JoinKind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == JoinStrictness::Semi)
            {
                if constexpr (KIND == JoinKind::Left)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == JoinKind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else if constexpr (STRICTNESS == JoinStrictness::Anti)
            {
                if constexpr (KIND == JoinKind::Left)
                    fillOne<Map>(columns, column_indices, it, key_pos, rows_added);
                else if constexpr (KIND == JoinKind::Right)
                    fillAll<Map>(columns, column_indices, it, key_pos, rows_added);
            }
            else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This JOIN is not implemented yet");

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

        return rows_added;
    }

    template <typename Map>
    static void fillOne(
        MutableColumns & columns,
        const ColumnNumbers & column_indices,
        typename Map::const_iterator & it,
        const std::optional<size_t> & key_pos,
        size_t & rows_added)
    {
        for (size_t j = 0; j < columns.size(); ++j)
            if (j == key_pos)
                columns[j]->insertData(rawData(it->getKey()), rawSize(it->getKey()));
            else
                columns[j]->insertFrom(*it->getMapped().block->getByPosition(column_indices[j]).column.get(), it->getMapped().row_num);
        ++rows_added;
    }

    template <typename Map>
    static void fillAll(
        MutableColumns & columns,
        const ColumnNumbers & column_indices,
        typename Map::const_iterator & it,
        const std::optional<size_t> & key_pos,
        size_t & rows_added)
    {
        for (auto ref_it = it->getMapped().begin(); ref_it.ok(); ++ref_it)
        {
            for (size_t j = 0; j < columns.size(); ++j)
                if (j == key_pos)
                    columns[j]->insertData(rawData(it->getKey()), rawSize(it->getKey()));
                else
                    columns[j]->insertFrom(*ref_it->block->getByPosition(column_indices[j]).column.get(), ref_it->row_num);
            ++rows_added;
        }
    }
};

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
            join->addJoinedBlock(final_block, true);
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
