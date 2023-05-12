#pragma once
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace local_engine
{
using namespace DB;
std::shared_ptr<DB::StorageInMemoryMetadata> buildMetaData(DB::NamesAndTypesList columns, ContextPtr context);

std::unique_ptr<MergeTreeSettings> buildMergeTreeSettings();

std::unique_ptr<SelectQueryInfo> buildQueryInfo(NamesAndTypesList & names_and_types_list);

struct MergeTreeTable
{
    std::string database;
    std::string table;
    std::string relative_path;
    int min_block;
    int max_block;

    std::string toString() const;
};

MergeTreeTable parseMergeTreeTableString(const std::string & info);

}
