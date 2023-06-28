#include "SerializedPlanParser.h"
#include <memory>
#include <string_view>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Builder/BroadCastJoinBuilder.h>
#include <Columns/ColumnSet.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsVisitor.h>
#include <Interpreters/CollectJoinOnKeysVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryPriorities.h>
#include <Operator/BlocksBufferPoolTransform.h>
#include <Operator/PartitionColumnFillingTransform.h>
#include <Parser/FunctionParser.h>
#include <Parser/RelParser.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Formats/Impl/ArrowBlockOutputFormat.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTreeFactory.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <base/Decimal.h>
#include <base/types.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/wrappers.pb.h>
#include <sys/select.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/CHUtil.h>
#include <Common/DebugUtils.h>
#include <Common/Exception.h>
#include <Common/JoinHelper.h>
#include <Common/MergeTreeTool.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int BAD_ARGUMENTS;
    extern const int NO_SUCH_DATA_PART;
    extern const int UNKNOWN_FUNCTION;
    extern const int CANNOT_PARSE_PROTOBUF_SCHEMA;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INVALID_JOIN_ON_EXPRESSION;
}
}

namespace local_engine
{
using namespace DB;

std::string join(const ActionsDAG::NodeRawConstPtrs & v, char c)
{
    std::string res;
    for (size_t i = 0; i < v.size(); ++i)
    {
        if (i)
            res += c;
        res += v[i]->result_name;
    }
    return res;
}

bool isTypeMatched(const substrait::Type & substrait_type, const DataTypePtr & ch_type)
{
    const auto parsed_ch_type = SerializedPlanParser::parseType(substrait_type);
    return parsed_ch_type->equals(*ch_type);
}

void SerializedPlanParser::parseExtensions(
    const ::google::protobuf::RepeatedPtrField<substrait::extensions::SimpleExtensionDeclaration> & extensions)
{
    for (const auto & extension : extensions)
    {
        if (extension.has_extension_function())
        {
            function_mapping.emplace(
                std::to_string(extension.extension_function().function_anchor()), extension.extension_function().name());
        }
    }
}

std::shared_ptr<DB::ActionsDAG> SerializedPlanParser::expressionsToActionsDAG(
    const std::vector<substrait::Expression> & expressions, const DB::Block & header, const DB::Block & read_schema)
{
    auto actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(header));
    NamesWithAliases required_columns;
    std::set<String> distinct_columns;

    for (const auto & expr : expressions)
    {
        if (expr.has_selection())
        {
            auto position = expr.selection().direct_reference().struct_field().field();
            auto col_name = read_schema.getByPosition(position).name;
            const ActionsDAG::Node * field = actions_dag->tryFindInOutputs(col_name);
            if (distinct_columns.contains(field->result_name))
            {
                auto unique_name = getUniqueName(field->result_name);
                required_columns.emplace_back(NameWithAlias(field->result_name, unique_name));
                distinct_columns.emplace(unique_name);
            }
            else
            {
                required_columns.emplace_back(NameWithAlias(field->result_name, field->result_name));
                distinct_columns.emplace(field->result_name);
            }
        }
        else if (expr.has_scalar_function())
        {
            const auto & scalar_function = expr.scalar_function();
            auto function_signature = function_mapping.at(std::to_string(scalar_function.function_reference()));

            std::vector<String> result_names;
            std::vector<String> useless;
            if (startsWith(function_signature, "explode:"))
                actions_dag = parseArrayJoin(header, expr, result_names, useless, actions_dag, true, false);
            else if (startsWith(function_signature, "posexplode:"))
                actions_dag = parseArrayJoin(header, expr, result_names, useless, actions_dag, true, true);
            else if (startsWith(function_signature, "json_tuple:"))
                actions_dag = parseJsonTuple(header, expr, result_names, actions_dag, true, false);
            else
            {
                result_names.resize(1);
                actions_dag = parseFunction(header, expr, result_names[0], useless, actions_dag, true);
            }
            for (const auto & result_name : result_names)
            {
                if (result_name.empty())
                    continue;

                if (distinct_columns.contains(result_name))
                {
                    auto unique_name = getUniqueName(result_name);
                    required_columns.emplace_back(NameWithAlias(result_name, unique_name));
                    distinct_columns.emplace(unique_name);
                }
                else
                {
                    required_columns.emplace_back(NameWithAlias(result_name, result_name));
                    distinct_columns.emplace(result_name);
                }
            }
        }
        else if (expr.has_cast() || expr.has_if_then() || expr.has_literal())
        {
            const auto * node = parseExpression(actions_dag, expr);
            actions_dag->addOrReplaceInOutputs(*node);
            if (distinct_columns.contains(node->result_name))
            {
                auto unique_name = getUniqueName(node->result_name);
                required_columns.emplace_back(NameWithAlias(node->result_name, unique_name));
                distinct_columns.emplace(unique_name);
            }
            else
            {
                required_columns.emplace_back(NameWithAlias(node->result_name, node->result_name));
                distinct_columns.emplace(node->result_name);
            }
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "unsupported projection type {}.", magic_enum::enum_name(expr.rex_type_case()));
    }

    actions_dag->project(required_columns);
    return actions_dag;
}

std::string getDecimalFunction(const substrait::Type_Decimal & decimal, bool null_on_overflow)
{
    std::string ch_function_name;
    UInt32 precision = decimal.precision();
    UInt32 scale = decimal.scale();

    if (precision <= DataTypeDecimal32::maxPrecision())
        ch_function_name = "toDecimal32";
    else if (precision <= DataTypeDecimal64::maxPrecision())
        ch_function_name = "toDecimal64";
    else if (precision <= DataTypeDecimal128::maxPrecision())
        ch_function_name = "toDecimal128";
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);

    if (null_on_overflow)
        ch_function_name = ch_function_name + "OrNull";
    return ch_function_name;
}

bool SerializedPlanParser::isReadRelFromJava(const substrait::ReadRel & rel)
{
    assert(rel.has_local_files());
    assert(rel.has_base_schema());
    return rel.local_files().items().size() == 1 && rel.local_files().items().at(0).uri_file().starts_with("iterator");
}

QueryPlanStepPtr SerializedPlanParser::parseReadRealWithLocalFile(const substrait::ReadRel & rel)
{
    assert(rel.has_local_files());
    assert(rel.has_base_schema());
    auto header = parseNameStruct(rel.base_schema());
    auto source = std::make_shared<SubstraitFileSource>(context, header, rel.local_files());
    auto source_pipe = Pipe(source);
    auto source_step = std::make_unique<ReadFromStorageStep>(std::move(source_pipe), "substrait local files", nullptr);
    source_step->setStepDescription("read local files");
    return source_step;
}

QueryPlanStepPtr SerializedPlanParser::parseReadRealWithJavaIter(const substrait::ReadRel & rel)
{
    assert(rel.has_local_files());
    assert(rel.local_files().items().size() == 1);
    assert(rel.has_base_schema());
    auto iter = rel.local_files().items().at(0).uri_file();
    auto pos = iter.find(':');
    auto iter_index = std::stoi(iter.substr(pos + 1, iter.size()));

    auto source = std::make_shared<SourceFromJavaIter>(parseNameStruct(rel.base_schema()), input_iters[iter_index]);
    QueryPlanStepPtr source_step = std::make_unique<ReadFromPreparedSource>(Pipe(source));
    source_step->setStepDescription("Read From Java Iter");
    return source_step;
}

IQueryPlanStep * SerializedPlanParser::addRemoveNullableStep(QueryPlan & plan, std::vector<String> columns)
{
    if (columns.empty())
        return nullptr;
    auto remove_nullable_actions_dag
        = std::make_shared<ActionsDAG>(blockToNameAndTypeList(plan.getCurrentDataStream().header));
    removeNullable(columns, remove_nullable_actions_dag);
    auto expression_step = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), remove_nullable_actions_dag);
    expression_step->setStepDescription("Remove nullable properties");
    auto * step_ptr = expression_step.get();
    plan.addStep(std::move(expression_step));
    return step_ptr;
}

DB::QueryPlanPtr SerializedPlanParser::parseMergeTreeTable(const substrait::ReadRel & rel, std::vector<IQueryPlanStep *>& steps)
{
    assert(rel.has_extension_table());
    google::protobuf::StringValue table;
    table.ParseFromString(rel.extension_table().detail().value());
    auto merge_tree_table = local_engine::parseMergeTreeTableString(table.value());
    DB::Block header;
    if (rel.has_base_schema() && rel.base_schema().names_size())
    {
        header = parseNameStruct(rel.base_schema());
    }
    else
    {
        // For count(*) case, there will be an empty base_schema, so we try to read at least once column
        auto all_parts_dir = MergeTreeUtil::getAllMergeTreeParts(std::filesystem::path("/") / merge_tree_table.relative_path);
        if (all_parts_dir.empty())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Empty mergetree directory: {}", merge_tree_table.relative_path);
        }
        auto part_names_types_list = MergeTreeUtil::getSchemaFromMergeTreePart(all_parts_dir[0]);
        NamesAndTypesList one_column_name_type;
        one_column_name_type.push_back(part_names_types_list.front());
        header = BlockUtil::buildHeader(one_column_name_type);
        LOG_DEBUG(&Poco::Logger::get("SerializedPlanParser"), "Try to read ({}) instead of empty header", header.dumpNames());
    }
    auto names_and_types_list = header.getNamesAndTypesList();
    auto storage_factory = StorageMergeTreeFactory::instance();
    auto metadata = buildMetaData(names_and_types_list, context);
    query_context.metadata = metadata;
    auto storage = storage_factory.getStorage(
        StorageID(merge_tree_table.database, merge_tree_table.table),
        metadata->getColumns(),
        [merge_tree_table, metadata]() -> CustomStorageMergeTreePtr
        {
            auto custom_storage_merge_tree = std::make_shared<CustomStorageMergeTree>(
                StorageID(merge_tree_table.database, merge_tree_table.table),
                merge_tree_table.relative_path,
                *metadata,
                false,
                global_context,
                "",
                MergeTreeData::MergingParams(),
                buildMergeTreeSettings());
            custom_storage_merge_tree->loadDataParts(false);
            return custom_storage_merge_tree;
        });
    query_context.storage_snapshot = std::make_shared<StorageSnapshot>(*storage, metadata);
    query_context.custom_storage_merge_tree = storage;
    auto query_info = buildQueryInfo(names_and_types_list);
    std::vector<String> not_null_columns;
    if (rel.has_filter())
    {
        query_info->prewhere_info = parsePreWhereInfo(rel.filter(), header, not_null_columns);
    }
    auto data_parts = query_context.custom_storage_merge_tree->getAllDataPartsVector();
    int min_block = merge_tree_table.min_block;
    int max_block = merge_tree_table.max_block;
    MergeTreeData::DataPartsVector selected_parts;
    std::copy_if(
        std::begin(data_parts),
        std::end(data_parts),
        std::inserter(selected_parts, std::begin(selected_parts)),
        [min_block, max_block](MergeTreeData::DataPartPtr part)
        { return part->info.min_block >= min_block && part->info.max_block < max_block; });
    if (selected_parts.empty())
    {
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "part {} to {} not found.", min_block, max_block);
    }
    auto read_step = query_context.custom_storage_merge_tree->reader.readFromParts(
        selected_parts,
        /* alter_conversions = */ {},
        names_and_types_list.getNames(),
        query_context.storage_snapshot,
        *query_info,
        context,
        4096 * 2,
        1);
    QueryPlanPtr query = std::make_unique<QueryPlan>();
    steps.emplace_back(read_step.get());
    query->addStep(std::move(read_step));
    if (!not_null_columns.empty())
    {
        auto input_header = query->getCurrentDataStream().header;
        std::erase_if(not_null_columns, [input_header](auto item) -> bool { return !input_header.has(item); });
        auto* remove_null_step = addRemoveNullableStep(*query, not_null_columns);
        if (remove_null_step)
        {
            steps.emplace_back(remove_null_step);
        }
    }
    return query;
}

PrewhereInfoPtr
SerializedPlanParser::parsePreWhereInfo(const substrait::Expression & rel, Block & input, std::vector<String> & not_nullable_columns)
{
    auto prewhere_info = std::make_shared<PrewhereInfo>();
    prewhere_info->prewhere_actions = std::make_shared<ActionsDAG>(input.getNamesAndTypesList());
    std::string filter_name;
    // for in function
    if (rel.has_singular_or_list())
    {
        const auto * in_node = parseExpression(prewhere_info->prewhere_actions, rel);
        prewhere_info->prewhere_actions->addOrReplaceInOutputs(*in_node);
        filter_name = in_node->result_name;
    }
    else
    {
        parseFunctionWithDAG(rel, filter_name, not_nullable_columns, prewhere_info->prewhere_actions, true);
    }
    prewhere_info->prewhere_column_name = filter_name;
    prewhere_info->need_filter = true;
    prewhere_info->remove_prewhere_column = true;
    auto cols = prewhere_info->prewhere_actions->getRequiredColumnsNames();
    if (last_project)
    {
        prewhere_info->prewhere_actions->removeUnusedActions(Names{filter_name}, true, true);
        prewhere_info->prewhere_actions->projectInput(false);
        for (const auto & expr : last_project->expressions())
        {
            if (expr.has_selection())
            {
                auto position = expr.selection().direct_reference().struct_field().field();
                auto name = input.getByPosition(position).name;
                prewhere_info->prewhere_actions->tryRestoreColumn(name);
            }
        }
    }
    else
    {
        prewhere_info->prewhere_actions->removeUnusedActions(Names{filter_name}, false, true);
        prewhere_info->prewhere_actions->projectInput(false);
        for (const auto & name : input.getNames())
        {
            prewhere_info->prewhere_actions->tryRestoreColumn(name);
        }
    }
    return prewhere_info;
}

Block SerializedPlanParser::parseNameStruct(const substrait::NamedStruct & struct_)
{
    ColumnsWithTypeAndName internal_cols;
    internal_cols.reserve(struct_.names_size());
    std::list<std::string> field_names;
    for (int i = 0; i < struct_.names_size(); ++i)
    {
        field_names.emplace_back(struct_.names(i));
    }

    for (int i = 0; i < struct_.struct_().types_size(); ++i)
    {
        auto name = field_names.front();
        const auto & type = struct_.struct_().types(i);
        auto data_type = parseType(type, &field_names);
        // This is a partial aggregate data column.
        // It's type is special, must be a struct type contains all arguments types.
        Poco::StringTokenizer name_parts(name, "#");
        if (name_parts.count() >= 4)
        {
            auto nested_data_type = DB::removeNullable(data_type);
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(nested_data_type.get());
            if (!tuple_type)
            {
                throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Tuple is expected, but got {}", data_type->getName());
            }
            auto args_types = tuple_type->getElements();

            auto agg_function_name = getFunctionName(name_parts[3], {});
            AggregateFunctionProperties properties;
            auto tmp = AggregateFunctionFactory::instance().get(agg_function_name, args_types, {}, properties);
            data_type = tmp->getStateType();
        }
        internal_cols.push_back(ColumnWithTypeAndName(data_type, name));
    }
    Block res(std::move(internal_cols));
    return std::move(res);
}

DataTypePtr wrapNullableType(substrait::Type_Nullability nullable, DataTypePtr nested_type)
{
    return wrapNullableType(nullable == substrait::Type_Nullability_NULLABILITY_NULLABLE, nested_type);
}

DataTypePtr wrapNullableType(bool nullable, DataTypePtr nested_type)
{
    if (nullable && !nested_type->isNullable())
        return std::make_shared<DataTypeNullable>(nested_type);
    else
        return nested_type;
}

/**
 * names is used to name struct type fields.
 *
 */
DataTypePtr SerializedPlanParser::parseType(const substrait::Type & substrait_type, std::list<std::string> * names)
{
    DataTypePtr ch_type;
    std::string_view current_name;
    if (names)
    {
        current_name = names->front();
        names->pop_front();
    }

    if (substrait_type.has_bool_())
    {
        ch_type = DataTypeFactory::instance().get("Bool");
        ch_type = wrapNullableType(substrait_type.bool_().nullability(), ch_type);
    }
    else if (substrait_type.has_i8())
    {
        ch_type = std::make_shared<DataTypeInt8>();
        ch_type = wrapNullableType(substrait_type.i8().nullability(), ch_type);
    }
    else if (substrait_type.has_i16())
    {
        ch_type = std::make_shared<DataTypeInt16>();
        ch_type = wrapNullableType(substrait_type.i16().nullability(), ch_type);
    }
    else if (substrait_type.has_i32())
    {
        ch_type = std::make_shared<DataTypeInt32>();
        ch_type = wrapNullableType(substrait_type.i32().nullability(), ch_type);
    }
    else if (substrait_type.has_i64())
    {
        ch_type = std::make_shared<DataTypeInt64>();
        ch_type = wrapNullableType(substrait_type.i64().nullability(), ch_type);
    }
    else if (substrait_type.has_string())
    {
        ch_type = std::make_shared<DataTypeString>();
        ch_type = wrapNullableType(substrait_type.string().nullability(), ch_type);
    }
    else if (substrait_type.has_binary())
    {
        ch_type = std::make_shared<DataTypeString>();
        ch_type = wrapNullableType(substrait_type.binary().nullability(), ch_type);
    }
    else if (substrait_type.has_fixed_char())
    {
        const auto & fixed_char = substrait_type.fixed_char();
        ch_type = std::make_shared<DataTypeFixedString>(fixed_char.length());
        ch_type = wrapNullableType(fixed_char.nullability(), ch_type);
    }
    else if (substrait_type.has_fixed_binary())
    {
        const auto & fixed_binary = substrait_type.fixed_binary();
        ch_type = std::make_shared<DataTypeFixedString>(fixed_binary.length());
        ch_type = wrapNullableType(fixed_binary.nullability(), ch_type);
    }
    else if (substrait_type.has_fp32())
    {
        ch_type = std::make_shared<DataTypeFloat32>();
        ch_type = wrapNullableType(substrait_type.fp32().nullability(), ch_type);
    }
    else if (substrait_type.has_fp64())
    {
        ch_type = std::make_shared<DataTypeFloat64>();
        ch_type = wrapNullableType(substrait_type.fp64().nullability(), ch_type);
    }
    else if (substrait_type.has_timestamp())
    {
        ch_type = std::make_shared<DataTypeDateTime64>(6);
        ch_type = wrapNullableType(substrait_type.timestamp().nullability(), ch_type);
    }
    else if (substrait_type.has_date())
    {
        ch_type = std::make_shared<DataTypeDate32>();
        ch_type = wrapNullableType(substrait_type.date().nullability(), ch_type);
    }
    else if (substrait_type.has_decimal())
    {
        UInt32 precision = substrait_type.decimal().precision();
        UInt32 scale = substrait_type.decimal().scale();
        if (precision > DataTypeDecimal128::maxPrecision())
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);
        ch_type = createDecimal<DataTypeDecimal>(precision, scale);
        ch_type = wrapNullableType(substrait_type.decimal().nullability(), ch_type);
    }
    else if (substrait_type.has_struct_())
    {
        DataTypes ch_field_types(substrait_type.struct_().types().size());
        Strings field_names;
        for (size_t i = 0; i < ch_field_types.size(); ++i)
        {
            if (names)
                field_names.push_back(names->front());

            ch_field_types[i] = std::move(parseType(substrait_type.struct_().types()[i], names));
        }
        if (!field_names.empty())
            ch_type = std::make_shared<DataTypeTuple>(ch_field_types, field_names);
        else
            ch_type = std::make_shared<DataTypeTuple>(ch_field_types);
        ch_type = wrapNullableType(substrait_type.struct_().nullability(), ch_type);
    }
    else if (substrait_type.has_list())
    {
        auto ch_nested_type = parseType(substrait_type.list().type());
        ch_type = std::make_shared<DataTypeArray>(ch_nested_type);
        ch_type = wrapNullableType(substrait_type.list().nullability(), ch_type);
    }
    else if (substrait_type.has_map())
    {
        auto ch_key_type = parseType(substrait_type.map().key());
        auto ch_val_type = parseType(substrait_type.map().value());
        ch_type = std::make_shared<DataTypeMap>(ch_key_type, ch_val_type);
        ch_type = wrapNullableType(substrait_type.map().nullability(), ch_type);
    }
    else if (substrait_type.has_nothing())
    {
        ch_type = std::make_shared<DataTypeNothing>();
        ch_type = wrapNullableType(true, ch_type);
    }
    else
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support type {}", substrait_type.DebugString());

    /// TODO(taiyang-li): consider Time/IntervalYear/IntervalDay/TimestampTZ/UUID/VarChar/FixedBinary/UserDefined
    return std::move(ch_type);
}

DB::DataTypePtr SerializedPlanParser::parseType(const std::string & type)
{
    static std::map<std::string, std::string> type2type
        = {{"BooleanType", "UInt8"},
           {"ByteType", "Int8"},
           {"ShortType", "Int16"},
           {"IntegerType", "Int32"},
           {"LongType", "Int64"},
           {"FloatType", "Float32"},
           {"DoubleType", "Float64"},
           {"StringType", "String"},
           {"DateType", "Date"}};

    auto it = type2type.find(type);
    if (it == type2type.end())
    {
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_TYPE, "Unknow spark type: {}", type);
    }
    return DB::DataTypeFactory::instance().get(it->second);
}

QueryPlanPtr SerializedPlanParser::parse(std::unique_ptr<substrait::Plan> plan)
{
    auto * logger = &Poco::Logger::get("SerializedPlanParser");
    if (logger->debug())
    {
        namespace pb_util = google::protobuf::util;
        pb_util::JsonOptions options;
        std::string json;
        pb_util::MessageToJsonString(*plan, &json, options);
        LOG_DEBUG(&Poco::Logger::get("SerializedPlanParser"), "substrait plan:\n{}", json);
    }
    parseExtensions(plan->extensions());
    if (plan->relations_size() == 1)
    {
        auto root_rel = plan->relations().at(0);
        if (!root_rel.has_root())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "must have root rel!");
        }
        std::list<const substrait::Rel *> rel_stack;
        auto query_plan = parseOp(root_rel.root().input(), rel_stack);
        if (root_rel.root().names_size())
        {
            ActionsDAGPtr actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(query_plan->getCurrentDataStream().header));
            NamesWithAliases aliases;
            auto cols = query_plan->getCurrentDataStream().header.getNamesAndTypesList();
            for (size_t i = 0; i < cols.getNames().size(); i++)
            {
                aliases.emplace_back(NameWithAlias(cols.getNames()[i], root_rel.root().names(i)));
            }
            actions_dag->project(aliases);
            auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), actions_dag);
            expression_step->setStepDescription("Rename Output");
            query_plan->addStep(std::move(expression_step));
        }
        return query_plan;
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "too many relations found");
    }
}

QueryPlanPtr SerializedPlanParser::parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack)
{
    QueryPlanPtr query_plan;
    std::vector<IQueryPlanStep *> steps;
    switch (rel.rel_type_case())
    {
        case substrait::Rel::RelTypeCase::kFetch: {
            rel_stack.push_back(&rel);
            const auto & limit = rel.fetch();
            query_plan = parseOp(limit.input(), rel_stack);
            rel_stack.pop_back();
            auto limit_step = std::make_unique<LimitStep>(query_plan->getCurrentDataStream(), limit.count(), limit.offset());
            limit_step->setStepDescription("LIMIT");
            steps.emplace_back(limit_step.get());
            query_plan->addStep(std::move(limit_step));
            break;
        }
        case substrait::Rel::RelTypeCase::kFilter: {
            rel_stack.push_back(&rel);
            const auto & filter = rel.filter();
            query_plan = parseOp(filter.input(), rel_stack);
            rel_stack.pop_back();
            std::string filter_name;
            std::vector<String> required_columns;

            ActionsDAGPtr actions_dag = nullptr;
            if (filter.condition().has_scalar_function())
            {
                actions_dag = parseFunction(
                    query_plan->getCurrentDataStream().header, filter.condition(), filter_name, required_columns, nullptr, true);
            }
            else
            {
                actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(query_plan->getCurrentDataStream().header));
                const auto * node = parseExpression(actions_dag, filter.condition());
                filter_name = node->result_name;
            }

            auto input = query_plan->getCurrentDataStream().header.getNames();
            Names input_with_condition(input);
            input_with_condition.emplace_back(filter_name);
            actions_dag->removeUnusedActions(input_with_condition);
            auto filter_step = std::make_unique<FilterStep>(query_plan->getCurrentDataStream(), actions_dag, filter_name, true);
            filter_step->setStepDescription("WHERE");
            steps.emplace_back(filter_step.get());
            query_plan->addStep(std::move(filter_step));
            // remove nullable
            auto * remove_null_step = addRemoveNullableStep(*query_plan, required_columns);
            if (remove_null_step)
            {
                steps.emplace_back(remove_null_step);
            }
            break;
        }
        case substrait::Rel::RelTypeCase::kGenerate:
        case substrait::Rel::RelTypeCase::kProject: {
            const substrait::Rel * input = nullptr;
            bool is_generate = false;
            std::vector<substrait::Expression> expressions;

            if (rel.has_project())
            {
                const auto & project = rel.project();
                last_project = &project;
                input = &project.input();

                expressions.reserve(project.expressions_size());
                for (int i = 0; i < project.expressions_size(); ++i)
                    expressions.emplace_back(project.expressions(i));
            }
            else
            {
                const auto & generate = rel.generate();
                input = &generate.input();
                is_generate = true;

                expressions.reserve(generate.child_output_size() + 1);
                for (int i = 0; i < generate.child_output_size(); ++i)
                    expressions.push_back(generate.child_output(i));
                expressions.emplace_back(generate.generator());
            }
            rel_stack.push_back(&rel);
            query_plan = parseOp(*input, rel_stack);
            rel_stack.pop_back();
            // for prewhere
            Block read_schema;
            bool is_mergetree_input = input->has_read() && !input->read().has_local_files();
            if (is_mergetree_input)
                read_schema = parseNameStruct(input->read().base_schema());
            else
                read_schema = query_plan->getCurrentDataStream().header;

            auto actions_dag = expressionsToActionsDAG(expressions, query_plan->getCurrentDataStream().header, read_schema);
            auto expression_step = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), actions_dag);
            expression_step->setStepDescription(is_generate ? "Generate" : "Project");
            steps.emplace_back(expression_step.get());
            query_plan->addStep(std::move(expression_step));
            break;
        }
        case substrait::Rel::RelTypeCase::kAggregate: {
            rel_stack.push_back(&rel);
            const auto & aggregate = rel.aggregate();
            query_plan = parseOp(aggregate.input(), rel_stack);
            rel_stack.pop_back();

            auto agg_parser = RelParserFactory::instance().getBuilder(substrait::Rel::RelTypeCase::kAggregate)(this);
            query_plan = agg_parser->parse(std::move(query_plan), rel, rel_stack);
            steps.insert(steps.end(), agg_parser->getSteps().begin(), agg_parser->getSteps().end());
            break;
        }
        case substrait::Rel::RelTypeCase::kRead: {
            const auto & read = rel.read();
            assert(read.has_local_files() || read.has_extension_table() && "Only support local parquet files or merge tree read rel");
            if (read.has_local_files())
            {
                query_plan = std::make_unique<QueryPlan>();
                QueryPlanStepPtr step;
                if (isReadRelFromJava(read))
                {
                    step = parseReadRealWithJavaIter(read);
                }
                else
                {
                    step = parseReadRealWithLocalFile(read);
                }
                steps.emplace_back(step.get());
                query_plan->addStep(std::move(step));

                // Add a buffer after source, it try to preload data from source and reduce the
                // waiting time of downstream nodes.
                if (context->getSettingsRef().max_threads > 1)
                {
                    auto buffer_step = std::make_unique<BlocksBufferPoolStep>(query_plan->getCurrentDataStream());
                    steps.emplace_back(buffer_step.get());
                    query_plan->addStep(std::move(buffer_step));
                }
            }
            else
            {
                query_plan = parseMergeTreeTable(read, steps);
            }
            last_project = nullptr;
            break;
        }
        case substrait::Rel::RelTypeCase::kJoin: {
            const auto & join = rel.join();
            if (!join.has_left() || !join.has_right())
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "left table or right table is missing.");
            }
            last_project = nullptr;
            rel_stack.push_back(&rel);
            auto left_plan = parseOp(join.left(), rel_stack);
            last_project = nullptr;
            auto right_plan = parseOp(join.right(), rel_stack);
            rel_stack.pop_back();

            query_plan = parseJoin(join, std::move(left_plan), std::move(right_plan), steps);
            break;
        }
        case substrait::Rel::RelTypeCase::kSort: {
            rel_stack.push_back(&rel);
            const auto & sort_rel = rel.sort();
            query_plan = parseOp(sort_rel.input(), rel_stack);
            rel_stack.pop_back();
            auto sort_parser = RelParserFactory::instance().getBuilder(substrait::Rel::RelTypeCase::kSort)(this);
            query_plan = sort_parser->parse(std::move(query_plan), rel, rel_stack);
            auto parser_steps = sort_parser->getSteps();
            steps.insert(steps.end(), parser_steps.begin(), parser_steps.end());
            break;
        }
        case substrait::Rel::RelTypeCase::kWindow: {
            rel_stack.push_back(&rel);
            const auto win_rel = rel.window();
            query_plan = parseOp(win_rel.input(), rel_stack);
            rel_stack.pop_back();
            auto win_parser = RelParserFactory::instance().getBuilder(substrait::Rel::RelTypeCase::kWindow)(this);
            query_plan = win_parser->parse(std::move(query_plan), rel, rel_stack);
            auto parser_steps = win_parser->getSteps();
            steps.insert(steps.end(), parser_steps.begin(), parser_steps.end());
            break;
        }
        case substrait::Rel::RelTypeCase::kExpand: {
            rel_stack.push_back(&rel);
            const auto & expand_rel = rel.expand();
            query_plan = parseOp(expand_rel.input(), rel_stack);
            rel_stack.pop_back();
            auto expand_parser = RelParserFactory::instance().getBuilder(substrait::Rel::RelTypeCase::kExpand)(this);
            query_plan = expand_parser->parse(std::move(query_plan), rel, rel_stack);
            auto parser_steps = expand_parser->getSteps();
            steps.insert(steps.end(), parser_steps.begin(), parser_steps.end());
            break;
        }
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "doesn't support relation type: {}.\n{}", rel.rel_type_case(), rel.DebugString());
    }

    if (rel.rel_type_case() == substrait::Rel::RelTypeCase::kRead)
    {
        size_t id = metrics.empty() ? 0 : metrics.back()->getId() + 1;
        metrics.emplace_back(std::make_shared<RelMetric>(id, String(magic_enum::enum_name(rel.rel_type_case())), steps));
    }
    else
    {
        metrics = {std::make_shared<RelMetric>(String(magic_enum::enum_name(rel.rel_type_case())), metrics, steps)};
    }
    return std::move(query_plan);
}

AggregateFunctionPtr getAggregateFunction(const std::string & name, DataTypes arg_types)
{
    auto & factory = AggregateFunctionFactory::instance();
    AggregateFunctionProperties properties;
    return factory.get(name, arg_types, Array{}, properties);
}


NamesAndTypesList SerializedPlanParser::blockToNameAndTypeList(const Block & header)
{
    NamesAndTypesList types;
    for (const auto & name : header.getNames())
    {
        const auto * column = header.findByName(name);
        types.push_back(NameAndTypePair(column->name, column->type));
    }
    return types;
}

std::string
SerializedPlanParser::getFunctionName(const std::string & function_signature, const substrait::Expression_ScalarFunction & function)
{
    const auto & output_type = function.output_type();
    auto args = function.arguments();
    auto pos = function_signature.find(':');
    auto function_name = function_signature.substr(0, pos);
    if (!SCALAR_FUNCTIONS.contains(function_name))
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Unsupported function {}", function_name);

    std::string ch_function_name;
    if (function_name == "trim")
        ch_function_name = args.size() == 1 ? "trimBoth" : "trimBothSpark";
    else if (function_name == "ltrim")
        ch_function_name = args.size() == 1 ? "trimLeft" : "trimLeftSpark";
    else if (function_name == "rtrim")
        ch_function_name = args.size() == 1 ? "trimRight" : "trimRightSpark";
    else if (function_name == "extract")
    {
        if (args.size() != 2)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Spark function extract requires two args, function:{}", function.ShortDebugString());

        // Get the first arg: field
        const auto & extract_field = args.at(0);

        if (extract_field.value().has_literal())
        {
            const auto & field_value = extract_field.value().literal().string();
            if (field_value == "YEAR")
                ch_function_name = "toYear"; // spark: extract(YEAR FROM) or year
            else if (field_value == "YEAR_OF_WEEK")
                ch_function_name = "toISOYear"; // spark: extract(YEAROFWEEK FROM)
            else if (field_value == "QUARTER")
                ch_function_name = "toQuarter"; // spark: extract(QUARTER FROM) or quarter
            else if (field_value == "MONTH")
                ch_function_name = "toMonth"; // spark: extract(MONTH FROM) or month
            else if (field_value == "WEEK_OF_YEAR")
                ch_function_name = "toISOWeek"; // spark: extract(WEEK FROM) or weekofyear
            else if (field_value == "WEEK_DAY")
                /// Spark WeekDay(date) (0 = Monday, 1 = Tuesday, ..., 6 = Sunday)
                /// Substrait: extract(WEEK_DAY from date)
                /// CH: toDayOfWeek(date, 1)
                ch_function_name = "toDayOfWeek";
            else if (field_value == "DAY_OF_WEEK")
                /// Spark: DayOfWeek(date) (1 = Sunday, 2 = Monday, ..., 7 = Saturday)
                /// Substrait: extract(DAY_OF_WEEK from date)
                /// CH: toDayOfWeek(date, 3)
                /// DAYOFWEEK is alias of function toDayOfWeek.
                /// This trick is to distinguish between extract fields DAY_OF_WEEK and WEEK_DAY in latter codes
                ch_function_name = "DAYOFWEEK";
            else if (field_value == "DAY")
                ch_function_name = "toDayOfMonth"; // spark: extract(DAY FROM) or dayofmonth
            else if (field_value == "DAY_OF_YEAR")
                ch_function_name = "toDayOfYear"; // spark: extract(DOY FROM) or dayofyear
            else if (field_value == "HOUR")
                ch_function_name = "toHour"; // spark: extract(HOUR FROM) or hour
            else if (field_value == "MINUTE")
                ch_function_name = "toMinute"; // spark: extract(MINUTE FROM) or minute
            else if (field_value == "SECOND")
                ch_function_name = "toSecond"; // spark: extract(SECOND FROM) or secondwithfraction
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first arg of spark extract function is wrong.");
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first arg of spark extract function is wrong.");
    }
    else if (function_name == "trunc")
    {
        if (args.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Spark function trunc requires two args, function:{}", function.ShortDebugString());

        const auto & trunc_field = args.at(0);
        if (!trunc_field.value().has_literal() || !trunc_field.value().literal().has_string())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second arg of spark trunc function is wrong.");

        const auto & field_value = trunc_field.value().literal().string();
        if (field_value == "YEAR" || field_value == "YYYY" || field_value == "YY")
            ch_function_name = "toStartOfYear";
        else if (field_value == "QUARTER")
            ch_function_name = "toStartOfQuarter";
        else if (field_value == "MONTH" || field_value == "MM" || field_value == "MON")
            ch_function_name = "toStartOfMonth";
        else if (field_value == "WEEK")
            ch_function_name = "toStartOfWeek";
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second arg of spark trunc function is wrong, value:{}", field_value);
    }
    else if (function_name == "sha2")
    {
        if (args.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Spark function sha2 requires two args, function:{}", function.ShortDebugString());

        const auto & bit_length = args.at(1);
        if (!bit_length.value().has_literal() || !bit_length.value().literal().has_i32())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second arg of spark sha2 function is wrong.");

        const auto & bit_length_value = bit_length.value().literal().i32();
        if (bit_length_value == 224)
            ch_function_name = "SHA224";
        else if (bit_length_value == 256 || bit_length_value == 0)
            ch_function_name = "SHA256";
        else if (bit_length_value == 384)
            ch_function_name = "SHA384";
        else if (bit_length_value == 512)
            ch_function_name = "SHA512";
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second arg of spark sha2 function is wrong, value:{}", bit_length_value);
    }
    else if (function_name == "check_overflow")
    {
        if (args.size() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "check_overflow function requires at least two args.");
        ch_function_name = SCALAR_FUNCTIONS.at(function_name);
        auto null_on_overflow = args.at(1).value().literal().boolean();
        if (null_on_overflow)
            ch_function_name = ch_function_name + "OrNull";
    }
    else if (function_name == "make_decimal")
    {
        if (args.size() < 3)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "make_decimal function requires at least 3 args.");
        ch_function_name = SCALAR_FUNCTIONS.at(function_name);
        auto null_on_overflow = args.at(2).value().literal().boolean();
        if (null_on_overflow)
            ch_function_name = ch_function_name + "OrNull";
    }
    else if (function_name == "char_length")
    {
        /// In Spark
        /// char_length returns the number of bytes when input is binary type, corresponding to CH length function
        /// char_length returns the number of characters when input is string type, corresponding to CH char_length function
        ch_function_name = SCALAR_FUNCTIONS.at(function_name);
        if (function_signature.find("vbin") != std::string::npos)
            ch_function_name = "length";
    }
    else
        ch_function_name = SCALAR_FUNCTIONS.at(function_name);

    return ch_function_name;
}

ActionsDAG::NodeRawConstPtrs SerializedPlanParser::parseArrayJoinWithDAG(
    const substrait::Expression & rel,
    std::vector<String> & result_names,
    std::vector<String> & required_columns,
    DB::ActionsDAGPtr actions_dag,
    bool keep_result,
    bool position)
{
    if (!rel.has_scalar_function())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The root of expression should be a scalar function:\n {}", rel.DebugString());

    const auto & scalar_function = rel.scalar_function();

    auto function_signature = function_mapping.at(std::to_string(rel.scalar_function().function_reference()));
    auto function_name = getFunctionName(function_signature, scalar_function);
    if (function_name != "arrayJoin")
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Function parseArrayJoinWithDAG should only process arrayJoin function, but input is {}",
            rel.ShortDebugString());

    /// The argument number of arrayJoin(converted from Spark explode/posexplode) should be 1
    if (scalar_function.arguments_size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument number of arrayJoin should be 1 but is {}", scalar_function.arguments_size());

    ActionsDAG::NodeRawConstPtrs args;
    parseFunctionArguments(actions_dag, args, required_columns, function_name, scalar_function);

    /// Remove Nullable from Nullable(Array(xx)) or Nullable(Map(xx, xx)) if needed
    const auto * arg_not_null = args[0];
    if (arg_not_null->result_type->isNullable())
    {
        auto assume_not_null_builder = FunctionFactory::instance().get("assumeNotNull", context);
        arg_not_null = &actions_dag->addFunction(assume_not_null_builder, {args[0]}, "assumeNotNull(" + args[0]->result_name + ")");
    }

    /// arrayJoin(arg_not_null)
    auto array_join_name = "arrayJoin(" + arg_not_null->result_name + ")";
    const auto * array_join_node = &actions_dag->addArrayJoin(*arg_not_null, array_join_name);

    auto arg_type = arg_not_null->result_type;
    WhichDataType which(arg_type.get());
    auto tuple_element_builder = FunctionFactory::instance().get("tupleElement", context);
    auto tuple_index_type = std::make_shared<DataTypeUInt32>();

    auto add_tuple_element = [&](const ActionsDAG::Node * tuple_node, size_t i) -> const ActionsDAG::Node *
    {
        ColumnWithTypeAndName index_col(tuple_index_type->createColumnConst(1, i), tuple_index_type, getUniqueName(std::to_string(i)));
        const auto * index_node = &actions_dag->addColumn(std::move(index_col));
        auto result_name = "tupleElement(" + tuple_node->result_name + ", " + index_node->result_name + ")";
        return &actions_dag->addFunction(tuple_element_builder, {tuple_node, index_node}, result_name);
    };

    /// Special process to keep compatiable with Spark
    if (!position)
    {
        /// Spark: explode(array_or_map) -> CH: arrayJoin(array_or_map)
        if (which.isMap())
        {
            /// In Spark: explode(map(k, v)) output 2 columns with default names "key" and "value"
            /// In CH: arrayJoin(map(k, v)) output 1 column with Tuple Type.
            /// So we must wrap arrayJoin with tupleElement function for compatiability.

            /// arrayJoin(arg_not_null).1
            const auto * key_node = add_tuple_element(array_join_node, 1);

            /// arrayJoin(arg_not_null).2
            const auto * val_node = add_tuple_element(array_join_node, 2);

            result_names.push_back(key_node->result_name);
            result_names.push_back(val_node->result_name);
            if (keep_result)
            {
                actions_dag->addOrReplaceInOutputs(*key_node);
                actions_dag->addOrReplaceInOutputs(*val_node);
            }
            return {key_node, val_node};
        }
        else if (which.isArray())
        {
            result_names.push_back(array_join_name);
            if (keep_result)
                actions_dag->addOrReplaceInOutputs(*array_join_node);
            return {array_join_node};
        }
        else
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Argument type of arrayJoin converted from explode should be Array or Map but is {}",
                arg_type->getName());
    }
    else
    {
        /// Spark: posexplode(array_or_map) -> CH: arrayJoin(map), in which map = mapFromArrays(range(length(array_or_map)), array_or_map)
        if (which.isMap())
        {
            /// In Spark: posexplode(array_of_map) output 2 or 3 columns: (pos, col) or (pos, key, value)
            /// In CH: arrayJoin(map(k, v)) output 1 column with Tuple Type.
            /// So we must wrap arrayJoin with tupleElement function for compatiability.

            /// pos = arrayJoin(arg_not_null).1
            const auto * pos_node = add_tuple_element(array_join_node, 1);

            /// col = arrayJoin(arg_not_null).2 or (key, value) = arrayJoin(arg_not_null).2
            const auto * item_node = add_tuple_element(array_join_node, 2);

            /// Get type of y from node: cast(mapFromArrays(x, y), 'Map(K, V)')
            DataTypePtr raw_child_type;
            if (args[0]->type == ActionsDAG::ActionType::FUNCTION && args[0]->function_base->getName() == "mapFromArrays")
            {
                /// Get Type of y from node: mapFromArrays(x, y)
                raw_child_type = DB::removeNullable(args[0]->children[1]->result_type);
            }
            else if (
                args[0]->type == ActionsDAG::ActionType::FUNCTION && args[0]->function_base->getName() == "_CAST"
                && args[0]->children[0]->type == ActionsDAG::ActionType::FUNCTION
                && args[0]->children[0]->function_base->getName() == "mapFromArrays")
            {
                /// Get Type of y from node: cast(mapFromArrays(x, y), 'Map(K, V)')
                raw_child_type = DB::removeNullable(args[0]->children[0]->children[1]->result_type);
            }
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid argument type of arrayJoin: {}", actions_dag->dumpDAG());


            if (isMap(raw_child_type))
            {
                /// key = arrayJoin(arg_not_null).2.1
                const auto * item_key_node = add_tuple_element(item_node, 1);

                /// value = arrayJoin(arg_not_null).2.2
                const auto * item_value_node = add_tuple_element(item_node, 2);

                result_names.push_back(pos_node->result_name);
                result_names.push_back(item_key_node->result_name);
                result_names.push_back(item_value_node->result_name);
                if (keep_result)
                {
                    actions_dag->addOrReplaceInOutputs(*pos_node);
                    actions_dag->addOrReplaceInOutputs(*item_key_node);
                    actions_dag->addOrReplaceInOutputs(*item_value_node);
                }

                return {pos_node, item_key_node, item_value_node};
            }
            else if (isArray(raw_child_type))
            {
                /// col = arrayJoin(arg_not_null).2
                result_names.push_back(pos_node->result_name);
                result_names.push_back(item_node->result_name);
                if (keep_result)
                {
                    actions_dag->addOrReplaceInOutputs(*pos_node);
                    actions_dag->addOrReplaceInOutputs(*item_node);
                }
                return {pos_node, item_node};
            }
            else
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The raw input of arrayJoin converted from posexplode should be Array or Map type but is {}",
                    raw_child_type->getName());
        }
        else
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Argument type of arrayJoin converted from posexplode should be Map but is {}",
                arg_type->getName());
    }
}

const ActionsDAG::Node * SerializedPlanParser::parseFunctionWithDAG(
    const substrait::Expression & rel,
    std::string & result_name,
    std::vector<String> & required_columns,
    DB::ActionsDAGPtr actions_dag,
    bool keep_result)
{
    if (!rel.has_scalar_function())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "the root of expression should be a scalar function:\n {}", rel.DebugString());

    const auto & scalar_function = rel.scalar_function();
    auto function_signature = function_mapping.at(std::to_string(scalar_function.function_reference()));

    /// If the substrait function name is registered in FunctionParserFactory, use it to parse the function, and return result directly
    auto pos = function_signature.find(':');
    auto func_name = function_signature.substr(0, pos);

    auto func_parser = FunctionParserFactory::instance().tryGet(func_name, this);
    if (func_parser)
    {
        const auto * result_node = func_parser->parse(scalar_function, actions_dag, required_columns);
        if (keep_result)
            actions_dag->addOrReplaceInOutputs(*result_node);

        result_name = result_node->result_name;
        return result_node;
    }

    auto ch_func_name = getFunctionName(function_signature, scalar_function);
    ActionsDAG::NodeRawConstPtrs args;
    parseFunctionArguments(actions_dag, args, required_columns, ch_func_name, scalar_function);

    /// If the first argument of function formatDateTimeInJodaSyntax is integer, replace formatDateTimeInJodaSyntax with fromUnixTimestampInJodaSyntax
    /// to avoid exception
    if (ch_func_name == "formatDateTimeInJodaSyntax")
    {
        if (args.size() > 1 && isInteger(DB::removeNullable(args[0]->result_type)))
            ch_func_name = "fromUnixTimestampInJodaSyntax";
    }

    const ActionsDAG::Node * result_node;
    if (ch_func_name == "alias")
    {
        result_name = args[0]->result_name;
        actions_dag->addOrReplaceInOutputs(*args[0]);
        result_node = &actions_dag->addAlias(actions_dag->findInOutputs(result_name), result_name);
    }
    else
    {
        if (ch_func_name == "isNotNull")
        {
            required_columns.emplace_back(args[0]->result_name);
        }
        else if (ch_func_name == "splitByRegexp")
        {
            if (args.size() >= 2)
            {
                /// In Spark: split(str, regex [, limit] )
                /// In CH: splitByRegexp(regexp, str [, limit])
                std::swap(args[0], args[1]);
            }
        }

        if (function_signature.find("check_overflow:", 0) != function_signature.npos)
        {
            if (scalar_function.arguments().size() < 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "check_overflow function requires at least two args.");

            ActionsDAG::NodeRawConstPtrs new_args;
            new_args.reserve(3);
            new_args.emplace_back(args[0]);

            UInt32 precision = rel.scalar_function().output_type().decimal().precision();
            UInt32 scale = rel.scalar_function().output_type().decimal().scale();
            auto uint32_type = std::make_shared<DataTypeUInt32>();
            new_args.emplace_back(&actions_dag->addColumn(
                ColumnWithTypeAndName(uint32_type->createColumnConst(1, precision), uint32_type, getUniqueName(toString(precision)))));
            new_args.emplace_back(&actions_dag->addColumn(
                ColumnWithTypeAndName(uint32_type->createColumnConst(1, scale), uint32_type, getUniqueName(toString(scale)))));
            args = std::move(new_args);
        }
        else if (startsWith(function_signature, "make_decimal:"))
        {
            if (scalar_function.arguments().size() < 3)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "make_decimal function requires at least three args.");

            ActionsDAG::NodeRawConstPtrs new_args;
            new_args.reserve(3);
            new_args.emplace_back(args[0]);

            UInt32 precision = rel.scalar_function().output_type().decimal().precision();
            UInt32 scale = rel.scalar_function().output_type().decimal().scale();
            auto uint32_type = std::make_shared<DataTypeUInt32>();
            new_args.emplace_back(&actions_dag->addColumn(
                ColumnWithTypeAndName(uint32_type->createColumnConst(1, precision), uint32_type, getUniqueName(toString(precision)))));
            new_args.emplace_back(&actions_dag->addColumn(
                ColumnWithTypeAndName(uint32_type->createColumnConst(1, scale), uint32_type, getUniqueName(toString(scale)))));
            args = std::move(new_args);
        }

        auto function_builder = FunctionFactory::instance().get(ch_func_name, context);
        std::string args_name = join(args, ',');
        result_name = ch_func_name + "(" + args_name + ")";
        const auto * function_node = &actions_dag->addFunction(function_builder, args, result_name);
        result_node = function_node;

        if (!isTypeMatched(rel.scalar_function().output_type(), function_node->result_type))
        {
            result_node = ActionsDAGUtil::convertNodeType(
                actions_dag,
                function_node,
                SerializedPlanParser::parseType(rel.scalar_function().output_type())->getName(),
                function_node->result_name);
        }

        if (ch_func_name == "JSON_VALUE")
            result_node->function->setResolver(function_builder);

        if (keep_result)
            actions_dag->addOrReplaceInOutputs(*result_node);
    }
    return result_node;
}

void SerializedPlanParser::parseFunctionArguments(
    DB::ActionsDAGPtr & actions_dag,
    ActionsDAG::NodeRawConstPtrs & parsed_args,
    std::vector<String> & required_columns,
    std::string & function_name,
    const substrait::Expression_ScalarFunction & scalar_function)
{
    auto add_column = [&](const DataTypePtr & type, const Field & field) -> auto
    {
        return &actions_dag->addColumn(ColumnWithTypeAndName(type->createColumnConst(1, field), type, getUniqueName(toString(field))));
    };

    auto function_signature = function_mapping.at(std::to_string(scalar_function.function_reference()));
    const auto & args = scalar_function.arguments();
    parsed_args.reserve(args.size());

    // Some functions need to be handled specially.
    if (function_name == "JSONExtract")
    {
        parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[0]);
        auto data_type = parseType(scalar_function.output_type());
        parsed_args.emplace_back(add_column(std::make_shared<DB::DataTypeString>(), data_type->getName()));
    }
    else if (function_name == "tupleElement")
    {
        // tupleElement. the field index must be unsigned integer in CH, cast the signed integer in substrait
        // which must be a positive value into unsigned integer here.
        parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[0]);

        // tuple indecies start from 1, in spark, start from 0
        if (!args[1].value().has_literal())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "get_struct_field's second argument must be a literal");
        }
        auto [data_type, field] = parseLiteral(args[1].value().literal());
        if (data_type->getTypeId() != DB::TypeIndex::Int32)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "get_struct_field's second argument must be i32");
        }
        UInt32 field_index = field.get<Int32>() + 1;
        const auto * index_node = add_column(std::make_shared<DB::DataTypeUInt32>(), field_index);
        parsed_args.emplace_back(index_node);
    }
    else if (function_name == "tuple")
    {
        // Arguments in the format, (<field name>, <value expression>[, <field name>, <value expression> ...])
        // We don't need to care the field names here.
        for (int index = 1; index < args.size(); index += 2)
            parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[index]);
    }
    else if (function_name == "has")
    {
        // since FunctionArrayIndex::useDefaultImplementationForNulls = false, we need to unwrap the
        // nullable
        const ActionsDAG::Node * arg_node = parseFunctionArgument(actions_dag, required_columns, function_name, args[0]);
        if (arg_node->result_type->isNullable())
        {
            auto nested_type = typeid_cast<const DB::DataTypeNullable *>(arg_node->result_type.get())->getNestedType();
            arg_node = ActionsDAGUtil::convertNodeType(actions_dag, arg_node, nested_type->getName());
        }
        parsed_args.emplace_back(arg_node);
        parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[1]);
    }
    else if (function_name == "arrayElement")
    {
        // arrayElement. in spark, the array element index must a be positive value. But in CH, a array element index
        // could be positive or negative and have different effects. So we make a cast here.
        // In clickhosue, map element are also accessed by arrayElement, not make the cast.
        parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[0]);
        auto element_type = parsed_args.back()->result_type;
        const auto * first_arg_type = element_type.get();
        if (first_arg_type->isNullable())
        {
            first_arg_type = typeid_cast<const DB::DataTypeNullable *>(first_arg_type)->getNestedType().get();
        }
        const auto * index_node = parseFunctionArgument(actions_dag, required_columns, function_name, args[1]);
        WhichDataType which(first_arg_type);
        if (which.isArray())
        {
            DB::DataTypeNullable target_type(std::make_shared<DB::DataTypeUInt32>());
            index_node = ActionsDAGUtil::convertNodeType(actions_dag, index_node, target_type.getName());
            parsed_args.emplace_back(index_node);
        }
        else
            parsed_args.push_back(index_node);
    }
    else if (function_name == "repeat")
    {
        // repeat. the field index must be unsigned integer in CH, cast the signed integer in substrait
        // which must be a positive value into unsigned integer here.
        parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[0]);
        const DB::ActionsDAG::Node * repeat_times_node = parseFunctionArgument(actions_dag, required_columns, function_name, args[1]);
        DB::DataTypeNullable target_type(std::make_shared<DB::DataTypeUInt32>());
        repeat_times_node = ActionsDAGUtil::convertNodeType(actions_dag, repeat_times_node, target_type.getName());
        parsed_args.emplace_back(repeat_times_node);
    }
    else if (function_name == "isNaN")
    {
        // the result of isNaN(NULL) is NULL in CH, but false in Spark
        const DB::ActionsDAG::Node * arg_node = nullptr;
        if (args[0].value().has_cast())
        {
            arg_node = parseExpression(actions_dag, args[0].value().cast().input());
            const auto * res_type = arg_node->result_type.get();
            if (res_type->isNullable())
            {
                res_type = typeid_cast<const DB::DataTypeNullable *>(res_type)->getNestedType().get();
            }
            if (isString(*res_type))
            {
                DB::ActionsDAG::NodeRawConstPtrs cast_func_args = {arg_node};
                arg_node = toFunctionNode(actions_dag, "toFloat64OrZero", cast_func_args);
            }
            else
            {
                arg_node = parseFunctionArgument(actions_dag, required_columns, function_name, args[0]);
            }
        }
        else
        {
            arg_node = parseFunctionArgument(actions_dag, required_columns, function_name, args[0]);
        }

        DB::ActionsDAG::NodeRawConstPtrs ifnull_func_args = {arg_node, add_column(std::make_shared<DataTypeInt32>(), 0)};
        parsed_args.emplace_back(toFunctionNode(actions_dag, "IfNull", ifnull_func_args));
    }
    else if (function_name == "positionUTF8Spark")
    {
        if (args.size() >= 2)
        {
            // In Spark: position(substr, str, Int32)
            // In CH:    position(str, subtr, UInt32)
            parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[1]);
            parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[0]);
        }
        if (args.size() >= 3)
        {
            // add cast: cast(start_pos as UInt32)
            const auto * start_pos_node = parseFunctionArgument(actions_dag, required_columns, function_name, args[2]);
            DB::DataTypeNullable target_type(std::make_shared<DB::DataTypeUInt32>());
            start_pos_node = ActionsDAGUtil::convertNodeType(actions_dag, start_pos_node, target_type.getName());
            parsed_args.emplace_back(start_pos_node);
        }
    }
    else if (function_name == "space")
    {
        // convert space function to repeat
        const DB::ActionsDAG::Node * repeat_times_node = parseFunctionArgument(actions_dag, required_columns, "repeat", args[0]);
        const DB::ActionsDAG::Node * space_str_node = add_column(std::make_shared<DataTypeString>(), " ");
        function_name = "repeat";
        parsed_args.emplace_back(space_str_node);
        parsed_args.emplace_back(repeat_times_node);
    }
    else if (function_name == "mapFromArrays")
    {
        /// Remove nullable for first arg
        parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[0]);
        const auto * first_arg = parsed_args.back();
        auto assume_not_null_builder = FunctionFactory::instance().get("assumeNotNull", context);
        const auto * first_arg_not_null
            = &actions_dag->addFunction(assume_not_null_builder, {first_arg}, "assumeNotNull(" + first_arg->result_name + ")");
        parsed_args.back() = first_arg_not_null;
        parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[1]);
        auto second_arg = parsed_args.back();
        const auto * second_arg_not_null
            = &actions_dag->addFunction(assume_not_null_builder, {second_arg}, "assumeNotNull(" + second_arg->result_name + ")");
        parsed_args.back() = second_arg_not_null;
    }
    else if (function_name == "trimBothSpark" || function_name == "trimLeftSpark" || function_name == "trimRightSpark")
    {
        /// In substrait, the first arg is srcStr, the second arg is trimStr
        /// But in CH, the first arg is trimStr, the second arg is srcStr
        parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[1]);
        parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[0]);
    }
    else if (startsWith(function_signature, "extract:"))
    {
        /// Skip the first arg of extract in substrait
        for (int i = 1; i < args.size(); i++)
            parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[i]);

        /// Append extra mode argument for extract(WEEK_DAY from date) or extract(DAY_OF_WEEK from date) in substrait
        if (function_name == "toDayOfWeek" || function_name == "DAYOFWEEK")
        {
            UInt8 mode = function_name == "toDayOfWeek" ? 1 : 3;
            auto mode_type = std::make_shared<DataTypeUInt8>();
            ColumnWithTypeAndName mode_col(mode_type->createColumnConst(1, mode), mode_type, getUniqueName(std::to_string(mode)));
            const auto & mode_node = actions_dag->addColumn(std::move(mode_col));
            parsed_args.emplace_back(&mode_node);
        }
    }
    else if (startsWith(function_signature, "trunc:") || startsWith(function_signature, "sha2:"))
    {
        /// Skip the last arg of trunc in substrait
        for (int i = 0; i < args.size() - 1; i++)
            parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, args[i]);
    }
    else
    {
        // Default handle
        for (const auto & arg : args)
            parseFunctionArgument(actions_dag, parsed_args, required_columns, function_name, arg);
    }
}

void SerializedPlanParser::parseFunctionArgument(
    DB::ActionsDAGPtr & actions_dag,
    ActionsDAG::NodeRawConstPtrs & parsed_args,
    std::vector<String> & required_columns,
    const std::string & function_name,
    const substrait::FunctionArgument & arg)
{
    parsed_args.emplace_back(parseFunctionArgument(actions_dag, required_columns, function_name, arg));
}

const DB::ActionsDAG::Node * SerializedPlanParser::parseFunctionArgument(
    DB::ActionsDAGPtr & actions_dag,
    std::vector<String> & required_columns,
    const std::string & function_name,
    const substrait::FunctionArgument & arg)
{
    const DB::ActionsDAG::Node * res;
    if (arg.value().has_scalar_function())
    {
        std::string arg_name;
        bool keep_arg = FUNCTION_NEED_KEEP_ARGUMENTS.contains(function_name);
        parseFunctionWithDAG(arg.value(), arg_name, required_columns, actions_dag, keep_arg);
        res = &actions_dag->getNodes().back();
    }
    else
    {
        res = parseExpression(actions_dag, arg.value());
    }
    return res;
}

// Convert signed integer index into unsigned integer index
std::pair<DB::DataTypePtr, DB::Field> SerializedPlanParser::convertStructFieldType(const DB::DataTypePtr & type, const DB::Field & field)
{
// For tupelElement, field index starts from 1, but int substrait plan, it starts from 0.
#define UINT_CONVERT(type_ptr, field, type_name) \
    if ((type_ptr)->getTypeId() == DB::TypeIndex::type_name) \
    { \
        return {std::make_shared<DB::DataTypeU##type_name>(), static_cast<U##type_name>((field).get<type_name>()) + 1}; \
    }

    auto type_id = type->getTypeId();
    if (type_id == DB::TypeIndex::UInt8 || type_id == DB::TypeIndex::UInt16 || type_id == DB::TypeIndex::UInt32
        || type_id == DB::TypeIndex::UInt64)
    {
        return {type, field};
    }
    UINT_CONVERT(type, field, Int8)
    UINT_CONVERT(type, field, Int16)
    UINT_CONVERT(type, field, Int32)
    UINT_CONVERT(type, field, Int64)
    throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Not valid interger type: {}", type->getName());
#undef UINT_CONVERT
}

ActionsDAGPtr SerializedPlanParser::parseFunction(
    const Block & header,
    const substrait::Expression & rel,
    std::string & result_name,
    std::vector<String> & required_columns,
    ActionsDAGPtr actions_dag,
    bool keep_result)
{
    if (!actions_dag)
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(header));

    parseFunctionWithDAG(rel, result_name, required_columns, actions_dag, keep_result);
    return actions_dag;
}

ActionsDAGPtr SerializedPlanParser::parseArrayJoin(
    const Block & input,
    const substrait::Expression & rel,
    std::vector<String> & result_names,
    std::vector<String> & required_columns,
    ActionsDAGPtr actions_dag,
    bool keep_result,
    bool position)
{
    if (!actions_dag)
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input));

    parseArrayJoinWithDAG(rel, result_names, required_columns, actions_dag, keep_result, position);
    return actions_dag;
}

ActionsDAGPtr SerializedPlanParser::parseJsonTuple(
    const Block & input,
    const substrait::Expression & rel,
    std::vector<String> & result_names,
    ActionsDAGPtr actions_dag,
    bool keep_result,
    bool position)
{
    if (!actions_dag)
    {
        actions_dag = std::make_shared<ActionsDAG>(blockToNameAndTypeList(input));
    }
    auto add_column = [&](const DataTypePtr & type, const Field & field) -> auto
    {
        return &actions_dag->addColumn(ColumnWithTypeAndName(type->createColumnConst(1, field), type, getUniqueName(toString(field))));
    };
    const auto & scalar_function = rel.scalar_function();
    auto function_signature = function_mapping.at(std::to_string(rel.scalar_function().function_reference()));
    auto function_name = getFunctionName(function_signature, scalar_function);
    auto args = scalar_function.arguments();
    if (args.size() < 2)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The function json_tuple should has at least 2 arguments.");
    }
    auto first_arg = args[0].value();
    const DB::ActionsDAG::Node * json_expr_node = parseExpression(actions_dag, first_arg);
    std::string extract_expr = "Tuple(";
    for (int i = 1; i < args.size(); i++)
    {
        auto arg_value = args[i].value();
        if (!arg_value.has_literal())
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "The arguments of function {} must be string literal", function_name);
        }
        DB::Field f = arg_value.literal().string();
        std::string s;
        if (f.tryGet(s))
        {
            extract_expr.append(s).append(" Nullable(String)");
            if (i != args.size() - 1)
            {
                extract_expr.append(",");
            }
        }
    }
    extract_expr.append(")");
    const DB::ActionsDAG::Node * extract_expr_node = add_column(std::make_shared<DataTypeString>(), extract_expr);
    auto json_extract_builder = FunctionFactory::instance().get("JSONExtract", context);
    auto json_extract_result_name = "JSONExtract(" + json_expr_node->result_name + "," + extract_expr_node->result_name + ")";
    const ActionsDAG::Node * json_extract_node = &actions_dag->addFunction(json_extract_builder, {json_expr_node, extract_expr_node}, json_extract_result_name);
    auto tuple_element_builder = FunctionFactory::instance().get("tupleElement", context);
    auto tuple_index_type = std::make_shared<DataTypeUInt32>();
    auto add_tuple_element = [&](const ActionsDAG::Node * tuple_node, size_t i) -> const ActionsDAG::Node *
    {
        ColumnWithTypeAndName index_col(tuple_index_type->createColumnConst(1, i), tuple_index_type, getUniqueName(std::to_string(i)));
        const auto * index_node = &actions_dag->addColumn(std::move(index_col));
        auto result_name = "tupleElement(" + tuple_node->result_name + ", " + index_node->result_name + ")";
        return &actions_dag->addFunction(tuple_element_builder, {tuple_node, index_node}, result_name);
    };
    for (int i = 1; i < args.size(); i ++)
    {
        const ActionsDAG::Node * tuple_node = add_tuple_element(json_extract_node, i);
        if (keep_result)
        {
            actions_dag->addOrReplaceInOutputs(*tuple_node);
            result_names.push_back(tuple_node->result_name);
        }
    }
    return actions_dag;
}

const ActionsDAG::Node *
SerializedPlanParser::toFunctionNode(ActionsDAGPtr actions_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args)
{
    auto function_builder = DB::FunctionFactory::instance().get(function, context);
    std::string args_name = join(args, ',');
    auto result_name = function + "(" + args_name + ")";
    const auto * function_node = &actions_dag->addFunction(function_builder, args, result_name);
    return function_node;
}

std::pair<DataTypePtr, Field> SerializedPlanParser::parseLiteral(const substrait::Expression_Literal & literal)
{
    DataTypePtr type;
    Field field;

    switch (literal.literal_type_case())
    {
        case substrait::Expression_Literal::kFp64: {
            type = std::make_shared<DataTypeFloat64>();
            field = literal.fp64();
            break;
        }
        case substrait::Expression_Literal::kFp32: {
            type = std::make_shared<DataTypeFloat32>();
            field = literal.fp32();
            break;
        }
        case substrait::Expression_Literal::kString: {
            type = std::make_shared<DataTypeString>();
            field = literal.string();
            break;
        }
        case substrait::Expression_Literal::kBinary: {
            type = std::make_shared<DataTypeString>();
            field = literal.binary();
            break;
        }
        case substrait::Expression_Literal::kI64: {
            type = std::make_shared<DataTypeInt64>();
            field = literal.i64();
            break;
        }
        case substrait::Expression_Literal::kI32: {
            type = std::make_shared<DataTypeInt32>();
            field = literal.i32();
            break;
        }
        case substrait::Expression_Literal::kBoolean: {
            type = std::make_shared<DataTypeUInt8>();
            field = literal.boolean() ? UInt8(1) : UInt8(0);
            break;
        }
        case substrait::Expression_Literal::kI16: {
            type = std::make_shared<DataTypeInt16>();
            field = literal.i16();
            break;
        }
        case substrait::Expression_Literal::kI8: {
            type = std::make_shared<DataTypeInt8>();
            field = literal.i8();
            break;
        }
        case substrait::Expression_Literal::kDate: {
            type = std::make_shared<DataTypeDate32>();
            field = literal.date();
            break;
        }
        case substrait::Expression_Literal::kTimestamp: {
            type = std::make_shared<DataTypeDateTime64>(6);
            field = DecimalField<DateTime64>(literal.timestamp(), 6);
            break;
        }
        case substrait::Expression_Literal::kDecimal: {
            UInt32 precision = literal.decimal().precision();
            UInt32 scale = literal.decimal().scale();
            const auto & bytes = literal.decimal().value();

            if (precision <= DataTypeDecimal32::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal32>(precision, scale);
                auto value = *reinterpret_cast<const Int32 *>(bytes.data());
                field = DecimalField<Decimal32>(value, scale);
            }
            else if (precision <= DataTypeDecimal64::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal64>(precision, scale);
                auto value = *reinterpret_cast<const Int64 *>(bytes.data());
                field = DecimalField<Decimal64>(value, scale);
            }
            else if (precision <= DataTypeDecimal128::maxPrecision())
            {
                type = std::make_shared<DataTypeDecimal128>(precision, scale);
                String bytes_copy(bytes);
                auto value = *reinterpret_cast<Decimal128 *>(bytes_copy.data());
                field = DecimalField<Decimal128>(value, scale);
            }
            else
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Spark doesn't support decimal type with precision {}", precision);
            break;
        }
        case substrait::Expression_Literal::kList: {
            const auto & values = literal.list().values();
            if (values.empty())
            {
                type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());
                field = Array();
                break;
            }

            DataTypePtr common_type;
            std::tie(common_type, std::ignore) = parseLiteral(values[0]);
            size_t list_len = values.size();
            Array array(list_len);
            for (size_t i = 0; i < list_len; ++i)
            {
                auto type_and_field = parseLiteral(values[i]);
                common_type = getLeastSupertype(DataTypes{common_type, type_and_field.first});
                array[i] = std::move(type_and_field.second);
            }

            type = std::make_shared<DataTypeArray>(common_type);
            field = std::move(array);
            break;
        }
        case substrait::Expression_Literal::kMap: {
            const auto & key_values = literal.map().key_values();
            if (key_values.empty())
            {
                type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeNothing>(), std::make_shared<DataTypeNothing>());
                field = Map();
                break;
            }

            const auto & first_key_value = key_values[0];

            DataTypePtr common_key_type;
            std::tie(common_key_type, std::ignore) = parseLiteral(first_key_value.key());

            DataTypePtr common_value_type;
            std::tie(common_value_type, std::ignore) = parseLiteral(first_key_value.value());

            Map map;
            map.reserve(key_values.size());
            for (const auto & key_value : key_values)
            {
                Tuple tuple(2);

                DataTypePtr key_type;
                std::tie(key_type, tuple[0]) = parseLiteral(key_value.key());
                /// Each key should has the same type
                if (!common_key_type->equals(*key_type))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Literal map key type mismatch:{} and {}",
                        common_key_type->getName(),
                        key_type->getName());

                DataTypePtr value_type;
                std::tie(value_type, tuple[1]) = parseLiteral(key_value.value());
                /// Each value should has least super type for all of them
                common_value_type = getLeastSupertype(DataTypes{common_value_type, value_type});

                map.emplace_back(std::move(tuple));
            }

            type = std::make_shared<DataTypeMap>(common_key_type, common_value_type);
            field = std::move(map);
            break;
        }
        case substrait::Expression_Literal::kStruct: {
            const auto & fields = literal.struct_().fields();

            DataTypes types;
            types.reserve(fields.size());
            Tuple tuple;
            tuple.reserve(fields.size());
            for (const auto & f : fields)
            {
                DataTypePtr field_type;
                Field field_value;
                std::tie(field_type, field_value) = parseLiteral(f);

                types.emplace_back(std::move(field_type));
                tuple.emplace_back(std::move(field_value));
            }

            type = std::make_shared<DataTypeTuple>(types);
            field = std::move(tuple);
            break;
        }
        case substrait::Expression_Literal::kNull: {
            type = parseType(literal.null());
            field = std::move(Field{});
            break;
        }
        default: {
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE, "Unsupported spark literal type {}", magic_enum::enum_name(literal.literal_type_case()));
        }
    }
    return std::make_pair(std::move(type), std::move(field));
}

const ActionsDAG::Node * SerializedPlanParser::parseExpression(ActionsDAGPtr actions_dag, const substrait::Expression & rel)
{
    auto add_column = [&](const DataTypePtr & type, const Field & field) -> auto
    {
        return &actions_dag->addColumn(ColumnWithTypeAndName(type->createColumnConst(1, field), type, getUniqueName(toString(field))));
    };

    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            DataTypePtr type;
            Field field;
            std::tie(type, field) = parseLiteral(rel.literal());
            return add_column(type, field);
        }

        case substrait::Expression::RexTypeCase::kSelection: {
            if (!rel.selection().has_direct_reference() || !rel.selection().direct_reference().has_struct_field())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can only have direct struct references in selections");

            const auto * field = actions_dag->getInputs()[rel.selection().direct_reference().struct_field().field()];
            return actions_dag->tryFindInOutputs(field->result_name);
        }

        case substrait::Expression::RexTypeCase::kCast: {
            if (!rel.cast().has_type() || !rel.cast().has_input())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Doesn't have type or input in cast node.");

            DB::ActionsDAG::NodeRawConstPtrs args;

            const auto & input = rel.cast().input();
            args.emplace_back(parseExpression(actions_dag, input));

            const auto & substrait_type = rel.cast().type();
            const ActionsDAG::Node * function_node = nullptr;
            /// Spark cast(x as BINARY) -> CH reinterpretAsStringSpark(x)
            if (substrait_type.has_binary())
                function_node = toFunctionNode(actions_dag, "reinterpretAsStringSpark", args);
            else
            {
                DataTypePtr ch_type = parseType(substrait_type);
                args.emplace_back(add_column(std::make_shared<DataTypeString>(), ch_type->getName()));

                function_node = toFunctionNode(actions_dag, "CAST", args);
            }

            actions_dag->addOrReplaceInOutputs(*function_node);
            return function_node;
        }

        case substrait::Expression::RexTypeCase::kIfThen: {
            const auto & if_then = rel.if_then();
            auto function_multi_if = DB::FunctionFactory::instance().get("multiIf", context);
            DB::ActionsDAG::NodeRawConstPtrs args;

            auto condition_nums = if_then.ifs_size();
            for (int i = 0; i < condition_nums; ++i)
            {
                const auto & ifs = if_then.ifs(i);
                const auto * if_node = parseExpression(actions_dag, ifs.if_());
                args.emplace_back(if_node);

                const auto * then_node = parseExpression(actions_dag, ifs.then());
                args.emplace_back(then_node);
            }

            const auto * else_node = parseExpression(actions_dag, if_then.else_());
            args.emplace_back(else_node);
            std::string args_name = join(args, ',');
            auto result_name = "multiIf(" + args_name + ")";
            const auto * function_node = &actions_dag->addFunction(function_multi_if, args, result_name);
            actions_dag->addOrReplaceInOutputs(*function_node);
            return function_node;
        }

        case substrait::Expression::RexTypeCase::kScalarFunction: {
            std::string result;
            std::vector<String> useless;
            return parseFunctionWithDAG(rel, result, useless, actions_dag, false);
        }

        case substrait::Expression::RexTypeCase::kSingularOrList: {
            const auto & options = rel.singular_or_list().options();
            /// options is empty always return false
            if (options.empty())
                return add_column(std::make_shared<DataTypeUInt8>(), 0);
            /// options should be literals
            if (!options[0].has_literal())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Options of SingularOrList must have literal type");

            DB::ActionsDAG::NodeRawConstPtrs args;
            args.emplace_back(parseExpression(actions_dag, rel.singular_or_list().value()));

            bool nullable = false;
            size_t options_len = options.size();
            for (size_t i = 0; i < options_len; ++i)
            {
                if (!options[i].has_literal())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "in expression values must be the literal!");
                if (!nullable)
                    nullable = options[i].literal().has_null();
            }

            DataTypePtr elem_type;
            std::tie(elem_type, std::ignore) = parseLiteral(options[0].literal());
            elem_type = wrapNullableType(nullable, elem_type);

            MutableColumnPtr elem_column = elem_type->createColumn();
            elem_column->reserve(options_len);
            for (size_t i = 0; i < options_len; ++i)
            {
                auto type_and_field = std::move(parseLiteral(options[i].literal()));
                auto option_type = wrapNullableType(nullable, type_and_field.first);
                if (!elem_type->equals(*option_type))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "SingularOrList options type mismatch:{} and {}",
                        elem_type->getName(),
                        option_type->getName());

                elem_column->insert(type_and_field.second);
            }

            MutableColumns elem_columns;
            elem_columns.emplace_back(std::move(elem_column));

            auto name = getUniqueName("__set");
            Block elem_block;
            elem_block.insert(ColumnWithTypeAndName(nullptr, elem_type, name));
            elem_block.setColumns(std::move(elem_columns));

            SizeLimits limit;
            auto elem_set = std::make_shared<Set>(limit, true, false);
            elem_set->setHeader(elem_block.getColumnsWithTypeAndName());
            elem_set->insertFromBlock(elem_block.getColumnsWithTypeAndName());
            elem_set->finishInsert();

            auto future_set = std::make_shared<FutureSetFromStorage>(std::move(elem_set));
            auto arg = ColumnSet::create(1, std::move(future_set));
            args.emplace_back(&actions_dag->addColumn(ColumnWithTypeAndName(std::move(arg), std::make_shared<DataTypeSet>(), name)));

            const auto * function_node = toFunctionNode(actions_dag, "in", args);
            actions_dag->addOrReplaceInOutputs(*function_node);
            if (nullable)
            {
                /// if sets has `null` and value not in sets
                /// In Spark: return `null`, is the standard behaviour from ANSI.(SPARK-37920)
                /// In CH: return `false`
                /// So we used if(a, b, c) cast `false` to `null` if sets has `null`
                auto type = wrapNullableType(true, function_node->result_type);
                DB::ActionsDAG::NodeRawConstPtrs cast_args({function_node, add_column(type, true), add_column(type, Field())});
                auto cast = FunctionFactory::instance().get("if", context);
                function_node = toFunctionNode(actions_dag, "if", cast_args);
            }
            return function_node;
        }

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported spark expression type {} : {}",
                magic_enum::enum_name(rel.rex_type_case()),
                rel.DebugString());
    }
}

QueryPlanPtr SerializedPlanParser::parse(const std::string & plan)
{
    auto plan_ptr = std::make_unique<substrait::Plan>();
    /// https://stackoverflow.com/questions/52028583/getting-error-parsing-protobuf-data
    /// Parsing may fail when the number of recursive layers is large.
    /// Here, set a limit large enough to avoid this problem.
    /// Once this problem occurs, it is difficult to troubleshoot, because the pb of c++ will not provide any valid information
    google::protobuf::io::CodedInputStream coded_in(reinterpret_cast<const uint8_t *>(plan.data()), plan.size());
    coded_in.SetRecursionLimit(100000);

    auto ok = plan_ptr->ParseFromCodedStream(&coded_in);
    if (!ok)
        throw Exception(ErrorCodes::CANNOT_PARSE_PROTOBUF_SCHEMA, "Parse substrait::Plan from string failed");

    auto res = std::move(parse(std::move(plan_ptr)));

    auto * logger = &Poco::Logger::get("SerializedPlanParser");
    if (logger->debug())
    {
        auto out = PlanUtil::explainPlan(*res);
        LOG_DEBUG(logger, "clickhouse plan:\n{}", out);
    }
    return std::move(res);
}

QueryPlanPtr SerializedPlanParser::parseJson(const std::string & json_plan)
{
    auto plan_ptr = std::make_unique<substrait::Plan>();
    google::protobuf::util::JsonStringToMessage(google::protobuf::stringpiece_internal::StringPiece(json_plan.c_str()), plan_ptr.get());
    return parse(std::move(plan_ptr));
}

SerializedPlanParser::SerializedPlanParser(const ContextPtr & context_) : context(context_)
{
}

ContextMutablePtr SerializedPlanParser::global_context = nullptr;

Context::ConfigurationPtr SerializedPlanParser::config = nullptr;

void SerializedPlanParser::collectJoinKeys(
    const substrait::Expression & condition, std::vector<std::pair<int32_t, int32_t>> & join_keys, int32_t right_key_start)
{
    auto condition_name = getFunctionName(
        function_mapping.at(std::to_string(condition.scalar_function().function_reference())), condition.scalar_function());
    if (condition_name == "and")
    {
        collectJoinKeys(condition.scalar_function().arguments(0).value(), join_keys, right_key_start);
        collectJoinKeys(condition.scalar_function().arguments(1).value(), join_keys, right_key_start);
    }
    else if (condition_name == "equals")
    {
        const auto & function = condition.scalar_function();
        auto left_key_idx = function.arguments(0).value().selection().direct_reference().struct_field().field();
        auto right_key_idx = function.arguments(1).value().selection().direct_reference().struct_field().field() - right_key_start;
        join_keys.emplace_back(std::pair(left_key_idx, right_key_idx));
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "doesn't support condition {}", condition_name);
    }
}

DB::QueryPlanPtr SerializedPlanParser::parseJoin(substrait::JoinRel join, DB::QueryPlanPtr left, DB::QueryPlanPtr right, std::vector<IQueryPlanStep *>& steps)
{
    google::protobuf::StringValue optimization;
    optimization.ParseFromString(join.advanced_extension().optimization().value());
    auto join_opt_info = parseJoinOptimizationInfo(optimization.value());
    auto table_join = std::make_shared<TableJoin>(global_context->getSettings(), global_context->getGlobalTemporaryVolume());
    if (join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_INNER)
    {
        table_join->setKind(DB::JoinKind::Inner);
        table_join->setStrictness(DB::JoinStrictness::All);
    }
    else if (join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI)
    {
        table_join->setKind(DB::JoinKind::Left);
        table_join->setStrictness(DB::JoinStrictness::Semi);
    }
    else if (join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_ANTI)
    {
        table_join->setKind(DB::JoinKind::Left);
        table_join->setStrictness(DB::JoinStrictness::Anti);
    }
    else if (join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_LEFT)
    {
        table_join->setKind(DB::JoinKind::Left);
        table_join->setStrictness(DB::JoinStrictness::All);
    }
    else if (join.type() == substrait::JoinRel_JoinType_JOIN_TYPE_OUTER)
    {
        table_join->setKind(DB::JoinKind::Full);
        table_join->setStrictness(DB::JoinStrictness::All);
    }
    else
    {
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "unsupported join type {}.", magic_enum::enum_name(join.type()));
    }

    if (join_opt_info.is_broadcast)
    {
        auto storage_join = BroadCastJoinBuilder::getJoin(join_opt_info.storage_join_key);
        ActionsDAGPtr project = ActionsDAG::makeConvertingActions(
            right->getCurrentDataStream().header.getColumnsWithTypeAndName(),
            storage_join->getRightSampleBlock().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);

        if (project)
        {
            QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right->getCurrentDataStream(), project);
            project_step->setStepDescription("Rename Broadcast Table Name");
            steps.emplace_back(project_step.get());
            right->addStep(std::move(project_step));
        }
    }

    NameSet left_columns_set;
    for (const auto & col : left->getCurrentDataStream().header.getNames())
    {
        left_columns_set.emplace(col);
    }
    table_join->setColumnsFromJoinedTable(
        right->getCurrentDataStream().header.getNamesAndTypesList(), left_columns_set, getUniqueName("right") + ".");
    // fix right table key duplicate
    NamesWithAliases right_table_alias;
    for (size_t idx = 0; idx < table_join->columnsFromJoinedTable().size(); idx++)
    {
        auto origin_name = right->getCurrentDataStream().header.getByPosition(idx).name;
        auto dedup_name = table_join->columnsFromJoinedTable().getNames().at(idx);
        if (origin_name != dedup_name)
        {
            right_table_alias.emplace_back(NameWithAlias(origin_name, dedup_name));
        }
    }
    if (!right_table_alias.empty())
    {
        ActionsDAGPtr rename_dag = std::make_shared<ActionsDAG>(right->getCurrentDataStream().header.getNamesAndTypesList());
        auto original_right_columns = right->getCurrentDataStream().header;
        for (const auto & column_alias : right_table_alias)
        {
            if (original_right_columns.has(column_alias.first))
            {
                auto pos = original_right_columns.getPositionByName(column_alias.first);
                const auto & alias = rename_dag->addAlias(*rename_dag->getInputs()[pos], column_alias.second);
                rename_dag->getOutputs()[pos] = &alias;
            }
        }
        rename_dag->projectInput();
        QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(right->getCurrentDataStream(), rename_dag);
        project_step->setStepDescription("Right Table Rename");
        steps.emplace_back(project_step.get());
        right->addStep(std::move(project_step));
    }

    for (const auto & column : table_join->columnsFromJoinedTable())
    {
        table_join->addJoinedColumn(column);
    }
    ActionsDAGPtr left_convert_actions = nullptr;
    ActionsDAGPtr right_convert_actions = nullptr;
    std::tie(left_convert_actions, right_convert_actions) = table_join->createConvertingActions(
        left->getCurrentDataStream().header.getColumnsWithTypeAndName(), right->getCurrentDataStream().header.getColumnsWithTypeAndName());

    if (right_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(right->getCurrentDataStream(), right_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        steps.emplace_back(converting_step.get());
        right->addStep(std::move(converting_step));
    }

    if (left_convert_actions)
    {
        auto converting_step = std::make_unique<ExpressionStep>(left->getCurrentDataStream(), left_convert_actions);
        converting_step->setStepDescription("Convert joined columns");
        steps.emplace_back(converting_step.get());
        left->addStep(std::move(converting_step));
    }
    QueryPlanPtr query_plan;
    Names after_join_names;
    auto left_names = left->getCurrentDataStream().header.getNames();
    after_join_names.insert(after_join_names.end(), left_names.begin(), left_names.end());
    auto right_name = table_join->columnsFromJoinedTable().getNames();
    after_join_names.insert(after_join_names.end(), right_name.begin(), right_name.end());

    bool add_filter_step = false;
    try
    {
        parseJoinKeysAndCondition(table_join, join, left, right, table_join->columnsFromJoinedTable(), after_join_names, steps);
    }
    // if ch not support the join type or join conditions, it will throw an exception like 'not support'.
    catch (Poco::Exception & e)
    {
        // CH not support join condition has 'or' and has different table in each side.
        // But in inner join, we could execute join condition after join. so we have add filter step
        if (e.code() == ErrorCodes::INVALID_JOIN_ON_EXPRESSION && table_join->kind() == DB::JoinKind::Inner)
        {
            add_filter_step = true;
        }
        else
        {
            throw;
        }
    }

    if (join_opt_info.is_broadcast)
    {
        auto storage_join = BroadCastJoinBuilder::getJoin(join_opt_info.storage_join_key);
        auto hash_join = storage_join->getJoinLocked(table_join, context);
        QueryPlanStepPtr join_step = std::make_unique<FilledJoinStep>(left->getCurrentDataStream(), hash_join, 8192);

        join_step->setStepDescription("JOIN");
        steps.emplace_back(join_step.get());
        left->addStep(std::move(join_step));
        query_plan = std::move(left);
        /// hold right plan for profile
        extra_plan_holder.emplace_back(std::move(right));
    }
    else
    {
        auto hash_join = std::make_shared<HashJoin>(table_join, right->getCurrentDataStream().header.cloneEmpty());
        QueryPlanStepPtr join_step
            = std::make_unique<DB::JoinStep>(left->getCurrentDataStream(), right->getCurrentDataStream(), hash_join, 8192, 1, false);

        join_step->setStepDescription("JOIN");
        steps.emplace_back(join_step.get());
        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::move(left));
        plans.emplace_back(std::move(right));

        query_plan = std::make_unique<QueryPlan>();
        query_plan->unitePlans(std::move(join_step), {std::move(plans)});
    }

    reorderJoinOutput(*query_plan, after_join_names);
    if (add_filter_step)
    {
        std::string filter_name;
        std::vector<String> useless;
        auto actions_dag
            = parseFunction(query_plan->getCurrentDataStream().header, join.post_join_filter(), filter_name, useless, nullptr, true);
        auto filter_step = std::make_unique<FilterStep>(query_plan->getCurrentDataStream(), actions_dag, filter_name, true);
        filter_step->setStepDescription("Post Join Filter");
        steps.emplace_back(filter_step.get());
        query_plan->addStep(std::move(filter_step));
    }
    return query_plan;
}

void SerializedPlanParser::parseJoinKeysAndCondition(
    std::shared_ptr<TableJoin> table_join,
    substrait::JoinRel & join,
    DB::QueryPlanPtr & left,
    DB::QueryPlanPtr & right,
    const NamesAndTypesList & alias_right,
    Names & names,
    std::vector<IQueryPlanStep *>& steps)
{
    ASTs args;
    ASTParser astParser(context, function_mapping);

    if (join.has_expression())
    {
        args.emplace_back(astParser.parseToAST(names, join.expression()));
    }

    if (join.has_post_join_filter())
    {
        args.emplace_back(astParser.parseToAST(names, join.post_join_filter()));
    }

    if (args.empty())
        return;

    ASTPtr ast = args.size() == 1 ? args.back() : makeASTFunction("and", args);

    bool is_asof = (table_join->strictness() == JoinStrictness::Asof);

    Aliases aliases;
    DatabaseAndTableWithAlias left_table_name;
    DatabaseAndTableWithAlias right_table_name;
    TableWithColumnNamesAndTypes left_table(left_table_name, left->getCurrentDataStream().header.getNamesAndTypesList());
    TableWithColumnNamesAndTypes right_table(right_table_name, alias_right);

    CollectJoinOnKeysVisitor::Data data{*table_join, left_table, right_table, aliases, is_asof};
    if (auto * or_func = ast->as<ASTFunction>(); or_func && or_func->name == "or")
    {
        for (auto & disjunct : or_func->arguments->children)
        {
            table_join->addDisjunct();
            CollectJoinOnKeysVisitor(data).visit(disjunct);
        }
        assert(table_join->getClauses().size() == or_func->arguments->children.size());
    }
    else
    {
        table_join->addDisjunct();
        CollectJoinOnKeysVisitor(data).visit(ast);
        assert(table_join->oneDisjunct());
    }

    if (join.has_post_join_filter())
    {
        auto left_keys = table_join->leftKeysList();
        auto right_keys = table_join->rightKeysList();
        if (!left_keys->children.empty())
        {
            auto actions = astParser.convertToActions(left->getCurrentDataStream().header.getNamesAndTypesList(), left_keys);
            QueryPlanStepPtr before_join_step = std::make_unique<ExpressionStep>(left->getCurrentDataStream(), actions);
            before_join_step->setStepDescription("Before JOIN LEFT");
            steps.emplace_back(before_join_step.get());
            left->addStep(std::move(before_join_step));
        }

        if (!right_keys->children.empty())
        {
            auto actions = astParser.convertToActions(right->getCurrentDataStream().header.getNamesAndTypesList(), right_keys);
            QueryPlanStepPtr before_join_step = std::make_unique<ExpressionStep>(right->getCurrentDataStream(), actions);
            before_join_step->setStepDescription("Before JOIN RIGHT");
            steps.emplace_back(before_join_step.get());
            right->addStep(std::move(before_join_step));
        }
    }
}

ActionsDAGPtr ASTParser::convertToActions(const NamesAndTypesList & name_and_types, const ASTPtr & ast)
{
    NamesAndTypesList aggregation_keys;
    ColumnNumbersList aggregation_keys_indexes_list;
    AggregationKeysInfo info(aggregation_keys, aggregation_keys_indexes_list, GroupByKind::NONE);
    SizeLimits size_limits_for_set;
    ActionsMatcher::Data visitor_data(
        context,
        size_limits_for_set,
        size_t(0),
        name_and_types,
        std::make_shared<ActionsDAG>(name_and_types),
        nullptr /* prepared_sets */,
        false /* no_subqueries */,
        false /* no_makeset */,
        false /* only_consts */,
        info);
    ActionsVisitor(visitor_data).visit(ast);
    return visitor_data.getActions();
}

ASTPtr ASTParser::parseToAST(const Names & names, const substrait::Expression & rel)
{
    LOG_DEBUG(&Poco::Logger::get("ASTParser"), "substrait plan:\n{}", rel.DebugString());
    if (!rel.has_scalar_function())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "the root of expression should be a scalar function:\n {}", rel.DebugString());

    const auto & scalar_function = rel.scalar_function();
    auto function_signature = function_mapping.at(std::to_string(rel.scalar_function().function_reference()));
    auto function_name = SerializedPlanParser::getFunctionName(function_signature, scalar_function);
    ASTs ast_args;
    parseFunctionArgumentsToAST(names, scalar_function, ast_args);

    return makeASTFunction(function_name, ast_args);
}

void ASTParser::parseFunctionArgumentsToAST(
    const Names & names, const substrait::Expression_ScalarFunction & scalar_function, ASTs & ast_args)
{
    const auto & args = scalar_function.arguments();

    for (const auto & arg : args)
    {
        if (arg.value().has_scalar_function())
        {
            ast_args.emplace_back(parseToAST(names, arg.value()));
        }
        else
        {
            ast_args.emplace_back(parseArgumentToAST(names, arg.value()));
        }
    }
}

ASTPtr ASTParser::parseArgumentToAST(const Names & names, const substrait::Expression & rel)
{
    switch (rel.rex_type_case())
    {
        case substrait::Expression::RexTypeCase::kLiteral: {
            DataTypePtr type;
            Field field;
            std::tie(std::ignore, field) = SerializedPlanParser::parseLiteral(rel.literal());
            return std::make_shared<ASTLiteral>(field);
        }
        case substrait::Expression::RexTypeCase::kSelection: {
            if (!rel.selection().has_direct_reference() || !rel.selection().direct_reference().has_struct_field())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can only have direct struct references in selections");

            const auto field = rel.selection().direct_reference().struct_field().field();
            return std::make_shared<ASTIdentifier>(names[field]);
        }
        case substrait::Expression::RexTypeCase::kCast: {
            if (!rel.cast().has_type() || !rel.cast().has_input())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Doesn't have type or input in cast node.");

            /// Append input to asts
            ASTs args;
            args.emplace_back(parseArgumentToAST(names, rel.cast().input()));

            /// Append destination type to asts
            const auto & substrait_type = rel.cast().type();
            /// Spark cast(x as BINARY) -> CH reinterpretAsStringSpark(x)
            if (substrait_type.has_binary())
                return makeASTFunction("reinterpretAsStringSpark", args);
            else
            {
                DataTypePtr ch_type = SerializedPlanParser::parseType(substrait_type);
                args.emplace_back(std::make_shared<ASTLiteral>(ch_type->getName()));

                return makeASTFunction("CAST", args);
            }
        }
        case substrait::Expression::RexTypeCase::kIfThen: {
            const auto & if_then = rel.if_then();
            const auto * ch_function_name = "multiIf";
            auto function_multi_if = DB::FunctionFactory::instance().get(ch_function_name, context);
            ASTs args;

            auto condition_nums = if_then.ifs_size();
            for (int i = 0; i < condition_nums; ++i)
            {
                const auto & ifs = if_then.ifs(i);
                auto if_node = parseArgumentToAST(names, ifs.if_());
                args.emplace_back(if_node);

                auto then_node = parseArgumentToAST(names, ifs.then());
                args.emplace_back(then_node);
            }

            auto else_node = parseArgumentToAST(names, if_then.else_());
            return makeASTFunction(ch_function_name, args);
        }
        case substrait::Expression::RexTypeCase::kScalarFunction: {
            return parseToAST(names, rel);
        }
        case substrait::Expression::RexTypeCase::kSingularOrList: {
            const auto & options = rel.singular_or_list().options();
            /// options is empty always return false
            if (options.empty())
                return std::make_shared<ASTLiteral>(0);
            /// options should be literals
            if (!options[0].has_literal())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Options of SingularOrList must have literal type");

            ASTs args;
            args.emplace_back(parseArgumentToAST(names, rel.singular_or_list().value()));

            bool nullable = false;
            size_t options_len = options.size();
            args.reserve(options_len);

            for (size_t i = 0; i < options_len; ++i)
            {
                if (!options[i].has_literal())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "in expression values must be the literal!");
                if (!nullable)
                    nullable = options[i].literal().has_null();
            }

            auto elem_type_and_field = SerializedPlanParser::parseLiteral(options[0].literal());
            DataTypePtr elem_type = wrapNullableType(nullable, elem_type_and_field.first);
            for (size_t i = 0; i < options_len; ++i)
            {
                auto type_and_field = std::move(SerializedPlanParser::parseLiteral(options[i].literal()));
                auto option_type = wrapNullableType(nullable, type_and_field.first);
                if (!elem_type->equals(*option_type))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "SingularOrList options type mismatch:{} and {}",
                        elem_type->getName(),
                        option_type->getName());

                args.emplace_back(std::make_shared<ASTLiteral>(type_and_field.second));
            }

            auto ast = makeASTFunction("in", args);
            if (nullable)
            {
                /// if sets has `null` and value not in sets
                /// In Spark: return `null`, is the standard behaviour from ANSI.(SPARK-37920)
                /// In CH: return `false`
                /// So we used if(a, b, c) cast `false` to `null` if sets has `null`
                ast = makeASTFunction("if", ast, std::make_shared<ASTLiteral>(true), std::make_shared<ASTLiteral>(Field()));
            }

            return ast;
        }
        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Join on condition error. Unsupported spark expression type {} : {}",
                magic_enum::enum_name(rel.rex_type_case()),
                rel.DebugString());
    }
}

void SerializedPlanParser::reorderJoinOutput(QueryPlan & plan, DB::Names cols)
{
    ActionsDAGPtr project = std::make_shared<ActionsDAG>(plan.getCurrentDataStream().header.getNamesAndTypesList());
    NamesWithAliases project_cols;
    for (const auto & col : cols)
    {
        project_cols.emplace_back(NameWithAlias(col, col));
    }
    project->project(project_cols);
    QueryPlanStepPtr project_step = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), project);
    project_step->setStepDescription("Reorder Join Output");
    plan.addStep(std::move(project_step));
}

void SerializedPlanParser::removeNullable(std::vector<String> require_columns, ActionsDAGPtr actionsDag)
{
    for (const auto & item : require_columns)
    {
        const auto * require_node = actionsDag->tryFindInOutputs(item);
        if (require_node)
        {
            auto function_builder = FunctionFactory::instance().get("assumeNotNull", context);
            ActionsDAG::NodeRawConstPtrs args = {require_node};
            const auto & node = actionsDag->addFunction(function_builder, args, item);
            actionsDag->addOrReplaceInOutputs(node);
        }
    }
}

void SerializedPlanParser::wrapNullable(
    std::vector<String> columns, ActionsDAGPtr actionsDag, std::map<std::string, std::string> & nullable_measure_names)
{
    for (const auto & item : columns)
    {
        ActionsDAG::NodeRawConstPtrs args;
        args.emplace_back(&actionsDag->findInOutputs(item));
        const auto * node = toFunctionNode(actionsDag, "toNullable", args);
        actionsDag->addOrReplaceInOutputs(*node);
        nullable_measure_names[item] = node->result_name;
    }
}

SharedContextHolder SerializedPlanParser::shared_context;

LocalExecutor::~LocalExecutor()
{
    if (spark_buffer)
    {
        ch_column_to_spark_row->freeMem(spark_buffer->address, spark_buffer->size);
        spark_buffer.reset();
    }
}


void LocalExecutor::execute(QueryPlanPtr query_plan)
{
    current_query_plan = std::move(query_plan);
    Stopwatch stopwatch;
    stopwatch.start();
    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
    DB::QueryPriorities priorities;
    String query = "query";
    const Settings & settings = context->getSettingsRef();
    auto query_status = std::make_shared<DB::QueryStatus>(context,
                                                          query,
                                                          context->getClientInfo(),
                                                          priorities.insert(static_cast<int>(context->getSettingsRef().priority)),
                                                          std::move(DB::CurrentThread::getGroup()),
                                                          DB::IAST::QueryKind::Select,
                                                          settings,
                                                          0);
    auto pipeline_builder = current_query_plan->buildQueryPipeline(
        optimization_settings,
        BuildQueryPipelineSettings{
            .actions_settings = ExpressionActionsSettings{
                .can_compile_expressions = true,
                .min_count_to_compile_expression = 3,
                .compile_expressions = CompileExpressions::yes},
                .process_list_element = query_status});
    query_pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));
    LOG_DEBUG(&Poco::Logger::get("LocalExecutor"), "clickhouse pipeline:\n{}", QueryPipelineUtil::explainPipeline(query_pipeline));
    auto t_pipeline = stopwatch.elapsedMicroseconds();
    executor = std::make_unique<PullingPipelineExecutor>(query_pipeline);
    auto t_executor = stopwatch.elapsedMicroseconds() - t_pipeline;
    stopwatch.stop();
    LOG_INFO(
        &Poco::Logger::get("SerializedPlanParser"),
        "build pipeline {} ms; create executor {} ms;",
        t_pipeline / 1000.0,
        t_executor / 1000.0);
    header = current_query_plan->getCurrentDataStream().header.cloneEmpty();
    ch_column_to_spark_row = std::make_unique<CHColumnToSparkRow>();
}
std::unique_ptr<SparkRowInfo> LocalExecutor::writeBlockToSparkRow(Block & block)
{
    return ch_column_to_spark_row->convertCHColumnToSparkRow(block);
}
bool LocalExecutor::hasNext()
{
    bool has_next;
    try
    {
        size_t columns = currentBlock().columns();
        if (columns == 0 || isConsumed())
        {
            auto empty_block = header.cloneEmpty();
            setCurrentBlock(empty_block);
            has_next = executor->pull(currentBlock());
            if (!has_next)
            {
                has_next = checkAndSetDefaultBlock(columns, has_next);
            }
            produce();
        }
        else
        {
            has_next = true;
        }
    }
    catch (DB::Exception & e)
    {
        LOG_ERROR(
            &Poco::Logger::get("LocalExecutor"),
            "LocalExecutor run query plan failed with message: {}. Plan Explained: \n{}",
            e.message(),
            PlanUtil::explainPlan(*current_query_plan));
        throw;
    }
    return has_next;
}
SparkRowInfoPtr LocalExecutor::next()
{
    checkNextValid();
    SparkRowInfoPtr row_info = writeBlockToSparkRow(currentBlock());
    consume();
    if (spark_buffer)
    {
        ch_column_to_spark_row->freeMem(spark_buffer->address, spark_buffer->size);
        spark_buffer.reset();
    }
    spark_buffer = std::make_unique<SparkBuffer>();
    spark_buffer->address = row_info->getBufferAddress();
    spark_buffer->size = row_info->getTotalBytes();
    return row_info;
}

Block * LocalExecutor::nextColumnar()
{
    checkNextValid();
    Block * columnar_batch;
    if (currentBlock().columns() > 0)
    {
        columnar_batch = &currentBlock();
    }
    else
    {
        auto empty_block = header.cloneEmpty();
        setCurrentBlock(empty_block);
        columnar_batch = &currentBlock();
    }
    consume();
    return columnar_batch;
}

Block & LocalExecutor::getHeader()
{
    return header;
}
LocalExecutor::LocalExecutor(QueryContext & _query_context, ContextPtr context_) : query_context(_query_context), context(context_)
{
}

bool LocalExecutor::checkAndSetDefaultBlock(size_t current_block_columns, bool has_next_blocks)
{
    if (current_block_columns > 0 || has_next_blocks)
    {
        return has_next_blocks;
    }
    auto cols = currentBlock().getColumnsWithTypeAndName();
    for (const auto & col : cols)
    {
        String col_name = col.name;
        DataTypePtr col_type = col.type;
        if (col_name.compare(0, 4, "sum#") != 0 && col_name.compare(0, 4, "max#") != 0 && col_name.compare(0, 4, "min#") != 0
            && col_name.compare(0, 6, "count#") != 0)
        {
            return false;
        }
        if (!isInteger(col_type) && !col_type->isNullable())
        {
            return false;
        }
    }
    for (size_t i = 0; i < cols.size(); i++)
    {
        const DB::ColumnWithTypeAndName col = cols[i];
        String col_name = col.name;
        DataTypePtr col_type = col.type;
        const DB::ColumnPtr & default_col_ptr = col_type->createColumnConst(1, col_type->getDefault());
        const DB::ColumnWithTypeAndName default_col(default_col_ptr, col_type, col_name);
        currentBlock().setColumn(i, default_col);
    }
    if (cols.size() > 0)
        return true;
    return false;
}

}
