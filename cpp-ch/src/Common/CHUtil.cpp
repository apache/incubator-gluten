#include <filesystem>
#include <memory>
#include <optional>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/printPipeline.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>
#include <Poco/Util/MapConfiguration.h>
#include <Common/typeid_cast.h>
#include <substrait/algebra.pb.h>
#include <substrait/plan.pb.h>
#include <Parser/SerializedPlanParser.h>
#include <Parser/RelParser.h>
#include <Common/Logger.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "CHUtil.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
constexpr auto VIRTUAL_ROW_COUNT_COLOUMN = "__VIRTUAL_ROW_COUNT_COLOUMNOUMN__";

namespace fs = std::filesystem;

DB::Block BlockUtil::buildRowCountHeader()
{
    DB::Block header;
    auto uint8_ty = std::make_shared<DB::DataTypeUInt8>();
    auto col = uint8_ty->createColumn();
    DB::ColumnWithTypeAndName named_col(std::move(col), uint8_ty, VIRTUAL_ROW_COUNT_COLOUMN);
    header.insert(named_col);
    return header.cloneEmpty();
}

DB::Chunk BlockUtil::buildRowCountChunk(UInt64 rows)
{
    auto data_type = std::make_shared<DB::DataTypeUInt8>();
    auto col = data_type->createColumnConst(rows, 0);
    DB::Columns res_columns;
    res_columns.emplace_back(std::move(col));
    return DB::Chunk(std::move(res_columns), rows);
}

DB::Block BlockUtil::buildRowCountBlock(UInt64 rows)
{
    DB::Block block;
    auto uint8_ty = std::make_shared<DB::DataTypeUInt8>();
    auto col = uint8_ty->createColumnConst(rows, 0);
    DB::ColumnWithTypeAndName named_col(col, uint8_ty, VIRTUAL_ROW_COUNT_COLOUMN);
    block.insert(named_col);
    return block;
}

DB::Block BlockUtil::buildHeader(const DB::NamesAndTypesList & names_types_list)
{
    DB::ColumnsWithTypeAndName cols;
    for (const auto & name_type : names_types_list)
    {
        DB::ColumnWithTypeAndName col(name_type.type->createColumn(), name_type.type, name_type.name);
        cols.emplace_back(col);
    }
    return DB::Block(cols);
}

/**
 * There is a special case with which we need be careful. In spark, struct/map/list are always
 * wrapped in Nullable, but this should not happen in clickhouse.
 */
DB::Block BlockUtil::flattenBlock(const DB::Block & block, UInt64 flags, bool recursively)
{
    DB::Block res;

    for (const auto & elem : block)
    {
        DB::DataTypePtr nested_type = nullptr;
        DB::ColumnPtr nested_col = nullptr;
        DB::ColumnPtr null_map_col = nullptr;
        if (elem.type->isNullable())
        {
            nested_type = typeid_cast<const DB::DataTypeNullable *>(elem.type.get())->getNestedType();
            const auto * null_col = typeid_cast<const DB::ColumnNullable *>(elem.column->getPtr().get());
            nested_col = null_col->getNestedColumnPtr();
            null_map_col = null_col->getNullMapColumnPtr();
        }
        else
        {
            nested_type = elem.type;
            nested_col = elem.column;
        }
        if (const DB::DataTypeArray * type_arr = typeid_cast<const DB::DataTypeArray *>(nested_type.get()))
        {
            const DB::DataTypeTuple * type_tuple = typeid_cast<const DB::DataTypeTuple *>(type_arr->getNestedType().get());
            if (type_tuple && type_tuple->haveExplicitNames() && (flags & FLAT_NESTED_TABLE))
            {
                const DB::DataTypes & element_types = type_tuple->getElements();
                const DB::Strings & names = type_tuple->getElementNames();
                size_t tuple_size = element_types.size();

                bool is_const = isColumnConst(*nested_col);
                const DB::ColumnArray * column_array;
                if (is_const)
                    column_array = typeid_cast<const DB::ColumnArray *>(&assert_cast<const DB::ColumnConst &>(*nested_col).getDataColumn());
                else
                    column_array = typeid_cast<const DB::ColumnArray *>(nested_col.get());

                const DB::ColumnPtr & column_offsets = column_array->getOffsetsPtr();

                const DB::ColumnTuple & column_tuple = typeid_cast<const DB::ColumnTuple &>(column_array->getData());
                const auto & element_columns = column_tuple.getColumns();

                for (size_t i = 0; i < tuple_size; ++i)
                {
                    String nested_name = DB::Nested::concatenateName(elem.name, names[i]);
                    DB::ColumnPtr column_array_of_element = DB::ColumnArray::create(element_columns[i], column_offsets);
                    auto named_column_array_of_element = DB::ColumnWithTypeAndName(
                        is_const ? DB::ColumnConst::create(std::move(column_array_of_element), block.rows()) : column_array_of_element,
                        std::make_shared<DB::DataTypeArray>(element_types[i]),
                        nested_name);
                    if (null_map_col)
                    {
                        // Should all field columns have the same null map ?
                        DB::DataTypePtr null_type = std::make_shared<DB::DataTypeNullable>(element_types[i]);
                        named_column_array_of_element.column = DB::ColumnNullable::create(named_column_array_of_element.column, null_map_col);
                        named_column_array_of_element.type = null_type;
                    }
                    if (recursively)
                    {
                        auto flatten_one_col_block = flattenBlock({named_column_array_of_element}, flags, recursively);
                        for (const auto & named_col : flatten_one_col_block.getColumnsWithTypeAndName())
                        {
                            res.insert(named_col);
                        }
                    }
                    else
                    {
                        res.insert(named_column_array_of_element);
                    }
                }
            }
            else
            {
                res.insert(elem);
            }
        }
        else if (const DB::DataTypeTuple * type_tuple = typeid_cast<const DB::DataTypeTuple *>(nested_type.get()))
        {
            if (type_tuple->haveExplicitNames() && (flags & FLAT_STRUCT))
            {
                const DB::DataTypes & element_types = type_tuple->getElements();
                const DB::Strings & names = type_tuple->getElementNames();
                const DB::ColumnTuple * column_tuple;
                if (isColumnConst(*nested_col))
                    column_tuple = typeid_cast<const DB::ColumnTuple *>(&assert_cast<const DB::ColumnConst &>(*nested_col).getDataColumn());
                else
                    column_tuple = typeid_cast<const DB::ColumnTuple *>(nested_col.get());
                size_t tuple_size = column_tuple->tupleSize();
                for (size_t i = 0; i < tuple_size; ++i)
                {
                    const auto & element_column = column_tuple->getColumn(i);
                    String nested_name = DB::Nested::concatenateName(elem.name, names[i]);
                    auto new_element_col = DB::ColumnWithTypeAndName(element_column.getPtr(), element_types[i], nested_name);
                    if (null_map_col && !element_types[i]->isNullable())
                    {
                        // Should all field columns have the same null map ?
                        new_element_col.column =  DB::ColumnNullable::create(new_element_col.column, null_map_col);
                        new_element_col.type = std::make_shared<DB::DataTypeNullable>(new_element_col.type);
                    }
                    if (recursively)
                    {
                        DB::Block one_col_block({new_element_col});
                        auto flatten_one_col_block = flattenBlock(one_col_block, flags, recursively);
                        for (const auto & named_col : flatten_one_col_block.getColumnsWithTypeAndName())
                        {
                            res.insert(named_col);
                        }
                    }
                    else
                    {
                        res.insert(std::move(new_element_col));
                    }
                }
            }
            else
            {
                res.insert(elem);
            }
        }
        else
        {
            res.insert(elem);
        }
    }

    return res;
}

std::string PlanUtil::explainPlan(DB::QueryPlan & plan)
{
    std::string plan_str;
    DB::QueryPlan::ExplainPlanOptions buf_opt
    {
        .header = true,
        .actions = true,
        .indexes = true,
    };
    DB::WriteBufferFromOwnString buf;
    plan.explainPlan(buf, buf_opt);
    plan_str = buf.str();
    return plan_str;
}

std::vector<MergeTreeUtil::Path> MergeTreeUtil::getAllMergeTreeParts(const Path &storage_path)
{
    if (!fs::exists(storage_path))
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid merge tree store path:{}", storage_path.string());
    }

    // TODO: May need to check the storage format version
    std::vector<fs::path> res;
    for (const auto & entry : fs::directory_iterator(storage_path))
    {
        auto filename = entry.path().filename();
        if (filename == "format_version.txt" || filename == "detached" || filename == "_delta_log")
            continue;
        res.push_back(entry.path());
    }
    return res;
}

DB::NamesAndTypesList MergeTreeUtil::getSchemaFromMergeTreePart(const fs::path & part_path)
{
    DB::NamesAndTypesList names_types_list;
    if (!fs::exists(part_path))
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid merge tree store path:{}", part_path.string());
    }
    DB::ReadBufferFromFile readbuffer((part_path / "columns.txt").string());
    names_types_list.readText(readbuffer);
    return names_types_list;
}


NestedColumnExtractHelper::NestedColumnExtractHelper(const DB::Block & block_, bool case_insentive_)
    : block(block_)
    , case_insentive(case_insentive_)
{}

std::optional<DB::ColumnWithTypeAndName> NestedColumnExtractHelper::extractColumn(const String & column_name)
{
    if (const auto * col = findColumn(block, column_name))
        return {*col};

    auto nested_names = DB::Nested::splitName(column_name);
    if (case_insentive)
    {
        boost::to_lower(nested_names.first);
        boost::to_lower(nested_names.second);
    }
    if (!findColumn(block, nested_names.first))
        return {};

    if (!nested_tables.contains(nested_names.first))
    {
        DB::ColumnsWithTypeAndName columns = {*findColumn(block, nested_names.first)};
        nested_tables[nested_names.first] = std::make_shared<DB::Block>(BlockUtil::flattenBlock(columns));
    }

    return extractColumn(column_name, nested_names.first, nested_names.second);
}

std::optional<DB::ColumnWithTypeAndName> NestedColumnExtractHelper::extractColumn(
    const String & original_column_name, const String & column_name_prefix, const String & column_name_suffix)
{
     auto table_iter = nested_tables.find(column_name_prefix);
    if (table_iter == nested_tables.end())
    {
        return {};
    }

    auto & nested_table = table_iter->second;
    auto nested_names = DB::Nested::splitName(column_name_suffix);
    auto new_column_name_prefix = DB::Nested::concatenateName(column_name_prefix, nested_names.first);
    if (nested_names.second.empty())
    {
        if (const auto * column_ref = findColumn(*nested_table, new_column_name_prefix))
        {
            DB::ColumnWithTypeAndName column = *column_ref;
            if (case_insentive)
                column.name = original_column_name;
            return {std::move(column)};
        }
        else
        {
            return {};
        }
    }

    const auto * sub_col = findColumn(*nested_table, new_column_name_prefix);
    if (!sub_col)
    {
        return {};
    }

    DB::ColumnsWithTypeAndName columns = {*sub_col};
    DB::Block sub_block(columns);
    nested_tables[new_column_name_prefix] = std::make_shared<DB::Block>(BlockUtil::flattenBlock(sub_block));
    return extractColumn(original_column_name, new_column_name_prefix, nested_names.second);
}

const DB::ColumnWithTypeAndName * NestedColumnExtractHelper::findColumn(const DB::Block & in_block, const std::string & name) const
{

    if (case_insentive)
    {
        std::string final_name = name;
        boost::to_lower(final_name);
        const auto & cols = in_block.getColumnsWithTypeAndName();
        auto found = std::find_if(cols.begin(), cols.end(), [&](const auto & column) { return boost::iequals(column.name, name); });
        if (found == cols.end())
        {
            return nullptr;
        }
        return &*found;
    }

    const auto * col = in_block.findByName(name);
    if (col)
        return col;
    return nullptr;
}

const DB::ActionsDAG::Node * ActionsDAGUtil::convertNodeType(
    DB::ActionsDAGPtr & actions_dag, const DB::ActionsDAG::Node * node, const std::string & type_name, const std::string & result_name)
{
    DB::ColumnWithTypeAndName type_name_col;
    type_name_col.name = type_name;
    type_name_col.column = DB::DataTypeString().createColumnConst(0, type_name_col.name);
    type_name_col.type = std::make_shared<DB::DataTypeString>();
    const auto * right_arg = &actions_dag->addColumn(std::move(type_name_col));
    const auto * left_arg = node;
    DB::FunctionCastBase::Diagnostic diagnostic = {node->result_name, node->result_name};
    DB::FunctionOverloadResolverPtr func_builder_cast
        = DB::CastInternalOverloadResolver<DB::CastType::nonAccurate>::createImpl(std::move(diagnostic));

    DB::ActionsDAG::NodeRawConstPtrs children = {left_arg, right_arg};
    return &actions_dag->addFunction(func_builder_cast, std::move(children), result_name);
}

String QueryPipelineUtil::explainPipeline(DB::QueryPipeline & pipeline)
{
    DB::WriteBufferFromOwnString buf;
    const auto & processors = pipeline.getProcessors();
    DB::printPipelineCompact(processors, buf, true);
    return buf.str();
}

using namespace DB;

std::map<std::string, std::string> BackendInitializerUtil::getBackendConfMap(const std::string &plan)
{
    std::map<std::string, std::string> ch_backend_conf;

    /// Parse backend configs from plan extensions
    do
    {
        auto plan_ptr = std::make_unique<substrait::Plan>();
        auto success = plan_ptr->ParseFromString(plan);
        if (!success)
            break;

        if (!plan_ptr->has_advanced_extensions() || !plan_ptr->advanced_extensions().has_enhancement())
            break;
        const auto & enhancement = plan_ptr->advanced_extensions().enhancement();

        if (!enhancement.Is<substrait::Expression>())
            break;

        substrait::Expression expression;
        if (!enhancement.UnpackTo(&expression) || !expression.has_literal() || !expression.literal().has_map())
            break;

        const auto & key_values = expression.literal().map().key_values();
        for (const auto & key_value : key_values)
        {
             if (!key_value.has_key() || !key_value.has_value())
                continue;

            const auto & key = key_value.key();
            const auto & value = key_value.value();
            if (!key.has_string() || !value.has_string())
                continue;

            if (!key.string().starts_with(CH_BACKEND_CONF_PREFIX) && key.string() != std::string(GLUTEN_TIMEZONE_KEY))
                continue;

            ch_backend_conf[key.string()] = value.string();
        }
    } while (false);

    if (!ch_backend_conf.count(CH_RUNTIME_CONF_FILE))
    {
        /// Try to get config path from environment variable
        const char * config_path = std::getenv("CLICKHOUSE_BACKEND_CONFIG"); /// NOLINT
        if (config_path)
        {
            ch_backend_conf[CH_RUNTIME_CONF_FILE] = config_path;
        }
    }
    return ch_backend_conf;
}

void BackendInitializerUtil::initConfig(const std::string &plan)
{
    /// Parse input substrait plan, and get native conf map from it.
    std::map<std::string, std::string> backend_conf_map;
    backend_conf_map = getBackendConfMap(plan);

    if (backend_conf_map.count(CH_RUNTIME_CONF_FILE))
    {
        if (fs::exists(CH_RUNTIME_CONF_FILE) && fs::is_regular_file(CH_RUNTIME_CONF_FILE))
        {
            ConfigProcessor config_processor(CH_RUNTIME_CONF_FILE, false, true);
            config_processor.setConfigPath(fs::path(CH_RUNTIME_CONF_FILE).parent_path());
            auto loaded_config = config_processor.loadConfig(false);
            config = loaded_config.configuration;
        }
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "{} is not a valid configure file.", CH_RUNTIME_CONF_FILE);
    }
    else
        config = Poco::AutoPtr(new Poco::Util::MapConfiguration());

    /// Update specified settings
    for (const auto & kv : backend_conf_map)
    {
        if (kv.first.starts_with(CH_RUNTIME_CONF_PREFIX) && kv.first != CH_RUNTIME_CONF_FILE)
            config->setString(kv.first.substr(CH_RUNTIME_CONF_PREFIX.size() + 1), kv.second);
        else if (kv.first == std::string(GLUTEN_TIMEZONE_KEY))
            config->setString(kv.first, kv.second);
    }
}

void BackendInitializerUtil::initLoggers()
{
    auto level = config->getString("logger.level", "error");
    if (config->has("logger.log"))
        local_engine::Logger::initFileLogger(*config, "ClickHouseBackend");
    else
        local_engine::Logger::initConsoleLogger(level);

    logger = &Poco::Logger::get("ClickHouseBackend");
}

void BackendInitializerUtil::initEnvs()
{
    /// Set environment variable TZ if possible
    if (config->has(GLUTEN_TIMEZONE_KEY))
    {
        String timezone_name = config->getString(GLUTEN_TIMEZONE_KEY);
        if (0 != setenv("TZ", timezone_name.data(), 1)) /// NOLINT
            throw Poco::Exception("Cannot setenv TZ variable");

        tzset();
        DateLUT::setDefaultTimezone(timezone_name);
    }

    /// Set environment variable LIBHDFS3_CONF if possible
    if (config->has(LIBHDFS3_CONF_KEY))
    {
        std::string libhdfs3_conf = config->getString(LIBHDFS3_CONF_KEY, "");
        setenv("LIBHDFS3_CONF", libhdfs3_conf.c_str(), true); /// NOLINT
    }
}

void BackendInitializerUtil::initSettings()
{
    static const std::string settings_path("local_engine.settings");

    settings = Settings();
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config->keys(settings_path, config_keys);

    for (const std::string & key : config_keys)
        settings.set(key, config->getString(settings_path + "." + key));
    settings.set("join_use_nulls", true);
    settings.set("input_format_orc_allow_missing_columns", true);
    settings.set("input_format_orc_case_insensitive_column_matching", true);
    settings.set("input_format_parquet_allow_missing_columns", true);
    settings.set("input_format_parquet_case_insensitive_column_matching", true);
}

void BackendInitializerUtil::initContexts()
{
    /// Make sure global_context and shared_context are constructed only once.
    auto & shared_context = SerializedPlanParser::shared_context;
    if (!shared_context.get())
    {
        shared_context = SharedContextHolder(Context::createShared());
    }

    auto & global_context = SerializedPlanParser::global_context;
    if (!global_context)
    {
        global_context = Context::createGlobal(shared_context.get());
        global_context->makeGlobalContext();
        global_context->setTemporaryStoragePath("/tmp/libch", 0);
        global_context->setPath(config->getString("path", "/"));
    }
}

void BackendInitializerUtil::applyConfigAndSettings()
{
    auto & global_context = SerializedPlanParser::global_context;
    global_context->setConfig(config);
    global_context->setSettings(settings);
}

extern void registerAggregateFunctionCombinatorPartialMerge(AggregateFunctionCombinatorFactory &);
extern void registerFunctions(FunctionFactory &);

void registerAllFunctions()
{
    DB::registerFunctions();
    DB::registerAggregateFunctions();

    {
        /// register aggregate function combinators from local_engine
        auto & factory = AggregateFunctionCombinatorFactory::instance();
        registerAggregateFunctionCombinatorPartialMerge(factory);
    }

    {
        /// register ordinary functions from local_engine
        auto & factory = FunctionFactory::instance();
        registerFunctions(factory);
    }
}

extern void registerAllFunctions();

void BackendInitializerUtil::registerAllFactories()
{
    registerReadBufferBuilders();
    LOG_INFO(logger, "Register read buffer builders.");

    registerRelParsers();
    LOG_INFO(logger, "Register relation parsers.");

    registerAllFunctions();
    LOG_INFO(logger, "Register all functions.");
}

void BackendInitializerUtil::initCompiledExpressionCache()
{
    #if USE_EMBEDDED_COMPILER
    /// 128 MB
    constexpr size_t compiled_expression_cache_size_default = 1024 * 1024 * 128;
    size_t compiled_expression_cache_size = config->getUInt64("compiled_expression_cache_size", compiled_expression_cache_size_default);

    constexpr size_t compiled_expression_cache_elements_size_default = 10000;
    size_t compiled_expression_cache_elements_size
        = config->getUInt64("compiled_expression_cache_elements_size", compiled_expression_cache_elements_size_default);

    CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_size, compiled_expression_cache_elements_size);
#endif
}

void BackendInitializerUtil::init(const std::string & plan)
{
    initConfig(plan);
    initLoggers();

    initEnvs();
    LOG_INFO(logger, "Init environment variables.");

    initSettings();
    LOG_INFO(logger, "Init settings.");

    initContexts();
    LOG_INFO(logger, "Init shared context and global context.");

    applyConfigAndSettings();
    LOG_INFO(logger, "Apply configuration and setting for global context.");

    std::call_once(
        init_flag,
        [&]
        {
            registerAllFactories();
            LOG_INFO(logger, "Register all factories.");

            initCompiledExpressionCache();
            LOG_INFO(logger, "Init compiled expressions cache factory.");
        });
}

void BackendFinalizerUtil::finalizeGlobally()
{
    auto & global_context = SerializedPlanParser::global_context;
    auto & shared_context = SerializedPlanParser::shared_context;
    auto * logger = BackendInitializerUtil::logger;
    if (global_context)
    {
        global_context->shutdown();
        global_context.reset();
        shared_context.reset();
    }
}

void BackendFinalizerUtil::finalizeSessionall()
{
    /// TODO: figure out why BroadCastJoinBuilder::clean would cause core issue
    /// Currently remove it as a workaround
    // local_engine::BroadCastJoinBuilder::clean();
}

}
