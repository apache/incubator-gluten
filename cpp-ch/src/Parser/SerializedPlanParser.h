#pragma once

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Aggregator.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/IStorage.h>
#include <Storages/SourceFromJavaIter.h>
#include <arrow/ipc/writer.h>
#include <substrait/plan.pb.h>
#include <Common/BlockIterator.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <base/types.h>
#include <Core/SortDescription.h>

namespace local_engine
{

static const std::map<std::string, std::string> SCALAR_FUNCTIONS = {
    {"is_not_null","isNotNull"},
    {"is_null","isNull"},
    {"gte","greaterOrEquals"},
    {"gt", "greater"},
    {"lte", "lessOrEquals"},
    {"lt", "less"},
    {"equal", "equals"},

    {"and", "and"},
    {"or", "or"},
    {"not", "not"},
    {"xor", "xor"},

    {"extract", ""},
    {"cast", ""},
    {"alias", "alias"},

    /// datetime functions
    {"to_date", "toDate"},
    {"quarter", "toQuarter"},
    {"to_unix_timestamp", "toUnixTimestamp"},
    {"unix_timestamp", "toUnixTimestamp"},
    {"date_format", "formatDateTimeInJodaSyntax"},

    /// arithmetic functions
    {"subtract", "minus"},
    {"multiply", "multiply"},
    {"add", "plus"},
    {"divide", "divide"},
    {"modulus", "modulo"},
    {"pmod", "pmod"},
    {"abs", "abs"},
    {"ceil", "ceil"},
    {"floor", "floor"},
    {"round", "round"},
    {"bround", "roundBankers"},
    {"exp", "exp"},
    {"power", "power"},
    {"cos", "cos"},
    {"cosh", "cosh"},
    {"sin", "sin"},
    {"sinh", "sinh"},
    {"tan", "tan"},
    {"tanh", "tanh"},
    {"acos", "acos"},
    {"asin", "asin"},
    {"atan", "atan"},
    {"atan2", "atan2"},
    {"bitwise_not", "bitNot"},
    {"bitwise_and", "bitAnd"},
    {"bitwise_or", "bitOr"},
    {"bitwise_xor", "bitXor"},
    {"sqrt", "sqrt"},
    {"cbrt", "cbrt"},
    {"degrees", "degrees"},
    {"e", "e"},
    {"pi", "pi"},
    {"hex", "hex"},
    {"unhex", "unhex"},
    {"hypot", "hypot"},
    {"sign", "sign"},
    {"log10", "log10"},
    {"log1p", "log1p"},
    {"log2", "log2"},
    {"log", "log"},
    {"radians", "radians"},
    {"greatest", "greatest"},
    {"least", "least"},
    {"shiftleft", "bitShiftLeft"},
    {"shiftright", "bitShiftRight"},
    {"check_overflow", "check_overflow"},
    {"factorial", "factorial"},
    {"rand", "randCanonical"},
    {"isnan", "isNaN"},

    /// string functions
    {"like", "like"},
    {"not_like", "notLike"},
    {"starts_with", "startsWith"},
    {"ends_with", "endsWith"},
    {"contains", "countSubstrings"},
    {"substring", "substring"},
    {"lower", "lower"},
    {"upper", "upper"},
    {"trim", ""},
    {"ltrim", ""},
    {"rtrim", ""},
    {"concat", "concat"},
    {"strpos", "position"},
    {"char_length", "char_length"},
    {"replace", "replaceAll"},
    {"regexp_replace", "replaceRegexpAll"},
    {"chr", "char"},
    {"rlike", "match"},
    {"ascii", "ascii"},
    {"split", "splitByRegexp"},
    {"concat_ws", "concat_ws"},
    {"base64", "base64Encode"},
    {"unbase64","base64Decode"},
    {"lpad","leftPadUTF8"},
    {"rpad","rightPadUTF8"},
    {"reverse","reverseUTF8"},
    // {"hash","murmurHash3_32"},
    {"md5","MD5"},
    {"translate", "translateUTF8"},
    {"repeat","repeat"},
    {"position", "positionUTF8Spark"},
    {"locate", "positionUTF8Spark"},

    /// hash functions
    {"hash", "murmurHashSpark3_32"},
    {"xxhash64", "xxHashSpark64"},

    // in functions
    {"in", "in"},

    // null related functions
    {"coalesce", "coalesce"},

    // aggregate functions
    {"count", "count"},
    {"avg", "avg"},
    {"sum", "sum"},
    {"min", "min"},
    {"max", "max"},
    {"collect_list", "groupArray"},
    {"stddev_samp", "stddev_samp"},
    {"stddev_pop", "stddev_pop"},

    // date or datetime functions
    {"from_unixtime", "fromUnixTimestampInJodaSyntax"},
    {"date_add", "addDays"},
    {"date_sub", "subtractDays"},
    {"datediff", "dateDiff"},
    {"second", "toSecond"},
    {"add_months", "addMonths"},
    {"trunc", ""},  /// dummy mapping

    // array functions
    {"array", "array"},
    {"size", "length"},
    {"get_array_item", "arrayElement"},
    {"element_at", "arrayElement"},
    {"array_contains", "has"},

    // map functions
    {"map", "map"},
    {"get_map_value", "arrayElement"},
    {"map_keys", "mapKeys"},
    {"map_values", "mapValues"},

    // tuple functions
    {"get_struct_field", "tupleElement"},
    {"named_struct", "tuple"},

    // table-valued generator function
    {"explode", "arrayJoin"},

    // json functions
    {"get_json_object", "JSON_VALUE"},
    {"to_json", "toJSONString"},
    {"from_json", "JSONExtract"},
};

static const std::set<std::string> FUNCTION_NEED_KEEP_ARGUMENTS = {"alias"};

struct QueryContext
{
    StorageSnapshotPtr storage_snapshot;
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata;
    std::shared_ptr<CustomStorageMergeTree> custom_storage_merge_tree;
};

DataTypePtr wrapNullableType(substrait::Type_Nullability nullable, DataTypePtr nested_type);
DataTypePtr wrapNullableType(bool nullable, DataTypePtr nested_type);

class SerializedPlanParser
{
    friend class RelParser;
    friend class ASTParser;
public:
    explicit SerializedPlanParser(const ContextPtr & context);
    static void initFunctionEnv();
    DB::QueryPlanPtr parse(const std::string & plan);
    DB::QueryPlanPtr parseJson(const std::string & json_plan);
    DB::QueryPlanPtr parse(std::unique_ptr<substrait::Plan> plan);

    DB::QueryPlanPtr parseReadRealWithLocalFile(const substrait::ReadRel & rel);
    DB::QueryPlanPtr parseReadRealWithJavaIter(const substrait::ReadRel & rel);
    DB::QueryPlanPtr parseMergeTreeTable(const substrait::ReadRel & rel);
    PrewhereInfoPtr parsePreWhereInfo(const substrait::Expression & rel, Block & input, std::vector<String>& not_nullable_columns);

    static bool isReadRelFromJava(const substrait::ReadRel & rel);
    static DB::Block parseNameStruct(const substrait::NamedStruct & struct_);
    static DB::DataTypePtr parseType(const substrait::Type & type, std::list<std::string> * names = nullptr);
    // This is used for construct a data type from spark type name;
    static DB::DataTypePtr parseType(const std::string & type);

    void addInputIter(jobject iter) { input_iters.emplace_back(iter); }

    void parseExtensions(const ::google::protobuf::RepeatedPtrField<substrait::extensions::SimpleExtensionDeclaration> & extensions);
    std::shared_ptr<DB::ActionsDAG> expressionsToActionsDAG(
        const std::vector<substrait::Expression> & expressions,
        const DB::Block & header,
        const DB::Block & read_schema);

    static ContextMutablePtr global_context;
    static Context::ConfigurationPtr config;
    static SharedContextHolder shared_context;
    QueryContext query_context;

private:
    static DB::NamesAndTypesList blockToNameAndTypeList(const DB::Block & header);
    DB::QueryPlanPtr parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack);
    void
    collectJoinKeys(const substrait::Expression & condition, std::vector<std::pair<int32_t, int32_t>> & join_keys, int32_t right_key_start);
    DB::QueryPlanPtr parseJoin(substrait::JoinRel join, DB::QueryPlanPtr left, DB::QueryPlanPtr right);
    void parseJoinKeysAndCondition(
        std::shared_ptr<TableJoin> table_join,
        substrait::JoinRel & join,
        DB::QueryPlanPtr & left,
        DB::QueryPlanPtr & right,
        const NamesAndTypesList & alias_right,
        Names & names);

    static void reorderJoinOutput(DB::QueryPlan & plan, DB::Names cols);
    static std::string getFunctionName(const std::string & function_sig, const substrait::Expression_ScalarFunction & function);
    DB::ActionsDAGPtr parseFunction(
        const Block & input,
        const substrait::Expression & rel,
        std::string & result_name,
        std::vector<String> & required_columns,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false);
    DB::ActionsDAGPtr parseArrayJoin(
        const Block & input,
        const substrait::Expression & rel,
        std::vector<String> & result_names,
        std::vector<String> & required_columns,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false);
    const ActionsDAG::Node * parseFunctionWithDAG(
        const substrait::Expression & rel,
        std::string & result_name,
        std::vector<String> & required_columns,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false);
    ActionsDAG::NodeRawConstPtrs parseArrayJoinWithDAG(
        const substrait::Expression & rel,
        std::vector<String> & result_name,
        std::vector<String> & required_columns,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false);
    void parseFunctionArguments(
        DB::ActionsDAGPtr & actions_dag,
        ActionsDAG::NodeRawConstPtrs & parsed_args,
        std::vector<String> & required_columns,
        const std::string & function_name,
        const substrait::Expression_ScalarFunction & scalar_function);
    void parseFunctionArgument(
        DB::ActionsDAGPtr & actions_dag,
        ActionsDAG::NodeRawConstPtrs & parsed_args,
        std::vector<String> & required_columns,
        const std::string & function_name,
        const substrait::FunctionArgument & arg);
    const DB::ActionsDAG::Node * parseFunctionArgument(
        DB::ActionsDAGPtr & actions_dag,
        std::vector<String> & required_columns,
        const std::string & function_name,
        const substrait::FunctionArgument & arg);
    void addPreProjectStepIfNeeded(
        QueryPlan & plan,
        const substrait::AggregateRel & rel,
        std::vector<std::string> & measure_names,
        std::map<std::string, std::string> & nullable_measure_names);
    DB::QueryPlanStepPtr parseAggregate(DB::QueryPlan & plan, const substrait::AggregateRel & rel, bool & is_final);
    const DB::ActionsDAG::Node * parseArgument(DB::ActionsDAGPtr action_dag, const substrait::Expression & rel);
    const ActionsDAG::Node *
    toFunctionNode(ActionsDAGPtr action_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args);
    // remove nullable after isNotNull
    void removeNullable(std::vector<String> require_columns, ActionsDAGPtr actionsDag);
    std::string getUniqueName(const std::string & name) { return name + "_" + std::to_string(name_no++); }

    static std::pair<DataTypePtr, Field> parseLiteral(const substrait::Expression_Literal & literal);
    void wrapNullable(std::vector<String> columns, ActionsDAGPtr actionsDag,
                      std::map<std::string, std::string>& nullable_measure_names);

    static Aggregator::Params getAggregateParam(const Names & keys,
                                                const AggregateDescriptions & aggregates)
    {
        Settings settings;
        return Aggregator::Params(
            keys,
            aggregates,
            false,
            settings.max_rows_to_group_by,
            settings.group_by_overflow_mode,
            settings.group_by_two_level_threshold,
            settings.group_by_two_level_threshold_bytes,
            settings.max_bytes_before_external_group_by,
            settings.empty_result_for_aggregation_by_empty_set,
            nullptr,
            settings.max_threads,
            settings.min_free_disk_space_for_temporary_data,
            true,
            3,
            settings.max_block_size,
            false,
            false);
    }

    static Aggregator::Params
    getMergedAggregateParam(const Names & keys, const AggregateDescriptions & aggregates)
    {
        Settings settings;
        return Aggregator::Params(keys, aggregates, false, settings.max_threads, settings.max_block_size);
    }

    void addRemoveNullableStep(QueryPlan & plan, std::vector<String> columns);

    static std::pair<DB::DataTypePtr, DB::Field> convertStructFieldType(const DB::DataTypePtr & type, const DB::Field & field);

    int name_no = 0;
    std::unordered_map<std::string, std::string> function_mapping;
    std::vector<jobject> input_iters;
    const substrait::ProjectRel * last_project = nullptr;
    ContextPtr context;

};

struct SparkBuffer
{
    char * address;
    size_t size;
};

class LocalExecutor : public BlockIterator
{
public:
    LocalExecutor() = default;
    explicit LocalExecutor(QueryContext & _query_context);
    void execute(QueryPlanPtr query_plan);
    SparkRowInfoPtr next();
    Block * nextColumnar();
    bool hasNext();
    ~LocalExecutor();

    Block & getHeader();

private:
    QueryContext query_context;
    std::unique_ptr<SparkRowInfo> writeBlockToSparkRow(DB::Block & block);
    QueryPipeline query_pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    Block header;
    std::unique_ptr<CHColumnToSparkRow> ch_column_to_spark_row;
    std::unique_ptr<SparkBuffer> spark_buffer;
    DB::QueryPlanPtr current_query_plan;
};


class ASTParser
{
public:
    explicit ASTParser(const ContextPtr & _context, std::unordered_map<std::string, std::string> & _function_mapping)
        : context(_context), function_mapping(_function_mapping){};
    ~ASTParser() = default;

    ASTPtr parseToAST(const Names & names, const substrait::Expression & rel);
    ActionsDAGPtr convertToActions(const NamesAndTypesList & name_and_types, const ASTPtr & ast);

private:
    ContextPtr context;
    std::unordered_map<std::string, std::string> function_mapping;

    void parseFunctionArgumentsToAST(const Names & names, const substrait::Expression_ScalarFunction & scalar_function, ASTs & ast_args);
    ASTPtr parseArgumentToAST(const Names & names, const substrait::Expression & rel);
};
}
