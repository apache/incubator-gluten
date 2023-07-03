#pragma once

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Aggregator.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/CustomStorageMergeTree.h>
#include <Storages/IStorage.h>
#include <Storages/SourceFromJavaIter.h>
#include <arrow/ipc/writer.h>
#include <base/types.h>
#include <substrait/plan.pb.h>
#include <Common/BlockIterator.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <base/types.h>
#include <Core/SortDescription.h>
#include <Parser/RelMetric.h>

namespace local_engine
{
static const std::map<std::string, std::string> SCALAR_FUNCTIONS
    = {{"is_not_null", "isNotNull"},
       {"is_null", "isNull"},
       {"gte", "greaterOrEquals"},
       {"gt", "greater"},
       {"lte", "lessOrEquals"},
       {"lt", "less"},
       {"equal", "equals"},

       {"and", "and"},
       {"or", "or"},
       {"not", "not"},
       {"xor", "xor"},

       {"extract", ""},
       {"cast", "CAST"},
       {"alias", "alias"},

       /// datetime functions
       {"get_timestamp", "parseDateTimeInJodaSyntaxOrNull"}, // for spark function: to_date/to_timestamp
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
       {"asinh", "asinh"},
       {"acosh", "acosh"},
       {"atanh", "atanh"},
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
       {"check_overflow", "checkDecimalOverflowSpark"},
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
       {"trim", ""}, // trimLeft or trimLeftSpark, depends on argument size
       {"ltrim", ""}, // trimRight or trimRightSpark, depends on argument size
       {"rtrim", ""}, // trimBoth or trimBothSpark, depends on argument size
       {"concat", "concat"},
       {"strpos", "position"},
       {"char_length",
        "char_length"}, /// Notice: when input argument is binary type, corresponding ch function is length instead of char_length
       {"replace", "replaceAll"},
       {"regexp_replace", "replaceRegexpAll"},
       {"regexp_extract", "regexpExtract"},
       {"regexp_extract_all", "regexpExtractAllSpark"},
       {"chr", "char"},
       {"rlike", "match"},
       {"ascii", "ascii"},
       {"split", "splitByRegexp"},
       {"concat_ws", "concat_ws"},
       {"base64", "base64Encode"},
       {"unbase64", "base64Decode"},
       {"lpad", "leftPadUTF8"},
       {"rpad", "rightPadUTF8"},
       {"reverse", "reverseUTF8"},
       {"md5", "MD5"},
       {"translate", "translateUTF8"},
       {"repeat", "repeat"},
       {"position", "positionUTF8Spark"},
       {"locate", "positionUTF8Spark"},
       {"space", "space"},

       /// hash functions
       {"murmur3hash", "murmurHashSpark3_32"},
       {"xxhash64", "xxHashSpark64"},
       {"sha1", "SHA1"},
       {"sha2", ""}, /// dummpy mapping
       {"crc32", "CRC32"},

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
       {"first", "first_value_respect_nulls"},
       {"first_ignore_null", "first_value"},
       {"last_ignore_null", "last_value"},
       {"last", "last_value_respect_nulls"},
       {"approx_percentile", ""}, /// dummy mapping, maybe quantilesGK or quantileGK

       // window functions
       {"lead", "lead"},
       {"lag", "lag"},
       {"dense_rank", "dense_rank"},
       {"rank", "rank"},
       {"row_number", "row_number"},
       {"ntile", "ntile"},
       {"percent_rank", "percent_rank"},
       {"cume_dist", "cume_dist"},

       // In Spark, stddev is the alias for stddev_samp.
       {"stddev", "stddev_samp"},
       {"stddev_samp", "stddev_samp"},
       {"stddev_pop", "stddev_pop"},
       {"bit_and", "groupBitAnd"},
       {"bit_or", "groupBitOr"},
       {"bit_xor", "groupBitXor"},
       {"covar_pop", "covarPop"},
       {"covar_samp", "covarSamp"},

       // date or datetime functions
       {"from_unixtime", "fromUnixTimestampInJodaSyntax"},
       {"date_add", "addDays"},
       {"date_sub", "subtractDays"},
       {"datediff", "dateDiff"},
       {"second", "toSecond"},
       {"add_months", "addMonths"},
       {"trunc", ""}, /// dummy mapping
       {"date_trunc", "dateTrunc"},
       {"floor_datetime", "dateTrunc"},

       // array functions
       {"array", "array"},
       {"size", "length"},
       {"get_array_item", "arrayElement"},
       {"element_at", "arrayElement"},
       {"array_contains", "has"},
       {"range", "range"}, /// dummy mapping

       // map functions
       {"map", "map"},
       {"get_map_value", "arrayElement"},
       {"map_keys", "mapKeys"},
       {"map_values", "mapValues"},
       {"map_from_arrays", "mapFromArrays"},

       // tuple functions
       {"get_struct_field", "tupleElement"},
       {"named_struct", "tuple"},

       // table-valued generator function
       {"explode", "arrayJoin"},
       {"posexplode", "arrayJoin"},

       // json functions
       {"get_json_object", "get_json_object"},
       {"to_json", "toJSONString"},
       {"from_json", "JSONExtract"},
       {"json_tuple", "json_tuple"},
       {"json_array_length", "JSONArrayLength"},
       {"make_decimal", "makeDecimalSpark"},
       {"unscaled_value", "unscaleValueSpark"}};

static const std::set<std::string> FUNCTION_NEED_KEEP_ARGUMENTS = {"alias"};

struct QueryContext
{
    StorageSnapshotPtr storage_snapshot;
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata;
    std::shared_ptr<CustomStorageMergeTree> custom_storage_merge_tree;
};

DataTypePtr wrapNullableType(substrait::Type_Nullability nullable, DataTypePtr nested_type);
DataTypePtr wrapNullableType(bool nullable, DataTypePtr nested_type);

std::string join(const ActionsDAG::NodeRawConstPtrs & v, char c);
bool isTypeMatched(const substrait::Type & substrait_type, const DataTypePtr & ch_type);

class SerializedPlanParser;

// Give a condition expression `cond_rel_`, found all columns with nullability that must not containt
// null after this filter.
// It's used to remove nullability of the columns for performance reason.
class NonNullableColumnsResolver
{
public:
    explicit NonNullableColumnsResolver(const DB::Block & header_, SerializedPlanParser & parser_,  const substrait::Expression & cond_rel_);
    ~NonNullableColumnsResolver() = default;
    // return column names
    std::vector<std::string> resolve();
private:
    DB::Block header;
    SerializedPlanParser & parser;
    const substrait::Expression & cond_rel;

    std::vector<std::string> collected_columns;

    void visit(const substrait::Expression & expr);
    void visitNonNullable(const substrait::Expression & expr);
};

template <typename T>
concept SubstraitFunction = std::same_as<T, substrait::Expression_ScalarFunction> || std::same_as<T, substrait::Expression_WindowFunction>
    || std::same_as<T, substrait::AggregateFunction>;

class SerializedPlanParser
{
private:
    friend class RelParser;
    friend class ASTParser;
    friend class FunctionParser;
    friend class FunctionExecutor;
    friend class NonNullableColumnsResolver;

public:

    explicit SerializedPlanParser(const ContextPtr & context);
    DB::QueryPlanPtr parse(const std::string & plan);
    DB::QueryPlanPtr parseJson(const std::string & json_plan);
    DB::QueryPlanPtr parse(std::unique_ptr<substrait::Plan> plan);

    DB::QueryPlanStepPtr parseReadRealWithLocalFile(const substrait::ReadRel & rel);
    DB::QueryPlanStepPtr parseReadRealWithJavaIter(const substrait::ReadRel & rel);
    // mergetree need create two steps in parse, can't return single step
    DB::QueryPlanPtr parseMergeTreeTable(const substrait::ReadRel & rel, std::vector<IQueryPlanStep *>& steps);
    PrewhereInfoPtr parsePreWhereInfo(const substrait::Expression & rel, Block & input);

    static bool isReadRelFromJava(const substrait::ReadRel & rel);
    static DB::Block parseNameStruct(const substrait::NamedStruct & struct_);
    static DB::DataTypePtr parseType(const substrait::Type & type, std::list<std::string> * names = nullptr);
    // This is used for construct a data type from spark type name;
    static DB::DataTypePtr parseType(const std::string & type);

    void addInputIter(jobject iter) { input_iters.emplace_back(iter); }

    void parseExtensions(const ::google::protobuf::RepeatedPtrField<substrait::extensions::SimpleExtensionDeclaration> & extensions);
    std::shared_ptr<DB::ActionsDAG> expressionsToActionsDAG(
        const std::vector<substrait::Expression> & expressions, const DB::Block & header, const DB::Block & read_schema);
    RelMetricPtr getMetric()
    {
        return metrics.at(0);
    }

    static ContextMutablePtr global_context;
    static Context::ConfigurationPtr config;
    static SharedContextHolder shared_context;
    QueryContext query_context;
    std::vector<QueryPlanPtr> extra_plan_holder;

private:
    static DB::NamesAndTypesList blockToNameAndTypeList(const DB::Block & header);
    DB::QueryPlanPtr parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack);
    void
    collectJoinKeys(const substrait::Expression & condition, std::vector<std::pair<int32_t, int32_t>> & join_keys, int32_t right_key_start);
    DB::QueryPlanPtr parseJoin(substrait::JoinRel join, DB::QueryPlanPtr left, DB::QueryPlanPtr right, std::vector<IQueryPlanStep *>& steps);
    void parseJoinKeysAndCondition(
        std::shared_ptr<TableJoin> table_join,
        substrait::JoinRel & join,
        DB::QueryPlanPtr & left,
        DB::QueryPlanPtr & right,
        const NamesAndTypesList & alias_right,
        Names & names,
        std::vector<IQueryPlanStep *>& steps);

    static void reorderJoinOutput(DB::QueryPlan & plan, DB::Names cols);

    template <SubstraitFunction F>
    static std::string getFunctionName(const std::string & function_sig, const F & function);

    DB::ActionsDAGPtr parseFunction(
        const Block & header,
        const substrait::Expression & rel,
        std::string & result_name,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false);
    DB::ActionsDAGPtr parseArrayJoin(
        const Block & input,
        const substrait::Expression & rel,
        std::vector<String> & result_names,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false,
        bool position = false);
    DB::ActionsDAGPtr parseJsonTuple(
        const Block & input,
        const substrait::Expression & rel,
        std::vector<String> & result_names,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false,
        bool position = false);
    const ActionsDAG::Node * parseFunctionWithDAG(
        const substrait::Expression & rel,
        std::string & result_name,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false);
    ActionsDAG::NodeRawConstPtrs parseArrayJoinWithDAG(
        const substrait::Expression & rel,
        std::vector<String> & result_name,
        DB::ActionsDAGPtr actions_dag = nullptr,
        bool keep_result = false,
        bool position = false);
    void parseFunctionArguments(
        DB::ActionsDAGPtr & actions_dag,
        ActionsDAG::NodeRawConstPtrs & parsed_args,
        std::string & function_name,
        const substrait::Expression_ScalarFunction & scalar_function);
    void parseFunctionArgument(
        DB::ActionsDAGPtr & actions_dag,
        ActionsDAG::NodeRawConstPtrs & parsed_args,
        const std::string & function_name,
        const substrait::FunctionArgument & arg);
    const DB::ActionsDAG::Node * parseFunctionArgument(
        DB::ActionsDAGPtr & actions_dag,
        const std::string & function_name,
        const substrait::FunctionArgument & arg);
    const DB::ActionsDAG::Node * parseExpression(DB::ActionsDAGPtr actions_dag, const substrait::Expression & rel);
    const ActionsDAG::Node *
    toFunctionNode(ActionsDAGPtr actions_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args);
    // remove nullable after isNotNull
    void removeNullable(std::vector<String> require_columns, ActionsDAGPtr actionsDag);
    std::string getUniqueName(const std::string & name) { return name + "_" + std::to_string(name_no++); }

    static std::pair<DataTypePtr, Field> parseLiteral(const substrait::Expression_Literal & literal);
    void wrapNullable(std::vector<String> columns, ActionsDAGPtr actionsDag, std::map<std::string, std::string> & nullable_measure_names);

    static Aggregator::Params getAggregateParam(const Names & keys, const AggregateDescriptions & aggregates, const ContextPtr & context_)
    {
        const Settings & settings = context_->getSettingsRef();
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
            context_->getTempDataOnDisk(),
            settings.max_threads,
            settings.min_free_disk_space_for_temporary_data,
            true,
            3,
            settings.max_block_size,
            false,
            false);
    }

    static Aggregator::Params getMergedAggregateParam(const Names & keys, const AggregateDescriptions & aggregates)
    {
        Settings settings;
        return Aggregator::Params(keys, aggregates, false, settings.max_threads, settings.max_block_size);
    }

    IQueryPlanStep * addRemoveNullableStep(QueryPlan & plan, std::vector<String> columns);

    static std::pair<DB::DataTypePtr, DB::Field> convertStructFieldType(const DB::DataTypePtr & type, const DB::Field & field);

    int name_no = 0;
    std::unordered_map<std::string, std::string> function_mapping;
    std::vector<jobject> input_iters;
    ContextPtr context;
    // for parse rel node, collect steps from a rel node
    std::vector<IQueryPlanStep *> temp_step_collection;
    std::vector<RelMetricPtr> metrics;
    ContextPtr contextPtr;
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
    explicit LocalExecutor(QueryContext & _query_context, ContextPtr context);
    void execute(QueryPlanPtr query_plan);
    SparkRowInfoPtr next();
    Block * nextColumnar();
    bool hasNext();
    ~LocalExecutor();

    Block & getHeader();
    const RelMetricPtr getMetric() const { return metric; }
    void setMetric(RelMetricPtr metric_) { metric = metric_; }
    void setExtraPlanHolder(std::vector<QueryPlanPtr> & extra_plan_holder_)
    {
        extra_plan_holder = std::move(extra_plan_holder_);
    }

private:
    QueryContext query_context;
    std::unique_ptr<SparkRowInfo> writeBlockToSparkRow(DB::Block & block);
    bool checkAndSetDefaultBlock(size_t current_block_columns, bool has_next_blocks);
    QueryPipeline query_pipeline;
    std::unique_ptr<PullingPipelineExecutor> executor;
    Block header;
    ContextPtr context;
    std::unique_ptr<CHColumnToSparkRow> ch_column_to_spark_row;
    std::unique_ptr<SparkBuffer> spark_buffer;
    DB::QueryPlanPtr current_query_plan;
    RelMetricPtr metric;
    std::vector<QueryPlanPtr> extra_plan_holder;
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
