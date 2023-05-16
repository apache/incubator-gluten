#pragma once
#include <filesystem>
#include <Builder/BroadCastJoinBuilder.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Storages/IStorage.h>
#include <base/types.h>
#include <Common/logger_useful.h>

namespace local_engine
{
class BlockUtil
{
public:
    // Build a header block with a virtual column which will be
    // use to indicate the number of rows in a block.
    // Commonly seen in the following quries:
    // - select count(1) from t
    // - select 1 from t
    static DB::Block buildRowCountHeader();
    static DB::Chunk buildRowCountChunk(UInt64 rows);
    static DB::Block buildRowCountBlock(UInt64 rows);

    static DB::Block buildHeader(const DB::NamesAndTypesList & names_types_list);

    static constexpr UInt64 FLAT_STRUCT = 1;
    static constexpr UInt64 FLAT_NESTED_TABLE = 2;
    // flatten the struct and array(struct) columns.
    // It's different from Nested::flattend()
    static DB::Block flattenBlock(const DB::Block & block, UInt64 flags = FLAT_STRUCT | FLAT_NESTED_TABLE, bool recursively = false);
};

/// Use this class to extract element columns from columns of nested type in a block, e.g. named Tuple.
/// It can extract a column from a multiple nested type column, e.g. named Tuple in named Tuple
/// Keeps some intermediate data to avoid rebuild them multi-times.
class NestedColumnExtractHelper
{
public:
    explicit NestedColumnExtractHelper(const DB::Block & block_, bool case_insentive_);
    std::optional<DB::ColumnWithTypeAndName> extractColumn(const String & column_name);

private:
    std::optional<DB::ColumnWithTypeAndName>
    extractColumn(const String & original_column_name, const String & column_name_prefix, const String & column_name_suffix);
    const DB::Block & block;
    bool case_insentive;
    std::map<String, DB::BlockPtr> nested_tables;

    const DB::ColumnWithTypeAndName * findColumn(const DB::Block & block, const std::string & name) const;
};

class PlanUtil
{
public:
    static std::string explainPlan(DB::QueryPlan & plan);
};

class MergeTreeUtil
{
public:
    using Path = std::filesystem::path;
    static std::vector<Path> getAllMergeTreeParts(const Path & storage_path);
    static DB::NamesAndTypesList getSchemaFromMergeTreePart(const Path & part_path);
};

class ActionsDAGUtil
{
public:
    static const DB::ActionsDAG::Node * convertNodeType(
        DB::ActionsDAGPtr & actions_dag,
        const DB::ActionsDAG::Node * node,
        const std::string & type_name,
        const std::string & result_name = "");
};

class QueryPipelineUtil
{
public:
    static String explainPipeline(DB::QueryPipeline & pipeline);
};


class BackendFinalizerUtil;
class JNIUtils;
class BackendInitializerUtil
{
public:
    /// Initialize two kinds of resources
    /// 1. global level resources like global_context/shared_context, notice that they can only be initialized once in process lifetime
    /// 2. session level resources like settings/configs, they can be initialized multiple times following the lifetime of executor/driver
    static void init(const std::string & conf_plan);

private:
    friend class BackendFinalizerUtil;
    friend class JNIUtils;

    static void initConfig(const std::string & plan);
    static void initLoggers();
    static void initEnvs();
    static void initSettings();
    static void initContexts();
    static void applyConfigAndSettings();
    static void registerAllFactories();
    static void initCompiledExpressionCache();

    static std::map<std::string, std::string> getBackendConfMap(const std::string & plan);

    inline static const String CH_BACKEND_PREFIX = "spark.gluten.sql.columnar.backend.ch";

    inline static const String CH_RUNTIME_CONFIG = "runtime_config";
    inline static const String CH_RUNTIME_CONFIG_PREFIX = CH_BACKEND_PREFIX + "." + CH_RUNTIME_CONFIG + ".";
    inline static const String CH_RUNTIME_CONFIG_FILE = CH_RUNTIME_CONFIG_PREFIX + "config_file";

    inline static const String CH_RUNTIME_SETTINGS = "runtime_settings";
    inline static const String CH_RUNTIME_SETTINGS_PREFIX = CH_BACKEND_PREFIX + "." + CH_RUNTIME_SETTINGS + ".";

    inline static const String LIBHDFS3_CONF_KEY = "hdfs.libhdfs3_conf";
    inline static const String SETTINGs_PATH = "local_engine.settings";
    inline static const std::string SPARK_S3_ACCESS_KEY = "spark.hadoop.fs.s3a.access.key";
    inline static const std::string SPARK_S3_SECRET_KEY = "spark.hadoop.fs.s3a.secret.key";
    inline static const std::string SPARK_S3_ENDPOINT = "spark.hadoop.fs.s3a.endpoint";
    inline static const std::map<std::string, std::string> S3_CONFIGS
        = {{SPARK_S3_ACCESS_KEY, "s3.access_key_id"}, {SPARK_S3_SECRET_KEY, "s3.secret_access_key"}, {SPARK_S3_ENDPOINT, "s3.endpoint"}};

    inline static std::once_flag init_flag;
    inline static std::map<std::string, std::string> backend_conf_map;
    inline static DB::Context::ConfigurationPtr config;
    inline static Poco::Logger * logger;
    inline static DB::Settings settings;
};

class BackendFinalizerUtil
{
public:
    /// Release global level resources like global_context/shared_context. Invoked only once in the lifetime of process when JVM is shuting down.
    static void finalizeGlobally();

    /// Release session level resources like StorageJoinBuilder. Invoked every time executor/driver shutdown.
    static void finalizeSessionally();
};

}
