#pragma once
#include <unordered_map>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/WindowDescription.h>
#include <Parser/FunctionParser.h>
#include <Parser/RelParser.h>
#include <Parser/aggregate_function_parser/CommonAggregateFunctionParser.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace local_engine
{
class WindowRelParser : public RelParser
{
public:
    explicit WindowRelParser(SerializedPlanParser * plan_paser_);
    ~WindowRelParser() override = default;
    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;

private:
    struct WindowInfo
    {
        const substrait::WindowRel::Measure * measure = nullptr;
        String result_column_name;
        Strings arg_column_names;
        DB::DataTypes arg_column_types;
        DB::Array params;
        String signature_function_name;
        // function name in CH
        String function_name;
        // For avoiding repeated builds.
        FunctionParser::CommonFunctionInfo parser_func_info;
        // For avoiding repeated builds.
        FunctionParserPtr function_parser;

        google::protobuf::RepeatedPtrField<substrait::Expression> partition_exprs;
        google::protobuf::RepeatedPtrField<substrait::SortField> sort_fields;

    };
    DB::QueryPlanPtr current_plan;
    DB::Block input_header;
    // The final output schema.
    DB::Block output_header;
    Poco::Logger * logger = &Poco::Logger::get("WindowRelParser");
    std::vector<WindowInfo> win_infos;

    /// There will be window descrptions generated for different window frame type;
    std::unordered_map<DB::String, WindowDescription> parseWindowDescriptions();

    // Build a window description in CH with respect to a window function, since the same
    // function may have different window frame in CH and spark.
    DB::WindowDescription
    parseWindowDescription(const WindowInfo & win_info);
    DB::WindowFrame parseWindowFrame(const WindowInfo & win_info);
    DB::WindowFrame::FrameType
    parseWindowFrameType(const std::string & function_name, const substrait::Expression::WindowFunction & window_function);
    static void parseBoundType(
        const substrait::Expression::WindowFunction::Bound & bound,
        bool is_begin_or_end,
        DB::WindowFrame::BoundaryType & bound_type,
        Field & offset,
        bool & preceding);
    DB::SortDescription parsePartitionBy(const google::protobuf::RepeatedPtrField<substrait::Expression> & expressions);
    DB::WindowFunctionDescription parseWindowFunctionDescription(
        const String & ch_function_name,
        const substrait::Expression::WindowFunction & window_function,
        const DB::Names & arg_names,
        const DB::DataTypes & arg_types,
        const DB::Array & params);

    void initWindowsInfos(const substrait::WindowRel & win_rel);
    void tryAddProjectionBeforeWindow();
    void tryAddProjectionAfterWindow();
};


}
