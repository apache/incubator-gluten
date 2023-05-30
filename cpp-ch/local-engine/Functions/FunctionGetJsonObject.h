#pragma once
#include <memory>
#include <string_view>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionSQLJSON.h>
#include <Parsers/Lexer.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}
namespace local_engine
{
// We notice that, `get_json_object` have different behavior with `JSON_VALUE/JSON_QUERY`.
// - ('{"x":[{"y":1},{"y":2}]}' '$.x[*].y'), `json_value` return only one element, but `get_json_object` return
//   return a list.
// - ('{"x":[{"y":1}]}' '$.x[*].y'), `json_query`'s result is '[1]',
//   but `get_json_object`'s result is '1'
//


struct GetJsonOject
{
    static constexpr auto name{"get_json_object"};
};

template <typename JSONParser>
class GetJsonObjectImpl
{
public:
    using Element = typename JSONParser::Element;

    static DB::DataTypePtr getReturnType(const char *, const DB::ColumnsWithTypeAndName &, const DB::ContextPtr &)
    {
        auto nested_type = std::make_shared<DB::DataTypeString>();
        return std::make_shared<DB::DataTypeNullable>(nested_type);
    }

    static size_t getNumberOfIndexArguments(const DB::ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    bool insertResultToColumn(DB::IColumn & dest, const Element & root, DB::ASTPtr & query_ptr, const DB::ContextPtr &)
    {
        if (!(has_array_wildcard_flag & 0x01)) [[unlikely]]
        {
            setupArrayWildcardFlag(query_ptr);
        }
        DB::GeneratorJSONPath<JSONParser> generator_json_path(query_ptr);
        Element current_element = root;
        DB::VisitorStatus status;
        std::stringstream out; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        /// Create json array of results: [res1, res2, ...]
        bool success = false;
        size_t element_count = 0;
        out << "[";
        while ((status = generator_json_path.getNextItem(current_element)) != DB::VisitorStatus::Exhausted)
        {
            if (status == DB::VisitorStatus::Ok)
            {
                if (success)
                {
                    out << ", ";
                }
                success = true;
                element_count++;
                out << current_element.getElement();
            }
            else if (status == DB::VisitorStatus::Error)
            {
                /// ON ERROR
                /// Here it is possible to handle errors with ON ERROR (as described in ISO/IEC TR 19075-6),
                ///  however this functionality is not implemented yet
            }
            current_element = root;
        }
        out << "]";
        if (!success)
        {
            return false;
        }
        DB::ColumnNullable & col_str = assert_cast<DB::ColumnNullable &>(dest);
        auto output_str = out.str();
        std::string_view final_out_str;
        assert(elelement_count);
        if (element_count == 1)
        {
            std::string_view output_str_view(output_str.data() + 1, output_str.size() - 2);
            if (output_str_view.size() >= 2 && output_str_view.front() == '\"' && output_str_view.back() == '\"')
            {
                final_out_str = std::string_view(output_str_view.data() + 1, output_str_view.size() - 2);
            }
            else
                final_out_str = std::string_view(output_str);
        }
        else
        {
            final_out_str = std::string_view(output_str);
        }
        col_str.insertData(final_out_str.data(), final_out_str.size());
        return true;
    }
private:
    UInt8 has_array_wildcard_flag = 0;

    void setupArrayWildcardFlag(DB::ASTPtr & query_ptr)
    {
        has_array_wildcard_flag |= 0x01;
        const auto * path = query_ptr->as<DB::ASTJSONPath>();
        if (!path)
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid path");
        }
        const auto * query = path->jsonpath_query;

        for (const auto & child_ast : query->children)
        {
            if (auto * range_ast = typeid_cast<DB::ASTJSONPathRange *>(child_ast.get()))
            {
                if (range_ast->is_star)
                {
                    has_array_wildcard_flag |= 0x02;
                    break;
                }
                for (const auto & range : range_ast->ranges)
                {
                    if (range.first != range.second - 1)
                    {
                        has_array_wildcard_flag |= 0x02;
                        break;
                    }
                }
            }
            else if (typeid_cast<DB::ASTJSONPathStar *>(child_ast.get()))
            {
                has_array_wildcard_flag |= 0x02;
                break;
            }
        }
    }
};

}
