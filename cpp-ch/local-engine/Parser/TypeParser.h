#pragma once
#include <list>
#include <optional>
#include <unordered_map>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <substrait/plan.pb.h>

namespace local_engine
{
class TypeParser
{
public:
    TypeParser() = default;
    ~TypeParser() = default;

    static String getCHTypeName(const String & spark_type_name);
    static DB::DataTypePtr getCHTypeByName(const String & spark_type_name);
    /// When parsing named structure, we need the field names.
    static DB::DataTypePtr parseType(const substrait::Type & substrait_type, std::list<String> * field_names);
    inline static DB::DataTypePtr parseType(const substrait::Type & substrait_type)
    {
        return parseType(substrait_type, nullptr);
    }
    static DB::Block buildBlockFromNamedStruct(const substrait::NamedStruct & struct_);
    static bool isTypeMatched(const substrait::Type & substrait_type, const DB::DataTypePtr & ch_type);
private:
    /// Mapping spark type names to CH type names.
    static std::unordered_map<String, String> type_names_mapping;

    static DB::DataTypePtr tryWrapNullable(substrait::Type_Nullability nullable, DB::DataTypePtr nested_type);
};
}
