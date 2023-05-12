#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace local_engine
{
enum ExpandFieldKind
{
    EXPAND_FIELD_KIND_SELECTION,
    EXPAND_FIELD_KIND_LITERAL,
};

class ExpandField
{
public:
    ExpandField() = default;
    ExpandField(
        const std::vector<std::string> & names_,
        const std::vector<DB::DataTypePtr> & types_,
        const std::vector<std::vector<ExpandFieldKind>> & kinds_,
        const std::vector<std::vector<DB::Field>> & fields_)
        : names(names_), types(types_), kinds(kinds_), fields(fields_)
    {
    }

    const std::vector<std::string> & getNames() const { return names; }
    const std::vector<DB::DataTypePtr> & getTypes() const { return types; }
    const std::vector<std::vector<ExpandFieldKind>> & getKinds() const { return kinds; }
    const std::vector<std::vector<DB::Field>> & getFields() const { return fields; }

    size_t getExpandRows() const { return kinds.size(); }
    size_t getExpandCols() const { return types.size(); }

private:
    std::vector<std::string> names;
    std::vector<DB::DataTypePtr> types;
    std::vector<std::vector<ExpandFieldKind>> kinds;
    std::vector<std::vector<DB::Field>> fields;
};

}
