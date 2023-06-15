#include "schema.h"
#include <Common/Exception.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static bool is_group(const parquet::format::SchemaElement& schema) {
    return schema.num_children > 0;
}

//static bool is_list(const parquet::format::SchemaElement& schema) {
//    return schema.__isset.converted_type && schema.converted_type == parquet::format::ConvertedType::LIST;
//}

//static bool is_map(const parquet::format::SchemaElement& schema) {
//    return schema.__isset.converted_type && (schema.converted_type == parquet::format::ConvertedType::MAP ||
//                                             schema.converted_type == parquet::format::ConvertedType::MAP_KEY_VALUE);
//}

static bool is_repeated(const parquet::format::SchemaElement& schema) {
    return schema.__isset.repetition_type && schema.repetition_type == parquet::format::FieldRepetitionType::REPEATED;
}

//static bool is_required(const parquet::format::SchemaElement& schema) {
//    return schema.__isset.repetition_type && schema.repetition_type == parquet::format::FieldRepetitionType::REQUIRED;
//}

static bool schema_is_optional(const parquet::format::SchemaElement& schema) {
    return schema.__isset.repetition_type && schema.repetition_type == parquet::format::FieldRepetitionType::OPTIONAL;
}

//static int schema_num_children(const parquet::format::SchemaElement& schema) {
//    return schema.__isset.num_children ? schema.num_children : 0;
//}

void Schema::fromThrift(const std::vector<parquet::format::SchemaElement> & t_schemas)
{
    if (t_schemas.size() == 0)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty parquet Schema");
    }
    auto & root_schema = t_schemas[0];
    if (!is_group(root_schema))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Root Schema is not group");
    }
    fields.resize(root_schema.num_children);
    // skip root SchemaElement
    size_t next_pos = 1;
    for (int i = 0; i < root_schema.num_children; ++i)
    {
        nodeToField(t_schemas, next_pos, LevelInfo(), &fields[i], &next_pos);
        if (field_by_name.find(fields[i].name) != field_by_name.end())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate field name: {}", fields[i].name);
        }
        field_by_name.emplace(fields[i].name, &fields[i]);
    }

    if (next_pos != t_schemas.size())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Remaining {} unparsed field", t_schemas.size() - next_pos);
    }
}

void Schema::getFieldNames(std::unordered_set<std::string>& names, bool case_sensitive) const {
    names.clear();
    for (const ParquetField& f : fields) {
        std::string name = case_sensitive ? f.name : Poco::toLower(f.name);
        names.emplace(std::move(name));
    }
}

size_t Schema::getColumnIndex(const std::string& column, bool case_sensitive) const {
    for (size_t i = 0; i < fields.size(); i++) {
        bool found =
            case_sensitive ? fields[i].name == column : strcasecmp(fields[i].name.c_str(), column.c_str()) == 0;
        if (found) {
            return i;
        }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "column {} not found in parquet", column);
}

void Schema::nodeToField(const std::vector<parquet::format::SchemaElement>& t_schemas, size_t pos,
                                     LevelInfo cur_level_info, ParquetField* field, size_t* next_pos) {
    if (pos >= t_schemas.size()) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Access out-of-bounds SchemaElement");
    }
    auto& t_schema = t_schemas[pos];
    if (is_group(t_schema)) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "group not supported");
    }
    if (is_repeated(t_schema)) {

        throw Exception(ErrorCodes::LOGICAL_ERROR, "repeated not supported");
        
    } else {
        // required int
        // or
        // optional int
        bool is_optional = schema_is_optional(t_schema);
        if (is_optional) {
            cur_level_info.max_def_level++;
        }
        leafToField(t_schema, cur_level_info, is_optional, field);
        *next_pos = pos + 1;
    }
}

void Schema::leafToField(const parquet::format::SchemaElement& t_schema, const LevelInfo& cur_level_info,
                                     bool is_nullable, ParquetField* field) {
    field->name = t_schema.name;
    field->schema_element = t_schema;
    field->is_nullable = is_nullable;
    field->physical_type = t_schema.type;
    field->type_length = t_schema.type_length;
    field->scale = t_schema.scale;
    field->precision = t_schema.precision;
    field->level_info = cur_level_info;
    // TODO fied->type
    physical_fields.push_back(field);
    field->physical_column_index = physical_fields.size() - 1;
}

std::string LevelInfo::debugString() const {
    std::stringstream ss;
    ss << "LevelInfo(maxDefLevel=" << max_def_level << ",maxRepLevel=" << max_rep_level
       << ",immediate_repeated_ancestor_def_level=" << immediate_repeated_ancestor_def_level << ")";
    return ss.str();
}

std::string ParquetField::debugString() const {
    std::stringstream ss;
    ss << "ParquetField(name=" << name << ",type=" << type->getName() << ",physical_type=" << physical_type
       << ",physical_column_index=" << physical_column_index << ",levels_info=" << level_info.debugString();
    if (children.size() > 0) {
        ss << ",children=[";
        for (size_t i = 0; i < children.size(); ++i) {
            if (i != 0) {
                ss << ",";
            }
            ss << children[i].debugString();
        }
        ss << "]";
    }
    ss << ")";
    return ss.str();
}


std::string Schema::debugString() const {
    std::stringstream ss;
    ss << "fields=[";
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        ss << fields[i].debugString();
    }
    ss << "]";
    return ss.str();
}
}
