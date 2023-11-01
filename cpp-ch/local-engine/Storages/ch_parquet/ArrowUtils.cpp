#include "ArrowUtils.h"

namespace local_engine
{
parquet::internal::LevelInfo ComputeLevelInfo(const parquet::ColumnDescriptor * descr)
{
    parquet::internal::LevelInfo level_info;
    level_info.def_level = descr->max_definition_level();
    level_info.rep_level = descr->max_repetition_level();

    int16_t min_spaced_def_level = descr->max_definition_level();
    const ::parquet::schema::Node * node = descr->schema_node().get();
    while (node != nullptr && !node->is_repeated())
    {
        if (node->is_optional())
            min_spaced_def_level--;
        node = node->parent();
    }
    level_info.repeated_ancestor_def_level = min_spaced_def_level;
    return level_info;
}
}
