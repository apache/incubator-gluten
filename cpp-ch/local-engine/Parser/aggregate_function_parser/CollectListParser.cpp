#include "CollectListParser.h"
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ActionsDAG.h>

namespace local_engine
{
FunctionParserRegister<CollectListParser> register_collect_list;
FunctionParserRegister<CollectSetParser> register_collect_set;
}
