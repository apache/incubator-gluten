#include "CollectListParser.h"
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ActionsDAG.h>

namespace local_engine
{
AggregateFunctionParserRegister<CollectListParser> register_collect_list;
AggregateFunctionParserRegister<CollectSetParser> register_collect_set;
}
