#pragma once

namespace local_engine
{

/// @param global_skip_access_check - skip access check regardless regardless
///                                   .skip_access_check config directive (used
///                                   for clickhouse-disks)
void registerGlutenDisks(bool global_skip_access_check);

}
