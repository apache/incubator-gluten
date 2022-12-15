# The Operators and Functions Support Progress
Although the Gluten project is still in active development, we still have made big progress on Operators and Functions support.
- The number of Operators supported is 12, the most common Operators are covered.
- The number of Functions supported is 94, and Velox supports 127 Functions.
- The number of Functions commonly used by Spark is 240, and the total number for Spark3.3 is 387.

Generally speaking, there are lots of work to do, but all 22 TPC-H queries can be offloaded to the native backend. The information
is presented separately according to the type of backend. The detail information is as follows:

## Backend-Velox

### Operator support
Support List
- TableScan
- Project
- Filter
- Aggregate: HashAggregate
- HashJoin: HashProbe and HashBuild
- GroupId
- OrderBy
- Limit/Offset
- MergeJoin
- Unnest
- Window

Unsupported List
- Values
- TableWrite
- CrossJoin
- TopN
- PartitionedOutput
- StreamingAggregate
- Exchange
- MergeExchange
- LocalMerge
- LocalPartition and LocalExchange
- PartitionedOutput
- EnforceSingleRow
- AssignUniqueId

### Function support
Wait to add


## Backed-Clickhouse
Wait to add

please keep this document updated when we make new progress.
