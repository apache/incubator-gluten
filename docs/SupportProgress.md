# The Operators and Functions Support Progress
Although the Gluten project is still in active development, we still have made big progress on Operators and Functions support.
- The number of Operators supported is 12, the most common Operators are covered.
- The number of Functions supported is 94, and Velox supports 127 Functions.
- The number of Functions commonly used by Spark is 240, and the total number for Spark3.3 is 387.

Generally speaking, there are lots of work to do, but all 22 TPC-H queries can be offloaded to the native backend. the information
is presented separately according to the type of backend. the detail information is as follows:

## Backend-Velox

### Operator support
Support List
- TableScan
- Values
- Project
- Filter
- Aggregate: HashAggregate or StreamingAggregate
- GroupId
- HashJoin: HashProbe and HashBuild
- OrderBy
- Limit/Offset
- TopN
- Window

Unsupported List
- Unnest
- MergeJoin
- CrossJoin
- MergeJoin
- PartitionedOutput
- AssignUniqueId
- Exchange
- MergeExchange
- LocalMerge
- LocalPartition and LocalExchange
- TableWrite
- PartitionedOutput
- EnforceSingleRow

### Function support
Wait to add


## Backed-Clickhouse
Wait to add

please keep this document updated when we make new progress.
