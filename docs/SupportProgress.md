# The Operators and Functions Support Progress
Although the Gluten project is still in active development, we still have made big progress on Operators and Functions support.

The total supported functions' number for [Spark3.3 is 387](https://spark.apache.org/docs/latest/api/sql/) and for [Velox is 204](https://facebookincubator.github.io/velox/functions/coverage.html). 
Gluten supported frequently used 94, in which offloaded 62 is implemented in velox/spark and 32 in velox/presto, shown as below picture.
![support](./docs/image/support.png)

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
- Unnest

### Function support
Wait to add


## Backed-Clickhouse
Wait to add

please keep this document updated when we make new progress.
