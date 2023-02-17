# Gluten Integration Testing (gluten-it)

The project makes it easy to test Gluten build locally.

## Gluten ?

Gluten is a native Spark SQL implementation as a standard Spark plug-in.

https://github.com/oap-project/gluten

## Getting Started

### 1. Install Gluten in your local machine

See offical Gluten build guidance https://github.com/oap-project/gluten#how-to-use-gluten

### 2. Install and run gluten-it

```sh
cd gluten/tools/gluten-it
mvn clean package
java -Xmx5G -cp target/gluten-it-1.0-SNAPSHOT-jar-with-dependencies.jar io.glutenproject.integration.tpc.Tpc
```

## Usage

### CMD args

```
Usage: gluten-tpc [-hV] [--disable-aqe] [--disable-bhj] [--disable-wscg]
                  [--enable-history] [--enable-ui] [--error-on-memleak]
                  [--explain] [--fixed-width-as-double]
                  [--gen-partitioned-data] [--min-scan-partitions]
                  [--skip-data-gen] [-b=<backendType>]
                  [--baseline-backend-type=<baselineBackendType>]
                  [--benchmark-type=<benchmarkType>] [--cpus=<cpus>]
                  [--history-ui-port=<hsUiPort>] [--iterations=<iterations>]
                  [--log-level=<logLevel>] [--mode=<mode>]
                  [--off-heap-size=<offHeapSize>] [-s=<scale>]
                  [--shuffle-partitions=<shufflePartitions>]
                  [--conf=<String=String>]... [--queries=<queries>[,
                  <queries>...]]...
Gluten integration test using TPC benchmark's data and queries
  -b, --backend-type=<backendType>
                           Backend used: vanilla, velox, gazelle-cpp, ...
                             Default: velox
      --baseline-backend-type=<baselineBackendType>
                           Baseline backend used: vanilla, velox, gazelle-cpp,
                             ...
                             Default: vanilla
      --benchmark-type=<benchmarkType>
                           TPC benchmark type: h, ds
                             Default: h
      --conf=<String=String>
                           Test line Spark conf, --conf=k1=v1 --conf=k2=v2
      --cpus=<cpus>        Executor cpu number
                             Default: 2
      --disable-aqe        Disable Spark SQL adaptive query execution
      --disable-bhj        Disable Spark SQL broadcast hash join
      --disable-wscg       Disable Spark SQL whole stage code generation
      --enable-history     Start a Spark history server during running
      --enable-ui          Enable Spark UI
      --error-on-memleak   Fail the test when memory leak is detected by
                             Spark's memory manager
      --explain            Output explain result for queries
      --fixed-width-as-double
                           Generate integer/long/date as double
      --gen-partitioned-data
                           Generate data with partitions
  -h, --help               Show this help message and exit.
      --history-ui-port=<hsUiPort>
                           Port that Spark history server UI binds to
                             Default: 18080
      --iterations=<iterations>
                           How many iterations to run
                             Default: 1
      --log-level=<logLevel>
                           Set log level: 0 for DEBUG, 1 for INFO, 2 for WARN
                             Default: 2
      --min-scan-partitions
                           Use minimum number of partitions to read data
      --mode=<mode>        Mode: data-gen-only, queries, queries-compare, spark-shell
                             Default: queries-compare
      --off-heap-size=<offHeapSize>
                           Off heap memory size per executor
                             Default: 6g
      --queries=<queries>[,<queries>...]
                           Set a comma-seperated list of query IDs to run, run
                             all queries if not specified. Example:
                             --queries=q1,q6
                             Default: __all__
  -s, --scale=<scale>      The scale factor of sample TPC-H dataset
                             Default: 0.1
      --shuffle-partitions=<shufflePartitions>
                           Generate data with partitions
                             Default: 100
      --skip-data-gen      Skip data generation
  -V, --version            Print version information and exit.
```
