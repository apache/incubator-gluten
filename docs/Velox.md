## Velox

Currently, Gluten requires Velox being pre-compiled.
In general, please refer to [Velox Installation](https://github.com/facebookincubator/velox/blob/main/scripts/setup-ubuntu.sh) to install all the dependencies and compile Velox.
Gluten depends on this [Velox branch](https://github.com/rui-mo/velox/tree/velox_for_gazelle_jni).
The changes to Velox are planned to be upstreamed in the future.
In addition to that, there are several points worth attention when compiling Gluten with Velox.

Firstly, please note that all the Gluten required libraries should be compiled as **position independent code**.
That means, for static libraries, "-fPIC" option should be added in their compiling processes.

Currently, Gluten with Velox depends on below libraries:

Required static libraries are:

- fmt
- folly
- iberty

Required shared libraries are:

- glog
- double-conversion
- gtest
- snappy

Gluten will try to find above libraries from system lib paths.
If they are not installed there, please copy them to system lib paths,
or change the finding paths specified in [CMakeLists.txt](https://github.com/oap-project/gluten/blob/master/cpp/velox/CMakeLists.txt).

```shell script
set(SYSTEM_LIB_PATH "/usr/lib" CACHE PATH "System Lib dir")
set(SYSTEM_LIB64_PATH "/usr/lib64" CACHE PATH "System Lib64 dir")
set(SYSTEM_LOCAL_LIB_PATH "/usr/local/lib" CACHE PATH "System Local Lib dir")
set(SYSTEM_LOCAL_LIB64_PATH "/usr/local/lib64" CACHE PATH "System Local Lib64 dir")
```

Secondly, when compiling Velox, please note that Velox generated static libraries should also be compiled as position independent code.
Also, some OBJECT settings in CMakeLists are removed in order to acquire the static libraries.
These two changes have already been covered in the Velox branch Gluten depends on.

After Velox being successfully compiled, please refer to [GlutenUsage](GlutenUsage.md) and
use below command to compile Gluten with Velox backend.

```shell script
mvn clean package -P full-scala-compiler -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_velox=ON -Dvelox_home=${VELOX_HOME}
```

### An example for offloading Spark's computing to Velox with Gluten

TPC-H Q1 and Q6 are supported in Gluten using Velox as backend. Current support has below limitations: 

- Found Date and Long types in Velox's TableScan are not fully ready, 
so converted related columns into Double type.
- Metrics are missing.

#### Test TPC-H Q1 and Q6 on Gluten with Velox backend

##### Data preparation

Considering only Hive LRE V1 is supported in Velox, below Spark option was adopted when generating ORC data. 

```shell script
--conf spark.hive.exec.orc.write.format=0.11
```

Considering Velox's support for Decimal, Date, Long types are not fully ready, the related columns of TPC-H Q6 were all transformed into Double type.
Below script shows how to convert Parquet into ORC format, and transforming TPC-H Q6 related columns into Double type.
To align with this data type change, the TPC-H Q6 query was changed accordingly.  

```shell script
for (filePath <- fileLists) {
  val parquet = spark.read.parquet(filePath)
  val df = parquet.select(parquet.col("l_orderkey"), parquet.col("l_partkey"), parquet.col("l_suppkey"), parquet.col("l_linenumber"), parquet.col("l_quantity"), parquet.col("l_extendedprice"), parquet.col("l_discount"), parquet.col("l_tax"), parquet.col("l_returnflag"), parquet.col("l_linestatus"), parquet.col("l_shipdate").cast(TimestampType).cast(LongType).cast(DoubleType).divide(seconds_in_a_day).alias("l_shipdate_new"), parquet.col("l_commitdate").cast(TimestampType).cast(LongType).cast(DoubleType).divide(seconds_in_a_day).alias("l_commitdate_new"), parquet.col("l_receiptdate").cast(TimestampType).cast(LongType).cast(DoubleType).divide(seconds_in_a_day).alias("l_receiptdate_new"), parquet.col("l_shipinstruct"), parquet.col("l_shipmode"), parquet.col("l_comment"))
  val part_df = df.repartition(1)
  part_df.write.mode("append").format("orc").save(ORC_path)
}
```

##### Submit the Spark SQL job

The modified TPC-H Q6 query is:

```shell script
select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate_new >= 8766 and l_shipdate_new < 9131 and l_discount between .06 - 0.01 and .06 + 0.01 and l_quantity < 24
```

The modified TPC-H Q6 query is:
```shell script
select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate_new <= 10471 group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus
```

Below script shows how to read the ORC data, and submit the modified TPC-H Q6 query.

cat tpch_q6.scala
```shell script
val lineitem = spark.read.format("orc").load("file:///mnt/lineitem_orcs")
lineitem.createOrReplaceTempView("lineitem")
// The modified TPC-H Q6 query
time{spark.sql("select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate_new >= 8766 and l_shipdate_new < 9131 and l_discount between .06 - 0.01 and .06 + 0.01 and l_quantity < 24").show}
```

Submit test script from spark-shell.

```shell script
cat tpch_q6.scala | spark-shell --name tpch_velox_q6 --master yarn --deploy-mode client --conf spark.plugins=io.glutenproject.GlutenPlugin --conf --conf spark.gluten.sql.columnar.backend.lib=velox --conf spark.driver.extraClassPath=${gluten_jvm_jar} --conf spark.executor.extraClassPath=${gluten_jvm_jar} --conf spark.memory.offHeap.size=20g --conf spark.sql.sources.useV1SourceList=avro --num-executors 6 --executor-cores 6 --driver-memory 20g --executor-memory 25g --conf spark.executor.memoryOverhead=5g --conf spark.driver.maxResultSize=32g
```

##### Result

![TPC-H Q6](./image/TPC-H_Q6_DAG.png)

##### Performance

Below table shows the TPC-H Q1 and Q6 Performance in a multiple-thread test (--num-executors 6 --executor-cores 6) for Velox and vanilla Spark.
Both Parquet and ORC datasets are sf1024.

| Query Performance (s) | Velox (ORC) | Vanilla Spark (Parquet) | Vanilla Spark (ORC) |
|---------------- | ----------- | ------------- | ------------- |
| TPC-H Q6 | 13.6 | 21.6  | 34.9 |
| TPC-H Q1 | 26.1 | 76.7 | 84.9 |
