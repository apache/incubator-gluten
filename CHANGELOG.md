# Change log
Generated on 2023-03-22

## Release 0.5.0

### Features
|||
|:---|:---|
|[#1131](https://github.com/oap-project/gluten/issues/1131)|[Gluten-core] Add an option to only fallback once|
|[#1165](https://github.com/oap-project/gluten/issues/1165)|Reduce GC Time when executing BHJ for CH backend.|
|[#1147](https://github.com/oap-project/gluten/issues/1147)|[Gluten-core]Make validate failure logLevel configuable|
|[#1112](https://github.com/oap-project/gluten/issues/1112)|Refactor Gluten metrics and add apis for each backend|
|[#926](https://github.com/oap-project/gluten/issues/926)|gluten timezone not the same as backend|
|[#1039](https://github.com/oap-project/gluten/issues/1039)|Remove compute pid metric in shuffle operator.|
|[#882](https://github.com/oap-project/gluten/issues/882)|Selective query execution|
|[#959](https://github.com/oap-project/gluten/issues/959)|Upgrade Arrow version to 11.0.0|
|[#969](https://github.com/oap-project/gluten/issues/969)|Docker for gluten running on centos 8|
|[#986](https://github.com/oap-project/gluten/issues/986)|Align and enrich metrics compare to Spark|
|[#972](https://github.com/oap-project/gluten/issues/972)|Can we separate native dynamic library from build generated jars?|
|[#913](https://github.com/oap-project/gluten/issues/913)|No Spark Shim Provider found for 3.2.0|
|[#853](https://github.com/oap-project/gluten/issues/853)|Support named struct type|
|[#888](https://github.com/oap-project/gluten/issues/888)|Clickhouse backend broadcast relation support r2c|
|[#850](https://github.com/oap-project/gluten/issues/850)|Add cast check in ExpressionTransformer|
|[#825](https://github.com/oap-project/gluten/issues/825)|Setup development environment for macOS|
|[#788](https://github.com/oap-project/gluten/issues/788)|Pass needed hadoop conf from driver to executor|
|[#773](https://github.com/oap-project/gluten/issues/773)|Partial enable GlutenSubquerySuite for ClickHouse Backend|
|[#692](https://github.com/oap-project/gluten/issues/692)|Support spark check_overflow function|
|[#663](https://github.com/oap-project/gluten/issues/663)|Support spliting partition by files count strategy for ClickHouse Backend (experimental)|
|[#664](https://github.com/oap-project/gluten/issues/664)|Remove unused NativeWholeStageRowRDD|
|[#665](https://github.com/oap-project/gluten/issues/665)|Log substrait plan in json format when generating GlutenPartition|
|[#666](https://github.com/oap-project/gluten/issues/666)|Remove the unused parameter 'spark.gluten.sql.columnar.loadnative' |
|[#638](https://github.com/oap-project/gluten/issues/638)|suggestions for compiling and installing |
|[#500](https://github.com/oap-project/gluten/issues/500)|Analyze S3 connector performance for Velox|
|[#517](https://github.com/oap-project/gluten/issues/517)|Build Velox from its folder|
|[#516](https://github.com/oap-project/gluten/issues/516)|Build relase with debug info|
|[#530](https://github.com/oap-project/gluten/issues/530)|Using spotless to format java source code automatically|
|[#401](https://github.com/oap-project/gluten/issues/401)|Support more data types in gluten and clickhouse backend: Decimal/Struct/Map/Array|
|[#378](https://github.com/oap-project/gluten/issues/378)|[backend-CK] Support map type for clickhouse backend|
|[#460](https://github.com/oap-project/gluten/issues/460)|Support adding external SparkSessionExtensions after GlutenSessionExtensions|
|[#388](https://github.com/oap-project/gluten/issues/388)|Support to read dwrf format file from hdfs while use velox backend|
|[#158](https://github.com/oap-project/gluten/issues/158)|HDFS Support|
|[#414](https://github.com/oap-project/gluten/issues/414)|Manage not supported test case in a centralized way|
|[#420](https://github.com/oap-project/gluten/issues/420)|Support for Java 17|
|[#155](https://github.com/oap-project/gluten/issues/155)|Does gluten support other TPC-H SQL more than Q1/Q6 ?|
|[#137](https://github.com/oap-project/gluten/issues/137)|Add a velox script to help users build velox from the source(git clone)|
|[#303](https://github.com/oap-project/gluten/issues/303)|Support Spark DPP|
|[#319](https://github.com/oap-project/gluten/issues/319)|Support to read from partitioned table|
|[#302](https://github.com/oap-project/gluten/issues/302)|Add a parameter 'spark.gluten.sql.columnar.separate.scan.rdd.for.ch' to control whether to separate scan rdd for ClickHouse Backend.|
|[#279](https://github.com/oap-project/gluten/issues/279)|Separate Scan Operator as  independent node for ClickHouse Backend and add metrics.|
|[#254](https://github.com/oap-project/gluten/issues/254)|Support Dynamic Pruning Filters and Spark FileScanRDD and DataSourceRDD|
|[#219](https://github.com/oap-project/gluten/issues/219)|Support CH backend unittest workflow|
|[#234](https://github.com/oap-project/gluten/issues/234)|Remove CPU type limitation|
|[#229](https://github.com/oap-project/gluten/issues/229)|Support Spark SortShuffleManager for ClickHouse Backend|
|[#228](https://github.com/oap-project/gluten/issues/228)|Use config from spark to control batch size in Velox table scan|
|[#207](https://github.com/oap-project/gluten/issues/207)|Support MergeTree DataSource V1 for ClickHouse backend|
|[#196](https://github.com/oap-project/gluten/issues/196)|Support Native ColumnarToRowExec on ClickHouse backend|
|[#171](https://github.com/oap-project/gluten/issues/171)|Support case_when, InSet, Extract expression|
|[#167](https://github.com/oap-project/gluten/issues/167)|Fallback ShuffleExchangeExec with RangePartitioning for ClickHouse backend.|
|[#147](https://github.com/oap-project/gluten/issues/147)|Update Velox|
|[#140](https://github.com/oap-project/gluten/issues/140)|Separate each backend as one module|

### Bugs Fixed
|||
|:---|:---|
|[#1179](https://github.com/oap-project/gluten/issues/1179)|[VL] CI is failing on boost's checksum|
|[#1162](https://github.com/oap-project/gluten/issues/1162)|[CK]fix CoaleseBatches metrics|
|[#1124](https://github.com/oap-project/gluten/issues/1124)|Memory management not suitable with Velox split preload feature.|
|[#1149](https://github.com/oap-project/gluten/issues/1149)|Run tpc-ds core|
|[#741](https://github.com/oap-project/gluten/issues/741)|Handle remainder for the case that its right input is zero|
|[#772](https://github.com/oap-project/gluten/issues/772)|Velox does not install folly in centos8 by default, break compile in centos8.|
|[#789](https://github.com/oap-project/gluten/issues/789)|Jar conflicts on Arrow and Protobuf between Vanilla Spark and Gluten|
|[#700](https://github.com/oap-project/gluten/issues/700)|AARCH64 port of Gluten|
|[#1027](https://github.com/oap-project/gluten/issues/1027)|[VL] unsupported method|
|[#489](https://github.com/oap-project/gluten/issues/489)|cannot build gluten (velox backend) in Amazon Linux 2|
|[#1012](https://github.com/oap-project/gluten/issues/1012)|Enable local cache throw exception|
|[#995](https://github.com/oap-project/gluten/issues/995)|Fix memory leak for ClickHouse Backend|
|[#914](https://github.com/oap-project/gluten/issues/914)|System variables related to Folly could not be found when compiling gluten.|
|[#990](https://github.com/oap-project/gluten/issues/990)|Failed to build velox|
|[#946](https://github.com/oap-project/gluten/issues/946)|Upgrade arrow version to 10.0.1|
|[#860](https://github.com/oap-project/gluten/issues/860)|CH backend inset result not equals spark result|
|[#601](https://github.com/oap-project/gluten/issues/601)|Can't decide data type of null value in gluten test framework, when transforming InteralRow to DataFrame|
|[#843](https://github.com/oap-project/gluten/issues/843)|Unable to convert BHJ to SHJ by using hint|
|[#826](https://github.com/oap-project/gluten/issues/826)|ch_backend not support inset is empty |
|[#815](https://github.com/oap-project/gluten/issues/815)|Gluten + Velox backend does not support Struct dataset with same element name.|
|[#563](https://github.com/oap-project/gluten/issues/563)|Error compiling within -Pbackends-xx,spark-3.3,spark-ut|
|[#560](https://github.com/oap-project/gluten/issues/560)|An unsupportedOperationException interrupted the query execution|
|[#770](https://github.com/oap-project/gluten/issues/770)|VeloxRuntimeError when reading parquet file with only meta data|
|[#800](https://github.com/oap-project/gluten/issues/800)|[UT]ExpectedAnswer may not match SparkAnswer when is sorted|
|[#676](https://github.com/oap-project/gluten/issues/676)|WholeStageTransformerSuite#logForFailedTest() swallows exceptions|
|[#790](https://github.com/oap-project/gluten/issues/790)|Join RuntimeException when having duplicated equal-join keys|
|[#797](https://github.com/oap-project/gluten/issues/797)|It won't  load the libparquet.so.1000  when we use Gluten with Velox backend and run it on the yarn.|
|[#784](https://github.com/oap-project/gluten/issues/784)|No Spark Shim Provider found for 3.3.0|
|[#547](https://github.com/oap-project/gluten/issues/547)|Jar conflict issue|
|[#727](https://github.com/oap-project/gluten/issues/727)|build from local velox repo doesn't work|
|[#694](https://github.com/oap-project/gluten/issues/694)|setup_ubuntu is executed twice|
|[#641](https://github.com/oap-project/gluten/issues/641)|Undefine symbol in libvelox.so|
|[#650](https://github.com/oap-project/gluten/issues/650)|Error java.lang.ClassCastException: scala.Tuple8 cannot be cast to scala.Tuple7 gluten + velox + spark-3.3.1 on latest main|
|[#633](https://github.com/oap-project/gluten/issues/633)|Shim error when running with EMR Spark 3.2.1-amzn-0|
|[#642](https://github.com/oap-project/gluten/issues/642)|Shim error when running with EMR Spark 3.3.0-amzn-0|
|[#654](https://github.com/oap-project/gluten/issues/654)|An exception occurs when executing Gluten's built-in tpcds script|
|[#615](https://github.com/oap-project/gluten/issues/615)|[compile bug] there is a compilation error cased by bridge.h|
|[#623](https://github.com/oap-project/gluten/issues/623)|TPCDS query execution fails|
|[#562](https://github.com/oap-project/gluten/issues/562)|Issue on building with Velox (from source code)|
|[#593](https://github.com/oap-project/gluten/issues/593)|Build fails on velox_memory_pool.cc|
|[#570](https://github.com/oap-project/gluten/issues/570)|compiler failture due to boots lib version|
|[#484](https://github.com/oap-project/gluten/issues/484)|Worker node throws exception trying to use protobuf|
|[#538](https://github.com/oap-project/gluten/issues/538)|Jackson databind missing from gluten jar|
|[#507](https://github.com/oap-project/gluten/issues/507)|Fix 'WrappedArray$ofRef; local class incompatible: stream classdesc' error|
|[#483](https://github.com/oap-project/gluten/issues/483)|gluten jar doesn't include libhdfs3 dependency|
|[#466](https://github.com/oap-project/gluten/issues/466)|Build failed: You have to use a classifier to attach supplemental artifacts to the project instead of replacing them.|
|[#349](https://github.com/oap-project/gluten/issues/349)|[backend-Velox] Use tpch_convert_parquet_dwrf.sh to convert parquet but cause exception 'double free or corruption (!prev)'|
|[#434](https://github.com/oap-project/gluten/issues/434)|[backend-?] Different results of the same parquet data |
|[#315](https://github.com/oap-project/gluten/issues/315)|[backend-Velox] Use tpch_datagen_parquet.sh to generate parquet data for tpch failed. |
|[#422](https://github.com/oap-project/gluten/issues/422)|Using wrong symbol for ARM64 throws error while building Arrow|
|[#443](https://github.com/oap-project/gluten/issues/443)|When rebuilding velox, the build pauses and asks if it's okay to replace protobuf|
|[#438](https://github.com/oap-project/gluten/issues/438)|Avoid to get all Partitions for each BasicScanTransformer every time|
|[#424](https://github.com/oap-project/gluten/issues/424)|Gluten has maven deps on packages that don't exist|
|[#426](https://github.com/oap-project/gluten/issues/426)|Build produces an empty jar, but it's not an error|
|[#368](https://github.com/oap-project/gluten/issues/368)|Refresh the README for Spark3.2 support and Performance Result Update|
|[#392](https://github.com/oap-project/gluten/issues/392)|Build failed , can't find the latest arrow jars dependency in public maven repo.|
|[#419](https://github.com/oap-project/gluten/issues/419)|Documentation: packages missing in Velox instructions installation|
|[#417](https://github.com/oap-project/gluten/issues/417)|Documentation: Not clear libhdfs3 installation instruction|
|[#418](https://github.com/oap-project/gluten/issues/418)|Documentation: Spark version not consistent across instructions|
|[#396](https://github.com/oap-project/gluten/issues/396)|Build uses "apt" while instructions use "yum"|
|[#410](https://github.com/oap-project/gluten/issues/410)|Can not get the config 'spark.gluten.sql.columnar.libpath' when executing task for ClickHouse backend|
|[#399](https://github.com/oap-project/gluten/issues/399)|Build failed: Failed to find name hashes for io.substrait.proto.ReadRel|
|[#277](https://github.com/oap-project/gluten/issues/277)|GHA report "Error: No space left on device" for velox backend testing|
|[#296](https://github.com/oap-project/gluten/issues/296)|A metrics-related error running DEBUG build|
|[#343](https://github.com/oap-project/gluten/issues/343)|Fix wrong results when executing avg(int), avg(long), avg(boolean) for ClickHouse backend|
|[#337](https://github.com/oap-project/gluten/issues/337)|result mismatch when dataset has null values|
|[#330](https://github.com/oap-project/gluten/issues/330)|Fix 'Method getXXX is not supported for Nullable(XXX)' when executing TPCH Q9 and add ut|
|[#324](https://github.com/oap-project/gluten/issues/324)|Fix nullable issues for ClickHouse Backend|
|[#309](https://github.com/oap-project/gluten/issues/309)|Fix fallback issue when executing 'select count(*)' or 'select count(1)'|
|[#307](https://github.com/oap-project/gluten/issues/307)|Fix NPE in JniResourceHelper.extractHeaders0|
|[#301](https://github.com/oap-project/gluten/issues/301)|Correct the scan time of the metrics|
|[#287](https://github.com/oap-project/gluten/issues/287)|One typo error for speedup description in README|
|[#276](https://github.com/oap-project/gluten/issues/276)|VeloxWholeStageTransformerSuite testcase failed.|
|[#274](https://github.com/oap-project/gluten/issues/274)|The dependency target "arrow" of target "velox_core" does not exist|
|[#253](https://github.com/oap-project/gluten/issues/253)|Fix merge operators issue when executing with DS V2 and there is only one partition|
|[#244](https://github.com/oap-project/gluten/issues/244)|Remove ‘testFailureIgnore’ = true and fix some memory leak for ClickHouse Backend|
|[#240](https://github.com/oap-project/gluten/issues/240)|Error while running DEBUG build of Gluten|
|[#194](https://github.com/oap-project/gluten/issues/194)|Arrow backend is broken|
|[#190](https://github.com/oap-project/gluten/issues/190)|Fix shuffle issues for ClickHouse Backend.|
|[#161](https://github.com/oap-project/gluten/issues/161)|The children of BaseJoinExec should insert WholeStageTransformerExec separately|
|[#153](https://github.com/oap-project/gluten/issues/153)|Fix NPE when executing with ClickHouse backend after PR#149 merged|
|[#145](https://github.com/oap-project/gluten/issues/145)|Library loading depends on release directory|
|[#133](https://github.com/oap-project/gluten/issues/133)|The parameter 'build_arrow' in pom.xml is invalid.|

### PRs
|||
|:---|:---|
|[#1177](https://github.com/oap-project/gluten/pull/1177)|[VL] avoid clean package/target/*.jar when call mvn clean|
|[#1184](https://github.com/oap-project/gluten/pull/1184)|[VL] Allow boolean scan|
|[#1186](https://github.com/oap-project/gluten/pull/1186)|[VL] make the VeloxBackend's data & functions more readable|
|[#1182](https://github.com/oap-project/gluten/pull/1182)|[VL] Fix typo in doc|
|[#1109](https://github.com/oap-project/gluten/pull/1109)|[Gluten-core] Introduce a whole stage fallback strategy|
|[#1154](https://github.com/oap-project/gluten/pull/1154)|[VL] Add centos8 on Velox CI|
|[#1170](https://github.com/oap-project/gluten/pull/1170)|[Gluten-core] Change some variables' name|
|[#1110](https://github.com/oap-project/gluten/pull/1110)|[WIP][document] Update logo and trademark info|
|[#1171](https://github.com/oap-project/gluten/pull/1171)|[VL] Fix spark 3.3 build broken|
|[#1020](https://github.com/oap-project/gluten/pull/1020)|[CH-313] support functions position/locate|
|[#1139](https://github.com/oap-project/gluten/pull/1139)|[CH-339] Support collecting metrics from CH backend Part One|
|[#1075](https://github.com/oap-project/gluten/pull/1075)|[CH-328] Support function isNaN|
|[#1167](https://github.com/oap-project/gluten/pull/1167)|[VL] Optimize nativeMake in JniWrapper|
|[#1168](https://github.com/oap-project/gluten/pull/1168)|[Gluten-core] Reuse ByteString in ExpressionNode.toProtobuf to avoid OOM|
|[#1166](https://github.com/oap-project/gluten/pull/1166)|[Gluten-core] Avoid printing stack traces with excessive depth|
|[#1169](https://github.com/oap-project/gluten/pull/1169)|[Gluten-1165] Reduce GC Time when executing BHJ for CH backend.|
|[#1138](https://github.com/oap-project/gluten/pull/1138)|[CH] Make bufferSize of IteratorOptions configurable|
|[#1155](https://github.com/oap-project/gluten/pull/1155)|[CI] GHA code style checking runs upon ignored cpp source folders|
|[#1151](https://github.com/oap-project/gluten/pull/1151)|[VL] Optimize the method of obtaining taskAttemptId in nativeMake|
|[#1159](https://github.com/oap-project/gluten/pull/1159)|[Gluten-core] Add a general trait for gluten plan to extend|
|[#1163](https://github.com/oap-project/gluten/pull/1163)|[Gluten-1162][CK]fix CoaleseBatches metrics: concatTime and collectTime|
|[#1125](https://github.com/oap-project/gluten/pull/1125)|[Gluten-1124] Close connector preload split on current memory management.|
|[#1157](https://github.com/oap-project/gluten/pull/1157)|[VL] Add Celeborn support in doc|
|[#1158](https://github.com/oap-project/gluten/pull/1158)|[UT] add Orc Test with arrow::adapters::orc|
|[#1156](https://github.com/oap-project/gluten/pull/1156)|[VL] Support offline build|
|[#1152](https://github.com/oap-project/gluten/pull/1152)|[Gluten-core] Remove std::cout in cpp code|
|[#1148](https://github.com/oap-project/gluten/pull/1148)|[Gluten-1147][Gluten-core] Make validate failure logLevel configuable|
|[#1137](https://github.com/oap-project/gluten/pull/1137)|[VL] Optimize column batch load in NativeRowToColumnar|
|[#1096](https://github.com/oap-project/gluten/pull/1096)|[VL] Fix unexpected fallback in HashAgg due to empty output|
|[#1144](https://github.com/oap-project/gluten/pull/1144)|[VL] Update doc for centos8 install|
|[#1142](https://github.com/oap-project/gluten/pull/1142)|[Documentation] Fix the typo problems of NewToGluten|
|[#1140](https://github.com/oap-project/gluten/pull/1140)|[VL] Update date support status|
|[#1101](https://github.com/oap-project/gluten/pull/1101)|[Gluten-1100] Making transformer plan log more obvious#1100|
|[#1105](https://github.com/oap-project/gluten/pull/1105)|[VL] Rename jar for release|
|[#1129](https://github.com/oap-project/gluten/pull/1129)|Fix docs about some support function info confused|
|[#1127](https://github.com/oap-project/gluten/pull/1127)|[Gluten-core] Get adaptive execution context known through checking thread stack trace|
|[#1136](https://github.com/oap-project/gluten/pull/1136)|[VL] Fix window validation|
|[#1132](https://github.com/oap-project/gluten/pull/1132)|[VL] Add ColumnarToRow test|
|[#1123](https://github.com/oap-project/gluten/pull/1123)|[VL] Support date_diff on Velox backend|
|[#1056](https://github.com/oap-project/gluten/pull/1056)|[Gluten-core] Correct the judgement for whether adaptive execution is supported|
|[#1004](https://github.com/oap-project/gluten/pull/1004)|[VL]Upgrade velox to 2023/3/6|
|[#1030](https://github.com/oap-project/gluten/pull/1030)|[GLUTEN-547] Resolve Jar dependency that conflicts with Apache Spark|
|[#971](https://github.com/oap-project/gluten/pull/971)|[Gluten-core] Refactor scan validation|
|[#812](https://github.com/oap-project/gluten/pull/812)|[CH-256] fully support from_unixtime and arrayJoin|
|[#603](https://github.com/oap-project/gluten/pull/603)|[CI] Skip CI jobs on backends whose code were unchanged|
|[#1018](https://github.com/oap-project/gluten/pull/1018)|[CH-306]Fix read empty parquet|
|[#925](https://github.com/oap-project/gluten/pull/925)|[CH-278] part2: support xxhash64/hash|
|[#966](https://github.com/oap-project/gluten/pull/966)|[CH-294] support collect_list|
|[#951](https://github.com/oap-project/gluten/pull/951)|[CH-290] support to_date/date_format/trunc/add_months/map_from_arrays/translate|
|[#1048](https://github.com/oap-project/gluten/pull/1048)|[Gluten-core] Add struct literal support|
|[#1049](https://github.com/oap-project/gluten/pull/1049)|[VL] Fix agg with filter|
|[#917](https://github.com/oap-project/gluten/pull/917)|[CH-278] part1: support function md5/lpad/rpad/reverse|
|[#975](https://github.com/oap-project/gluten/pull/975)|[gluten-core] Support repeat function|
|[#897](https://github.com/oap-project/gluten/pull/897)|[CH-269]support more struct/array/map functions|
