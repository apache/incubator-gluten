### Build parameters

Based on the different environment, there are some parameters can be set via -D with mvn.

| Parameters                                            | Description                                                                                                                                                                         | Default Value                                       |
|-------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| backends-velox                                        | Add -Pbackends-velox in maven command to compile the JVM part of Velox backend.                                                                                                     | false                                              |
| backends-gazelle                                        | Add -Pbackends-gazelle in maven command to compile the JVM part of Gazelle backend.                                                                                                     | false                                              |
| backends-clickhouse                                   | Add -Pbackends-clickhouse in maven command to compile the JVM part of ClickHouse backend.                                                                                           | false                                              |

There are some parameters can be set via -- with build_arrow_for_gazelle.sh.

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
| gluten_dir | Root path of gluten project | /path_to_gluten |
| build_dir  | Arrow build path            | GLUTEN_DIR/ep/build-arrow/build|
| build_type | ARROW build type            | release|

There are some parameters can be set via -- with build_arrow_for_velox.sh.

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
| gluten_dir | Root path of gluten project               | /path_to_gluten |
| build_dir  | Arrow build path                          | GLUTEN_DIR/ep/build-arrow/build|
| build_type | ARROW build type                          | release|
| build_test | Build arrow with -DARROW_JSON=ON          | OFF           |
| build_benchmarks | Build arrow with -DWITH_PARQUET=ON  | OFF           |

There are some parameters can be set via -- with compile.sh.

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
| gluten_dir | Root path of gluten project | /path_to_gluten |
| arrow_root | path of arrow lib           | /path_to_gluten/ep/build-arrow/build/arrow_install |
| velox_home | path of velox lib           | /path_to_gluten/ep/build-velox/build/velox_ep |
| build_type | Gluten cpp part build type  | release |
| build_gazelle_cpp_backend | build gazelle-cpp in cpp folder | OFF |
| build_velox_backend | build velox in cpp folder | OFF |
| build_test | build test code in cpp folder      | OFF |
| build_benchmarks | build benchmark code in cpp folder | OFF |
| build_jemalloc   | build with jemalloc | ON |
| enable_hbm | enable HBM allocator      | OFF|
| build_protobuf | build protobuf lib    | OFF|
| enable_s3   | build with s3 lib        | OFF|
| enable_hdfs | build with hdfs lib      | OFF|

### Build jar

The gluten jar name pattern is gluten-spark<sparkbundle.version>_<scala.binary.version>-<version>-SNAPSHOT-jar-with-dependencies.jar

| Spark Version | sparkbundle.version | scala.binary.version |
| ---------- | ----------- | ------------- |
| 3.2.2 | 3.2 | 2.12 |
| 3.3.1 | 3.3 | 2.12 |

Backend velox support both spark3.2.2 and spark3.3.1 while backend clickhouse support spark3.2.2
