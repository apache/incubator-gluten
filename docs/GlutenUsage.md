### Build parameters

#### Parameters for one_step_veloxbackend.sh setting via --

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
| build_type | Gluten build type(for arrow/velox/cpp)  | release |
| build_test | build test code in cpp folder and arrow | OFF |
| build_benchmarks | build benchmark code in cpp folder and arrow| OFF |
| build_jemalloc   | build with jemalloc | ON |
| enable_hbm | enable HBM allocator      | OFF|
| build_protobuf | build protobuf lib    | ON|
| enable_s3   | build with s3 lib        | OFF|
| enable_hdfs | build with hdfs lib      | OFF|
| build_arrow_from_source   | pull the source code and build arrow lib| ON|
| build_velox_from_source   | pull the source code and build velox lib| ON|

#### Parameters for build_arrow_for_gazelle.sh setting via --

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
| arrow_home | Arrow build path            | GLUTEN_DIR/ep/build-arrow/build|
| build_type | ARROW build type            | release|   

#### Parameters for build_arrow_for_velox.sh setting via --

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
| arrow_home | Arrow build path                          | GLUTEN_DIR/ep/build-arrow/build|
| build_type | ARROW build type                          | release|
| build_test | Build arrow with -DARROW_JSON=ON          | OFF           |

#### Parameters for build_velox.sh setting via --

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
| velox_home | Velox build path                          | GLUTEN_DIR/ep/build-velox/build/velox_ep|
| build_type | Velox build type                          | release|
| enable_s3  | Build Velox with -DENABLE_S3              | OFF           |
| enable_hdfs | Build Velox with -DENABLE_HDFS           | OFF           |
| build_protobuf | build protobuf from source            | ON           |

#### Parameters for compile.sh setting via --

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
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

#### To build different backends, there are 3 parameters can be set via -D with mvn 

| Parameters                                            | Description                                                                                                                                                                         | Default Value                                       |
|-------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| backends-velox                                        | Add -Pbackends-velox in maven command to compile the JVM part of Velox backend.                                                                                                     | false                                              |
| backends-gazelle                                        | Add -Pbackends-gazelle in maven command to compile the JVM part of Gazelle backend.                                                                                                     | false                                              |
| backends-clickhouse                                   | Add -Pbackends-clickhouse in maven command to compile the JVM part of ClickHouse backend.                                                                                           | false                                              |

### Build jar

The gluten jar name pattern is gluten-spark<sparkbundle.version>_<scala.binary.version>-<version>-SNAPSHOT-jar-with-dependencies.jar

| Spark Version | sparkbundle.version | scala.binary.version |
| ---------- | ----------- | ------------- |
| 3.2.2 | 3.2 | 2.12 |
| 3.3.1 | 3.3 | 2.12 |

Backend velox support both spark3.2.2 and spark3.3.1 while backend clickhouse support spark3.2.2
