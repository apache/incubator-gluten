---
layout: page
title: Build Parameters for Velox Backend
nav_order: 4
parent: Getting-Started
---
# Build parameters
## Parameters for buildbundle-veloxbe.sh or builddeps-veloxbe.sh
Please set them via `--`, e.g. `--build_type=Release`.

| Parameters       | Description                                                 | Default value |
|------------------|-------------------------------------------------------------|---------------|
| build_type       | build type for arrow, velox & gluten cpp, CMAKE_BUILD_TYPE  | Release       |
| build_tests      | build test code in cpp folder and arrow                     | OFF           |
| build_benchmarks | build benchmark code in cpp folder and arrow                | OFF           |
| build_jemalloc   | build with jemalloc                                         | ON            |
| build_protobuf   | build protobuf lib                                          | ON            |
| enable_qat       | enable QAT for shuffle data de/compression                  | OFF           |
| enable_iaa       | enable IAA for shuffle data de/compression                  | OFF           |
| enable_hbm       | enable HBM allocator                                        | OFF           |
| enable_s3        | build with s3 lib                                           | OFF           |
| enable_hdfs      | build with hdfs lib                                         | OFF           |
| enable_ep_cache  | enable caching for external project build (Arrow and Velox) | OFF           |
| skip_build_ep    | skip the build of external projects (arrow, velox)          | OFF           |
| enable_vcpkg     | enable vcpkg for static build                               | OFF           |

## Parameters for get_arrow.sh
Please set them via `--`, e.g., `--enable_custom_codec=ON`.

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
| enable_custom_codec | Apply patch to plugin custom codec (used by QAT/IAA) in Arrow cpp IPC module. | OFF |

## Parameters for build_arrow.sh
Please set them via `--`, e.g., `--arrow_home=/YOUR/PATH`.

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
| arrow_home | Arrow build path                          | GLUTEN_DIR/ep/build-arrow/build|
| build_type | ARROW build type, CMAKE_BUILD_TYPE        | Release|
| build_tests | Build arrow with -DARROW_JSON=ON          | OFF           |

## Parameters for build_velox.sh
Please set them via `--`, e.g., `--velox_home=/YOUR/PATH`.

| Parameters | Description | Default value |
| ---------- | ----------- | ------------- |
| velox_home | Velox build path                          | GLUTEN_DIR/ep/build-velox/build/velox_ep|
| build_type | Velox build type, CMAKE_BUILD_TYPE        | Release|
| enable_s3  | Build Velox with -DENABLE_S3              | OFF           |
| enable_hdfs | Build Velox with -DENABLE_HDFS           | OFF           |
| build_protobuf | build protobuf from source            | ON           |
| run_setup_script | Run Velox setup script before build | ON           |

## Maven building parameters
To build different backends, there are 3 parameters can be set via `-P` for mvn.

| Parameters          | Description                                                                                    | Activation state by default |
|---------------------|------------------------------------------------------------------------------------------------|-----------------------------|
| backends-velox      | Add -Pbackends-velox in maven command to compile the JVM part of Velox backend.                | disabled                    |
| backends-clickhouse | Add -Pbackends-clickhouse in maven command to compile the JVM part of ClickHouse backend.      | disabled                    |
| rss                 | Add -Prss in maven command to compile the JVM part of rss, current only support Velox backend. | disabled                    |

# Gluten jar for deployment

The gluten jar's name pattern is `gluten-<backend_type>-bundle-spark<sparkbundle.version>_<scala.binary.version>-<os.detected.release>_<os.detected.release.version>-<project.version>.jar`.

| Spark Version | sparkbundle.version | scala.binary.version |
| ---------- | ----------- | ------------- |
| 3.2.2 | 3.2 | 2.12 |
| 3.3.1 | 3.3 | 2.12 |

The velox backend and the clickhouse backend support both spark-3.2.2 and spark-3.3.1.
