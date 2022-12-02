## Introduction

Arrow backend is derived from the C++ part of
the [gazelle_plugin](https://github.com/oap-project/gazelle_plugin) project. It converts the
intermediate substrait plan to Arrow's execution engine to build a pipeline for the off-loaded stage
from Spark, and uses the optimized compute functions that are implemented in gazelle_plugin.

### Prerequisites

- **OS Requirements**
  We have tested Gluten on Ubuntu 20.04 (kernel version 5.13.0-44). We recommend you use **Ubuntu
  20.04**.
- **Software Requirements**
    - OpenJDK 8
    - Maven 3.6.3 or higher version
    - Spark 3.1.1

#### Building Gluten with Arrow Backend

```bash
## install gcc and libraries to build arrow
apt-get update && apt-get install -y sudo locales wget tar tzdata git ccache cmake ninja-build build-essential llvm-11-dev clang-11 libiberty-dev libdwarf-dev libre2-dev libz-dev libssl-dev libboost-all-dev libcurl4-openssl-dev openjdk-8-jdk maven

## make sure jdk8 is used
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

##setup proxy if needed
export http_proxy=xxxx
export https_proxy=xxxx

##config maven proxy
mkdir ~/.m2/
apt install vim
vim ~/.m2/settings.xml

git clone https://github.com/oap-project/gluten.git

## fetch arrow for gazelle and compile
cd /path_to_gluten/ep/build-arrow/src
./get_arrow.sh  --arrow_branch=arrow-8.0.0-gluten-20220427a --gluten_dir=/path_to_gluten 
./build_arrow_for_gazelle.sh --gluten_dir=/path_to_gluten 

## compile gluten cpp
cd /path_to_gluten/cpp
./compile.sh --build_gazelle_cpp_backend=ON --build_protobuf=ON --gluten_dir=/path_to_gluten 

## compile gluten jvm and package. If you are using spark 3.3, replace -Pspark-3.2 with -Pspark3.3
cd /path_to_gluten
mvn clean package -Pspark-3.2 -Pbackends-gazelle -DskipTests

```

### Enabling Arrow Backend at Runtime

In addition to your cutomized Spark configurations, extra configurations for enabling Gluten with
ArrowBackend should be added.

#### Common configurations for Gluten

| Configuration | Value | Comment |
| --- | --- | --- |
| spark.driver.extraClassPath | /path/to/gluten/backends-velox/target/gluten-1.0.0-SNAPSHOT-jar-with-dependencies.jar |  |
| spark.executor.extraClassPath | /path/to/gluten/backends-velox/target/gluten-1.0.0-SNAPSHOT-jar-with-dependencies.jar |  |
| spark.plugins | io.glutenproject.GlutenPlugin |  |
| spark.gluten.sql.columnar.backend.lib | gazelle_cpp |  |
| spark.shuffle.manager | org.apache.spark.shuffle.sort.ColumnarShuffleManager |  |
| spark.sql.sources.useV1SourceList | avro |  |
| spark.memory.offHeap.size | 20g |  |

## Performance

_WIP_
