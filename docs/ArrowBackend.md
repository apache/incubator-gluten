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

#### Environment Setup

```bash
$ apt-get update -y && \
    apt-get install -y \
        build-essential \
        ccache \
        cmake \
        git \
        libssl-dev \
        libcurl4-openssl-dev \
        python3-pip \
        wget \
        llvm-10 \
        clang-10 \
        libboost-dev
```

#### Building Gluten

The build and install of our custom Arrow is embedded into the build of Gluten.

```bash
$ git clone https://github.com/oap-project/gluten.git
$ pushd gluten
$ mvn clean package -Pfull-scala-compiler -Pbackends-gazelle -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_arrow=ON -Dbuild_protobuf=ON -Dbuild_gazelle_cpp=ON
$ popd
```

### Enabling Arrow Bckend at Runtime

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
