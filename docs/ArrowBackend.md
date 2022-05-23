## Introduction
Arrow backend is derived from the C++ part of the [gazelle_plugin](https://github.com/oap-project/gazelle_plugin) project. It converts the intermediate substrait plan to Arrow's execution engine to build a pipeline for the off-loaded stage from Spark, and uses the optimized compute functions that are implemented in gazelle_plugin.
## Environment Setup
### Prerequisites

- **OS Requirements**
We have tested OAP on Fedora 29 and CentOS 7.6 (kernel-4.18.16). We recommend you use **Fedora 29 CentOS 7.6 or above**. Besides, for [Memkind](https://github.com/memkind/memkind/tree/v1.10.1-rc2) we recommend you use **kernel above 3.10**.
- **Software Requirements**
   - OpenJDK 8
   - Maven 3.6.3 or higher version
   - Spark 3.1.1
   - Hadoop 2.7.5 or higher version
- **Conda Requirements**
Install Conda on your cluster nodes with below commands and follow the prompts on the installer screens.

```bash
$ wget -c https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
$ chmod +x Miniconda3-latest-Linux-x86_64.sh 
$ bash Miniconda3-latest-Linux-x86_64.sh
```

For changes to take effect, **_close and re-open_** your current shell.
To test your installation,  run the command `conda list` in your terminal window. A list of installed packages appears if it has been installed correctly.

### Building Gluten with Arrow backend

#### Environment Setup

It's recommended to use Conda environment to install packages and all requirements in one go, including OAP Conda package and dependencies for building Arrow.

```bash
$ git clone https://github.com/oap-project/arrow.git -b arrow-8.0.0-gluten-20220427a
$ conda create -n oapenv -c conda-forge -c intel -y \
      --file arrow/ci/conda_env_unix.txt \
      --file arrow/ci/conda_env_cpp.txt \
      --file arrow/ci/conda_env_python.txt \
      --file arrow/ci/conda_env_gandiva.txt \
      compilers \
      python=3.9 \
      pandas \
      oap=1.2.0
```
#### 
Make sure the Conda environment is activated during the building process.
```bash
$ conda activate oapenv
$ export ARROW_HOME=$CONDA_PREFIX
```

#### Installing Arrow C++
```bash
$ mkdir arrow/cpp/build
$ pushd arrow/cpp/build
$ cmake -DARROW_BUILD_STATIC=OFF \
        -DARROW_BUILD_SHARED=ON \
        -DARROW_COMPUTE=ON \
        -DARROW_SUBSTRAIT=ON \
        -DARROW_S3=ON \
        -DARROW_GANDIVA_JAVA=ON \
        -DARROW_GANDIVA=ON \
        -DARROW_PARQUET=ON \
        -DARROW_ORC=ON \
        -DARROW_HDFS=ON \
        -DARROW_BOOST_USE_SHARED=OFF \
        -DARROW_JNI=ON \
        -DARROW_DATASET=ON \
        -DARROW_WITH_PROTOBUF=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_ZSTD=OFF \
        -DARROW_WITH_BROTLI=OFF \
        -DARROW_WITH_ZLIB=OFF \
        -DARROW_WITH_FASTPFOR=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_JSON=ON \
        -DARROW_CSV=ON \
        -DARROW_FLIGHT=OFF \
        -DARROW_JEMALLOC=ON \
        -DARROW_SIMD_LEVEL=AVX2 \
        -DARROW_RUNTIME_SIMD_LEVEL=MAX \
        -DARROW_DEPENDENCY_SOURCE=BUNDLED \
        -DCMAKE_INSTALL_PREFIX=${ARROW_HOME} \
        -DCMAKE_INSTALL_LIBDIR=lib \
        ..

$ make -j$(nproc --ignore=2)
$ make install
$ popd
```
#### 
#### Installing Arrow Java
```bash
$ pushd arrow/java
$ mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=/path/to/arrow/cpp/build/release/ -DskipTests
$ popd
```

#### Building Gluten
```bash
$ git clone https://github.com/oap-project/gluten.git
$ pushd gluten
$ mvn clean package -Pbackends-gazelle -P full-scala-compiler -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_protobuf=ON -Dbuild_gazelle_cpp=ON -Darrow_root=${ARROW_HOME}
$ popd
```

### Enabling Arrow Bckend at Runtime

In addition to your cutomized Spark configurations, extra configurations for enabling Gluten with ArrowBackend should be added.
#### Common configurations for Gluten
| Configuration | Value | Comment |
| --- | --- | --- |
| spark.plugins | io.glutenproject.GlutenPlugin |  |
| spark.gluten.sql.columnar.backend.lib | gazelle_cpp |  |
| spark.shuffle.manager | org.apache.spark.shuffle.sort.ColumnarShuffleManager |  |
| spark.sql.sources.useV1SourceList | avro |  |
| spark.memory.offHeap.size | 20g | 
 |


## Performance


_WIP_
