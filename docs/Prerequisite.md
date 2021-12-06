# Prerequisite

There are some requirements before you build the project.
Please make sure you have already installed the software in your system.

1. GCC 7.0 or higher version
2. LLVM 7.0 or higher version
3. java8 OpenJDK -> yum install java-1.8.0-openjdk
4. cmake 3.16 or higher version
5. Maven 3.6.3 or higher version
6. Hadoop 2.7.5 or higher version
7. Spark 3.1.1 or higher version
8. Intel Optimized Arrow 4.0.0

## gcc installation

// installing GCC 7.0 or higher version

Please notes for better performance support, GCC 7.0 is a minimal requirement with Intel Microarchitecture such as SKYLAKE, CASCADELAKE, ICELAKE.
https://gcc.gnu.org/install/index.html

Follow the above website to download gcc.
C++ library may ask a certain version, if you are using GCC 7.0 the version would be libstdc++.so.6.0.28.
You may have to launch ./contrib/download_prerequisites command to install all the prerequisites for gcc.
If you are facing downloading issue in download_prerequisites command, you can try to change ftp to http.

//Follow the steps to configure gcc
https://gcc.gnu.org/install/configure.html

If you are facing a multilib issue, you can try to add --disable-multilib parameter in ../configure

//Follow the steps to build gc
https://gcc.gnu.org/install/build.html

//Follow the steps to install gcc
https://gcc.gnu.org/install/finalinstall.html

//Set up Environment for new gcc
```
export PATH=$YOUR_GCC_INSTALLATION_DIR/bin:$PATH
export LD_LIBRARY_PATH=$YOUR_GCC_INSTALLATION_DIR/lib64:$LD_LIBRARY_PATH
```
Please remember to add and source the setup in your environment files such as /etc/profile or /etc/bashrc

//Verify if gcc has been installation
Use `gcc -v` command to verify if your gcc version is correct.(Must larger than 7.0)

## LLVM 7.0 installation

Arrow Gandiva depends on LLVM, and I noticed current version strictly depends on llvm7.0 if you installed any other version rather than 7.0, it will fail.
``` shell
wget http://releases.llvm.org/7.0.1/llvm-7.0.1.src.tar.xz
tar xf llvm-7.0.1.src.tar.xz
cd llvm-7.0.1.src/
cd tools
wget http://releases.llvm.org/7.0.1/cfe-7.0.1.src.tar.xz
tar xf cfe-7.0.1.src.tar.xz
mv cfe-7.0.1.src clang
cd ..
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j
cmake --build . --target install
# check if clang has also been compiled, if no
cd tools/clang
mkdir build
cd build
cmake ..
make -j
make install
```


## cmake installation
If you are facing some trouble when installing cmake, please follow below steps to install cmake.

```
// installing Cmake 3.16.1
sudo yum install cmake3

// If you have an existing cmake, you can use below command to set it as an option within alternatives command
sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake 10 --slave /usr/local/bin/ctest ctest /usr/bin/ctest --slave /usr/local/bin/cpack cpack /usr/bin/cpack --slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake --family cmake

// Set cmake3 as an option within alternatives command
sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake3 20 --slave /usr/local/bin/ctest ctest /usr/bin/ctest3 --slave /usr/local/bin/cpack cpack /usr/bin/cpack3 --slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake3 --family cmake

// Use alternatives to choose cmake version
sudo alternatives --config cmake
```

## maven installation

If you are facing some trouble when installing maven, please follow below steps to install maven

// installing maven 3.6.3

Go to https://maven.apache.org/download.cgi and download the specific version of maven

// Below command use maven 3.6.3 as an example
```
wget htps://ftp.wayne.edu/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
wget https://ftp.wayne.edu/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
tar xzf apache-maven-3.6.3-bin.tar.gz
mkdir /usr/local/maven
mv apache-maven-3.6.3/ /usr/local/maven/
```

// Set maven 3.6.3 as an option within alternatives command
```
sudo alternatives --install /usr/bin/mvn mvn /usr/local/maven/apache-maven-3.6.3/bin/mvn 1
```

// Use alternatives to choose mvn version

```
sudo alternatives --config mvn
```

## HADOOP/SPARK Installation

If there is no existing Hadoop/Spark installed, Please follow the guide to install your Hadoop/Spark [SPARK/HADOOP Installation](./SparkInstallation.md)

### Hadoop Native Library(Default)

Please make sure you have set up Hadoop directory properly with Hadoop Native Libraries
By default, Apache Arrow would scan `$HADOOP_HOME` and find the native Hadoop library `libhdfs.so`(under `$HADOOP_HOME/lib/native` directory) to be used for Hadoop client.

You can also use `ARROW_LIBHDFS_DIR` to configure the location of `libhdfs.so` if it is installed in other directory than `$HADOOP_HOME/lib/native`

If your SPARK and HADOOP are separated in different nodes, please find `libhdfs.so` in your Hadoop cluster and copy it to SPARK cluster, then use one of the above methods to set it properly.

For more information, please check
Arrow HDFS interface [documentation](https://github.com/apache/arrow/blob/master/cpp/apidoc/HDFS.md)
Hadoop Native Library, please read the official Hadoop website [documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/NativeLibraries.html)

### Use libhdfs3 library for better performance(Optional)

For better performance ArrowDataSource reads HDFS files using the third-party library `libhdfs3`. The library must be pre-installed on machines Spark Executor nodes are running on.

To install the library, use of [Conda](https://docs.conda.io/en/latest/) is recommended.

```
// installing libhdfs3
conda install -c conda-forge libhdfs3

// check the installed library file
ll ~/miniconda/envs/$(YOUR_ENV_NAME)/lib/libhdfs3.so
```

We also provide a libhdfs3 binary in cpp/src/resources directory.

To set up libhdfs3, there are two different ways:
Option1: Overwrite the soft link for libhdfs.so
To install libhdfs3.so, you have to create a soft link for libhdfs.so in your Hadoop directory(`$HADOOP_HOME/lib/native` by default).

```
ln -f -s libhdfs3.so libhdfs.so
```

Option2:
Add env variable to the system
```
export ARROW_LIBHDFS3_DIR="PATH_TO_LIBHDFS3_DIR/"
```

Add following Spark configuration options before running the DataSource to make the library to be recognized:

* `spark.executorEnv.ARROW_LIBHDFS3_DIR = "PATH_TO_LIBHDFS3_DIR/"`
* `spark.executorEnv.LD_LIBRARY_PATH = "PATH_TO_LIBHDFS3_DEPENDENCIES_DIR/"`

Please notes: If you choose to use libhdfs3.so, there are some other dependency libraries you have to installed such as libprotobuf or libcrypto.


## Intel Optimized Apache Arrow Installation

During the mvn compile command, it will launch a script(build_arrow.sh) to help install and compile a Intel custom Arrow library.
If you wish to build Apache Arrow by yourself, please follow the guide to build and install Apache Arrow [ArrowInstallation](./ApacheArrowInstallation.md)

