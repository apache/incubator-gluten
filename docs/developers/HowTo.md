---
layout: page
title: How To Use Gluten
nav_order: 1
parent: Developer Overview
---
There are some common questions about developing, debugging and testing been asked again and again. In order to help the developers to contribute
to Gluten as soon as possible, we collected these frequently asked questions, and organized them in the form of Q&A. It's convenient for the developers
to check and learn.

when you encountered a new problem and then resolved it, please add a new item to this document if you think it may be helpful to the other developers.

We use `gluten_home` to represent the home directory of Gluten in this document.

# How to understand the key work of Gluten?

The Gluten worked as the role of bridge, it's a middle layer between the Spark and the native execution library.

The Gluten is responsibility for validating whether the operators of the Spark plan can be executed by the native engine or not. If yes, the Gluten
transforms Spark plan to Substrait plan, and then send the Substrait plan to the native engine.

The Gluten codes consist of two parts: the C++ codes and the Java/Scala codes. 
1. All C++ codes are placed under the directory of `gluten_home/cpp`, the Java/Scala codes are placed under several directories, such as
  `gluten_home/gluten-core` `gluten_home/gluten-data` `gluten_home/backends-velox`.
2. The Java/Scala codes are responsibility for validating and transforming the execution plan. Source data should also be provided, the source data may
  come from files or other forms such as networks.
3. The C++ codes take the Substrait plan and the source data as inputs and transform the Substrait plan to the corresponding backend plan. If the backend
  is Velox, the Substrait plan will be transformed to the Velox plan, and then be executed.

JNI is a programming technology of invoking C++ from Java. All JNI interfaces are defined in the file `JniWrapper.cc` under the directory `jni`.

# How to debug in Gluten?

## 1 How to debug C++
If you don't concern about the Scala/Java codes and just want to debug the C++ codes executed in native engine, you may debug the C++ via benchmarks
with GDB.

To debug C++, you have to generate the example files, the example files consist of:
- A file contained Substrait plan in JSON format
- One or more input data files in Parquet format

You can generate the example files by the following steps:

1. get and build Arrow
```
cd gluten_home/ep/build-arrow/src
./get_arrow.sh
./build_arrow.sh
```

2. get and build Velox
```
cd gluten_home/ep/build-velox/src
./get_velox.sh
./build_velox.sh --build_type=Debug
```

3. compile the CPP
```
cd gluten_home/cpp
mkdir build
cd build
cmake -DBUILD_VELOX_BACKEND=ON -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Debug ..
make -j
```
- Compiling with `--build_type=Debug` is good for debugging.
- The executable file `generic_benchmark` will be generated under the directory of `gluten_home/cpp/build/velox/benchmarks/`.

4. build Gluten and generate the example files
```
cd gluten_home
mvn clean package -Pspark-3.2 -Pbackends-velox -Prss
mvn test -Pspark-3.2 -Pbackends-velox -Prss -pl backends-velox -am -DtagsToInclude="io.glutenproject.tags.GenerateExample" -Dtest=none -DfailIfNoTests=false -Darrow.version=11.0.0-gluten -Dexec.skip
```
- After the above operations, the examples files are generated under `gluten_home/backends-velox`
- You can check it by the command `tree gluten_home/backends-velox/generated-native-benchmark/`
- You may replace `-Pspark-3.2` with `-Pspark-3.3` if your spark's version is 3.3
```shell
$ tree gluten_home/backends-velox/generated-native-benchmark/
gluten_home/backends-velox/generated-native-benchmark/
├── example.json
├── example_lineitem
│   ├── part-00000-3ec19189-d20e-4240-85ae-88631d46b612-c000.snappy.parquet
│   └── _SUCCESS
└── example_orders
    ├── part-00000-1e66fb98-4dd6-47a6-8679-8625dbc437ee-c000.snappy.parquet
    └── _SUCCESS
```

5. now, run benchmarks with GDB
```
cd gluten_home/cpp/build/velox/benchmarks/
gdb generic_benchmark
```
- When GDB load `generic_benchmark` successfully, you can set `breakpoint` on the `main` function with command `b main`, and then run with command `r`,
  then the process `generic_benchmark` will start and stop at the `main` function.
- You can check the variables' state with command `p variable_name`, or execute the program line by line with command `n`, or step-in the function been
  called with command `s`.
- Actually, you can debug `generic_benchmark` with any gdb commands as debugging normal C++ program, because the `generic_benchmark` is a pure C++
  executable file in fact.

6. `gdb-tui` is a valuable feature and is worth trying. You can get more help from the online docs.
[gdb-tui](https://sourceware.org/gdb/onlinedocs/gdb/TUI.html)

7. you can start `generic_benchmark` with specific JSON plan and input files
- If you omit them, the `example.json, example_lineitem + example_orders` under the directory of `gluten_home/backends-velox/generated-native-benchmark`
  will be used as default.
- You can also edit the file `example.json` to custom the Substrait plan or specify the inputs files placed in the other directory.

8. get more detail information about benchmarks from [MicroBenchmarks](./MicroBenchmarks.md)

## 2 How to debug Java/Scala
wait to add

## 3 How to debug with core-dump
wait to complete
```
cd the_directory_of_core_file_generated
gdb gluten_home/cpp/build/releases/libgluten.so 'core-Executor task l-2000883-1671542526'

```
- the `core-Executor task l-2000883-1671542526` represents the core file name.

# How to run TPC-H on Velox backend

Now, both Parquet and DWRF format files are supported, related scripts and files are under the directory of `gluten_home/backends-velox/workload/tpch`.
The file `README.md` under `gluten_home/backends-velox/workload/tpch` offers some useful help but it's still not enough and exact.

One way of run TPC-H test is to run velox-be by workflow, you can refer to [velox_be.yml](https://github.com/oap-project/gluten/blob/main/.github/workflows/velox_be.yml#L90)

Here will explain how to run TPC-H on Velox backend with the Parquet file format.
1. First step, prepare the datasets, you have two choices.
  - One way, generate Parquet datasets using the script under `gluten_home/backends-velox/workload/tpch/gen_data/parquet_dataset`, You can get help from the above
    mentioned `README.md`.
  - The other way, using the small dataset under `gluten_home/backends-velox/src/test/resources/tpch-data-parquet-velox` directly, If you just want to make simple
    TPC-H testing, this dataset is a good choice.
2. Second step, run TPC-H on Velox backend testing.
  - Modify `gluten_home/backends-velox/workload/tpch/run_tpch/tpch_parquet.scala`.
    - set `var parquet_file_path` to correct directory. If using the small dataset directly in the step one, then modify it as below
    ```
    var parquet_file_path = "gluten_home/backends-velox/src/test/resources/tpch-data-parquet-velox"
    ```
    - set `var gluten_root` to correct directory. If `gluten_home` is the directory of `/home/gluten`, then modify it as below
    ```
    var gluten_root = "/home/gluten"
    ```
  - Modify `gluten_home/backends-velox/workload/tpch/run_tpch/tpch_parquet.sh`.
    - Set `GLUTEN_JAR` correctly. Please refer to the section of [Build Gluten with Velox Backend](../get-started/Velox.md/#2-build-gluten-with-velox-backend)
    - Set `SPARK_HOME` correctly.
    - Set the memory configurations appropriately.
  - Execute `tpch_parquet.sh` using the below command.
    - `cd gluten_home/backends-velox/workload/tpch/run_tpch/`
    - `./tpch_parquet.sh`

# How to run TPC-DS
wait to add

# How to track the memory exhaust problem
When your gluten spark jobs failed because of OOM, you can track the memory allocation's call stack by configuring `spark.gluten.backtrace.allocation = true`.
The above configuration will use `BacktraceAllocationListener` wrapping from `SparkAllocationListener` to create `VeloxMemoryManager`.

`BacktraceAllocationListener` will check every allocation, if a single allocation bytes exceeds a fixed value or the accumulative allocation bytes exceeds 1/2/3...G,
the call stack of memory allocation will be outputted to standard output, you can check the backtrace and get some valuable information about tracking the memory exhaust issues.

You can also adjust the policy to decide when to backtrace, such as the fixed value.

