There are some common questions about developing, debugging and testing been asked again and again. In order to help the developers to contribute
to Gluten as soon as possible, we collected these frequently asked questions, and organized them in the form of Q&A. It's convenient for the developers
to check and learn.

when you encountered a new problem and then resolved it, please add a new item to this document if you think it may be helpful to the other developers.

# How to understand the key work of Gluten?

The Gluten worked as the role of bridge, it's a middle layer between the Spark and the native execution library.

The Gluten is responsibility for validating whether the operators of the Spark plan can be executed by the native engine or not. If yes, the Gluten
transforms Spark plan to Substrait plan, and then send the Substrait plan to the native engine.

The Gluten codes consist of two parts: the C++ codes and the Java/Scala codes. 
1. All C++ codes place under the directory of `gluten_home/cpp`, the Java/Scala codes place under several directories, such as `gluten_home/gluten-core`,
  `gluten_home/gluten-data`, `gluten_home/backends_velox`.
2. The Java/Scala codes are responsibility for validating and transforming execution plan. Source data should also be provided, the source data may come
   from files or other forms such as networks.
3. The C++ codes take the Substrait plan and the source data as inputs and transform the Substrait plan to the corresponding backend plan. If the backend
   is Velox, the Substrait plan will be transformed to the Velox plan, and then be executed.

JNI is a programming technology of invoding C++ from Java. All JNI interfaces are defined in the file `JniWrapper.cc` under directory `jni`.

# How to debug in Gluten?

## 1 How to debug C++
If you don't concern about the Scala/Java codes and just want to debug the C++ codes, you may debug the Velox bench-mark following the below steps.

1. First step, compile cpp with `BUILD_TYPE` debug.
  - `cd gluten_home/cpp`
  - `./compile.sh --BUILD_TYPE=debug`. 
  compiling with `BUILD_TYPE=debug` is good for debugging, after the above two operations, the executable file `generic_benchmark` will be generated under
  the directory of `gluten_home/cpp/velox/benchmarks/`.

2. Second step, debug benchmarks using GDB
  - `cd gluten_home/cpp/velox/benchmarks/`
  Now, debug benchmarks with GDB, execute the below command:
  - `gdb generic_benchmark`
  When GDB load `generic_benchmark` successfully, you can set `breakpoint` on the `main` function with the command `b main`, and then run with the `r` 
  command of gdb.
  You can debug `generic_benchmark` using any gdb commands as debugging normal C++ program, because the `generic_benchmark` is a pure C++ executable file.

3. `gdb-tui` is a valuable feature and is worth trying. You can get more help from online docs.
  [gdb-tui](https://sourceware.org/gdb/current/onlinedocs/gdb/TUI.html#TUI)

4. You can start `generic_benchmark` with specific JSON plan and input files.
  If you omit them, the `example.json, example_lineitem + example_orders` under the directory of `gluten_home/backends-velox/generated-native-benchmark` 
  will be used as the default inputs.
  You can edit the `example.json` to custom the Substrait plan or provide the inputs files placed other directory.

## 2 How to debug Java/Scala
wait to add

# How to run TPC-H on Velox backend

Now, both Parquet and DWRF format files are supported, related scripts and files are under the directory of `gluten_home/backends_velox/workload/tpch`.
The file `README.md` under `gluten_home/backends_velox/workload/tpch` offers some useful help but it's still not enough and exact.

Here will explain how to run TPC-H on Velox backend with the Parquet file format.
1. First step, prepare the datasets, you have two choices.
  - One way, generate Parquet datasets using the script under `gluten_home/backends_velox/workload/tpch/gen_data/parquet_dataset`, You can get help from the above
    mentioned `README.md`.
  - The Other way, using the small dataset under `gluten_home/backends-velox/src/test/resources/tpch-data-parquet-velox` directly, If you just want to make simple
    TPC-H testing, this dataset is a good choice.
2. Second step, run TPC-H on Velox backend testing.
  - Modify `gluten_home/backends-velox/workload/tpch/run_tpch/tpch_parquet.scala`.
    - set `var parquet_file_path` to correct directory, if using the small dataset directly in the step one, then modify it as below
    ```
    var parquet_file_path = "gluten_home/backends-velox/src/test/resources/tpch-data-parquet-velox"
    ```
    - set `var gluten_root` to correct directory, if `gluten_home` is the directory of `/home/gluten`, then modify it as below
    ```
    var gluten_root = "/home/gluten"
    ```
  - Modify `gluten_home/backends-velox/workload/tpch/run_tpch/tpch_parquet.sh`.
    - Set `GLUTEN_JAR` correctly.
    - Set `SPARK_HOME` correctly.
    - Set the memory configurations appropriately.
  - Execute `tpch_parquet.sh` using the below command.
    - `cd gluten_home/backends-velox/workload/tpch/run_tpch/`
    - `./tpch_parquet.sh`

# How to run TPC-DS
wait to add
