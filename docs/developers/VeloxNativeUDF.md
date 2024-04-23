# Velox User-Defined Functions (UDF) and User-Defined Aggregate Functions (UDAF)

## Introduction

Velox backend supports User-Defined Functions (UDF) and User-Defined Aggregate Functions (UDAF).
Users can create their own functions using the UDF interface provided in Velox backend and build libraries for these functions.
At runtime, the UDF are registered at the start of applications.
Once registered, Gluten will be able to parse and offload these UDF into Velox during execution.

## Create and Build UDF/UDAF library

The following steps demonstrate how to set up a UDF library project:

- **Include the UDF Interface Header:**
  First, include the UDF interface header file [Udf.h](../../cpp/velox/udf/Udf.h) in the project file.
  The header file defines the `UdfEntry` struct, along with the macros for declaring the necessary functions to integrate the UDF into Gluten and Velox.

- **Implement the UDF:**
  Implement UDF. These functions should be able to register to Velox.

- **Implement the Interface Functions:**
  Implement the following interface functions that integrate UDF into Project Gluten:

  - `getNumUdf()`:
    This function should return the number of UDF in the library.
    This is used to allocating udfEntries array as the argument for the next function `getUdfEntries`.

  - `getUdfEntries(gluten::UdfEntry* udfEntries)`:
    This function should populate the provided udfEntries array with the details of the UDF, including function names and signatures.

  - `registerUdf()`:
    This function is called to register the UDF to Velox function registry.
    This is where users should register functions by calling `facebook::velox::exec::registerVecotorFunction` or other Velox APIs.

  - The interface functions are mapped to marcos in [Udf.h](../../cpp/velox/udf/Udf.h). Here's an example of how to implement these functions:

  ```
  // Filename MyUDF.cc

  #include <velox/expression/VectorFunction.h>
  #include <velox/udf/Udf.h>

  namespace {
  static const char* kInteger = "integer";
  }

  const int kNumMyUdf = 1;

  const char* myUdfArgs[] = {kInteger}:
  gluten::UdfEntry myUdfSig = {"myudf", kInteger, 1, myUdfArgs};

  class MyUdf : public facebook::velox::exec::VectorFunction {
    ... // Omit concrete implementation
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>>
  myUdfSignatures() {
    return {facebook::velox::exec::FunctionSignatureBuilder()
                .returnType(myUdfSig.dataType)
                .argumentType(myUdfSig.argTypes[0])
                .build()};
  }

  DEFINE_GET_NUM_UDF { return kNumMyUdf; }

  DEFINE_GET_UDF_ENTRIES { udfEntries[0] = myUdfSig; }

  DEFINE_REGISTER_UDF {
    facebook::velox::exec::registerVectorFunction(
        myUdf[0].name, myUdfSignatures(), std::make_unique<MyUdf>());
  }

  ```

To build the UDF library, users need to compile the C++ code and link to `libvelox.so`.
It's recommended to create a CMakeLists.txt for the project. Here's an example:

```
project(myudf)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(GLUTEN_HOME /path/to/gluten)

add_library(myudf SHARED "MyUDF.cpp")

find_library(VELOX_LIBRARY REQUIRED NAMES velox HINTS ${GLUTEN_HOME}/cpp/build/releases NO_DEFAULT_PATH)

target_include_directories(myudf PRIVATE ${GLUTEN_HOME}/cpp ${GLUTEN_HOME}/ep/build-velox/build/velox_ep)
target_link_libraries(myudf PRIVATE ${VELOX_LIBRARY})
```

The steps for creating and building a UDAF library are quite similar to those for a UDF library.
The major difference lies in including and defining specific functions within the UDAF header file [Udaf.h](../../cpp/velox/udf/Udaf.h)

- `getNumUdaf()`
- `getUdafEntries(gluten::UdafEntry* udafEntries)`
- `registerUdaf()`

`gluten::UdafEntry` requires an additional field `intermediateType`, to specify the output type from partial aggregation.
For detailed implementation, you can refer to the example code in [MyUDAF.cc](../../cpp/velox/udf/examples/MyUDAF.cc)

## Using UDF/UDAF in Gluten

Gluten loads the UDF libraries at runtime. You can upload UDF libraries via `--files` or `--archives`, and configure the library paths using the provided Spark configuration, which accepts comma separated list of library paths.

Note if running on Yarn client mode, the uploaded files are not reachable on driver side. Users should copy those files to somewhere reachable for driver and set `spark.gluten.sql.columnar.backend.velox.driver.udfLibraryPaths`.
This configuration is also useful when the `udfLibraryPaths` is different between driver side and executor side.

- Use the `--files` option to upload a library and configure its relative path

```shell
--files /path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=libmyudf.so
# Needed for Yarn client mode
--conf spark.gluten.sql.columnar.backend.velox.driver.udfLibraryPaths=file:///path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
```

- Use the `--archives` option to upload an archive and configure its relative path

```shell
--archives /path/to/udf_archives.zip#udf_archives
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=udf_archives
# Needed for Yarn client mode
--conf spark.gluten.sql.columnar.backend.velox.driver.udfLibraryPaths=file:///path/to/udf_archives.zip
```

- Configure URI

You can also specify the local or HDFS URIs to the UDF libraries or archives. Local URIs should exist on driver and every worker nodes.

```shell
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=file:///path/to/library_or_archive
```

## Try the example

We provided Velox UDF examples in file [MyUDF.cc](../../cpp/velox/udf/examples/MyUDF.cc) and UDAF examples in file [MyUDAF.cc](../../cpp/velox/udf/examples/MyUDAF.cc).
After building gluten cpp, you can find the example libraries at /path/to/gluten/cpp/build/velox/udf/examples/

Start spark-shell or spark-sql with below configuration

```shell
# Use the `--files` option to upload a library and configure its relative path
--files /path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=libmyudf.so
```

or

```shell
# Only configure URI
--conf spark.gluten.sql.columnar.backend.velox.udfLibraryPaths=file:///path/to/gluten/cpp/build/velox/udf/examples/libmyudf.so
```

Run query. The functions `myudf1` and `myudf2` increment the input value by a constant of 5

```
select myudf1(1), myudf2(100L)
```

The output from spark-shell will be like

```
+----------------+------------------+
|udfexpression(1)|udfexpression(100)|
+----------------+------------------+
|               6|               105|
+----------------+------------------+
```

