# Apache Gluten Copilot Instructions

Apache Gluten is a middle layer for offloading Spark SQL execution to native engines (Velox or ClickHouse). It transforms Spark's physical plan to Substrait, then to native execution via JNI.

## Build Commands

### Full Build (Velox Backend)
```bash
# First-time build (all Spark versions)
./dev/buildbundle-veloxbe.sh

# Subsequent builds (skip arrow and setup)
./dev/buildbundle-veloxbe.sh --build_arrow=OFF --run_setup_script=OFF

# Build for specific Spark version
./dev/buildbundle-veloxbe.sh --spark_version=3.5
```

### Step-by-Step Build
```bash
# Build dependencies
./dev/builddeps-veloxbe.sh build_arrow
./dev/builddeps-veloxbe.sh build_velox
./dev/builddeps-veloxbe.sh build_gluten_cpp

# Build Java (choose Spark version)
mvn clean package -Pbackends-velox -Pspark-3.5 -DskipTests
```

### Debug Build (for C++ debugging)
```bash
./dev/builddeps-veloxbe.sh --build_tests=ON --build_benchmarks=ON --build_type=Debug
```

## Test Commands

### Run All Unit Tests
```bash
mvn test -Pbackends-velox -Pspark-3.5 -Pspark-ut
```

### Run a Single Test Class
```bash
mvn test -Pbackends-velox -Pspark-3.5 -Pspark-ut -Dtest=YourTestClass
```

### Run a Single Test Method
```bash
mvn test -Pbackends-velox -Pspark-3.5 -Pspark-ut -Dtest=YourTestClass#testMethodName
```

### Run C++ Tests
```bash
# After building with --build_tests=ON
./cpp/build/velox/tests/velox_shuffle_writer_test
```

## Code Formatting

### Java/Scala
```bash
./dev/format-scala-code.sh
```

### C++ (requires clang-format-15)
```bash
./dev/format-cpp-code.sh
```

### CMake Files
```bash
cmake-format --first-comment-is-literal True --in-place cpp/velox/CMakeLists.txt
```

### License Headers
```bash
dev/check.py header main --fix
```

## Architecture

### Core Flow
1. **Spark Physical Plan** → Substrait plan conversion (`gluten-substrait`)
2. **Substrait Plan** → Native plan via JNI (`cpp/`)
3. **Native Execution** → Velox or ClickHouse backend
4. **Results** → Returned as ColumnarBatch using Arrow format

### Key Modules
- `gluten-core/` - Core Gluten functionality and Spark integration
- `gluten-substrait/` - Substrait plan conversion
- `backends-velox/` - Velox backend integration
- `backends-clickhouse/` - ClickHouse backend integration
- `cpp/velox/` - Velox JNI bridge and native operators
- `shims/` - Spark version compatibility layer (supports 3.2-3.5, 4.0-4.1)
- `gluten-ut/` - Unit tests organized by Spark version

### Fallback Mechanism
Gluten falls back to vanilla Spark for unsupported operators. Look for `GlutenRowToArrowColumnar`/`VeloxColumnarToRowExec` in query plans to identify fallback points.

## Code Conventions

### PR Title Format
- Velox backend: `[GLUTEN-<issue>][VL] description`
- ClickHouse backend: `[GLUTEN-<issue>][CH] description`
- Common code: `[GLUTEN-<issue>][CORE] description`
- Documentation: `[GLUTEN-<issue>][DOC] description`

### Java/Scala Style
- Import order: gluten → substrait.spark → spark → others → javax → java → scala
- Max line length: 100 characters
- Use ScalaTest for Spark-related tests, place in `org.apache.spark` package
- Use ScalaTest for Gluten tests, place in `org.apache.gluten` package

### C++ Style
- File extensions: `.h` for headers, `.cc` for sources
- Naming: PascalCase (types, files), camelCase (functions, variables), camelCase_ (private members)
- All code in `namespace gluten`
- Use `#pragma once` for include guards
- Prefer `unique_ptr` over `shared_ptr`
- Use clang-format-15 for formatting

### Test Placement
- Native code changes: Add gtest in `cpp/velox/tests/`
- Gluten code changes: Add ScalaTest in `org.apache.gluten` package
- Spark code changes: Add ScalaTest in `org.apache.spark` package
- CI runs tests from `org.apache.gluten` and `org.apache.spark` packages in parallel

## Maven Profiles

| Profile | Purpose |
|---------|---------|
| `backends-velox` | Build Velox backend |
| `backends-clickhouse` | Build ClickHouse backend |
| `spark-3.3`, `spark-3.4`, `spark-3.5`, `spark-4.0` | Target Spark version |
| `spark-ut` | Enable unit tests |
| `delta`, `iceberg`, `hudi`, `paimon` | Datalake support |
| `celeborn`, `uniffle` | Remote shuffle service |
| `java-11`, `java-17` | JDK version |

## Environment Requirements

- **JDK**: 8 (Spark 3.2-3.5), 17 (Spark 4.0+)
- **Maven**: 3.6.3+
- **GCC**: 11+
- **Memory**: 64GB+ recommended (Velox build is memory-intensive, adjust `NUM_THREADS` if OOM)
- **OS**: Ubuntu 20.04/22.04, CentOS 7/8

## Debugging Tips

### Identify Fallback Reasons
```scala
// Disable AQE and check plan
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.sql("your_query").explain()
```

### Debug C++ with GDB
```cpp
// Add to debug path:
pid_t pid = getpid();
printf("pid: %lu\n", pid);
sleep(10);
```
Then: `gdb attach <pid>`

### Arrow Memory Debug
Add JVM option: `-Darrow.memory.debug.allocator=true`
