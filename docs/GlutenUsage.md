### Build parameters

Based on the different environment, there are some parameters can be set via -D with mvn.

| Parameters                                            | Description                                                                                                                                                                        | Default Value |
|-------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ------------- |
| build_cpp                                             | Enable or Disable building CPP library                                                                                                                                             | OFF |
| cpp_tests                                             | Enable or Disable CPP Tests                                                                                                                                                        | OFF |
| build_arrow                                           | Build Arrow from Source                                                                                                                                                            | OFF |
| arrow_root                                            | When build_arrow set to False, arrow_root will be enabled to find the location of your existing arrow library.                                                                     | /PATH_TO_GLUTEN/tools/build/arrow_install |
| build_protobuf                                        | Build Protobuf from Source. If set to False, default library path will be used to find protobuf library.                                                                           |ON |
| build_velox_from_source                               | Enable or Disable building Velox from a specific velox github repository. A default installed path will be in velox_home                                                           | OFF |
| backends-velox                                        | Add -Pbackends-velox in maven command to compile the JVM part of Velox backend                                                                                                     | false |
| backends-clickhouse                                   | Add -Pbackends-clickhouse in maven command to compile the JVM part of ClickHouse backend                                                                                           | false |
| build_velox                                           | Enable or Disable building the CPP part of Velox backend                                                                                                                           | OFF |
| velox_home (only valid when build_velox is ON)        | The path to the compiled Velox project. When building Gluten with Velox, if you have an existing Velox, please set it.                                                             | /PATH_TO_GLUTEN/tools/build/velox_ep |
| compile_velox(only valid when velox_home is assigned) | recompile exising Velox use custom compile parameters                                                                                                                              | OFF |
| velox_build_type                                      | The build type Velox was built with from source code. Gluten uses this value to locate the binary path of Velox's binary libraries.                                                | release |
| debug_build                                           | Whether to generate debug binary library from Gluten's C++ codes.                                                                                                                  | OFF |
| enable_ep_cache                                       | Whether to cache the source folder for some critical external projects (e.g. Arrow, Velox) to spped-up build process. Note, this will not be invalidated even you use 'mvn clean'. | OFF |                                                                                                                                |

When build_arrow set to True, the build_arrow.sh will be launched and compile a custom arrow library from [OAP Arrow](https://github.com/oap-project/arrow/tree/arrow-8.0.0-gluten)
If you wish to change any parameters from Arrow, you can change it from the [build_arrow.sh](../tools/build_arrow.sh) script.

### Build jar

The gluten jar name pattern is gluten-spark<sparkbundle.version>_<scala.binary.version>-<version>-SNAPSHOT-jar-with-dependencies.jar

| Spark Version | sparkbundle.version | scala.binary.version |
| ---------- | ----------- | ------------- |
| 3.2.x | 3.2 | 2.12 |
| 3.3.x | 3.3 | 2.12 |

Backend velox support both spar3.2 and spark3.3 while backend clickhouse support spark3.2
