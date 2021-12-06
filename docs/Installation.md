# Gazelle Plugin Installation

For detailed testing scripts, please refer to [solution guide](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql)

## Install Googletest and Googlemock

``` shell
yum install gtest-devel
yum install gmock
```

## Build Gazelle Plugin

``` shell
git clone -b ${version} https://github.com/oap-project/native-sql-engine.git
cd oap-native-sql
mvn clean package -DskipTests -Dcpp_tests=OFF -Dbuild_arrow=ON -Dcheckstyle.skip
```

Based on the different environment, there are some parameters can be set via -D with mvn.

| Parameters | Description | Default Value |
| ---------- | ----------- | ------------- |
| cpp_tests  | Enable or Disable CPP Tests | False |
| build_arrow | Build Arrow from Source | True |
| arrow_root | When build_arrow set to False, arrow_root will be enabled to find the location of your existing arrow library. | /usr/local |
| build_protobuf | Build Protobuf from Source. If set to False, default library path will be used to find protobuf library. | True |

When build_arrow set to True, the build_arrow.sh will be launched and compile a custom arrow library from [OAP Arrow](https://github.com/oap-project/arrow/tree/arrow-4.0.0-oap)
If you wish to change any parameters from Arrow, you can change it from the `build_arrow.sh` script under `native-sql-engine/arrow-data-source/script/`.

### Additional Notes
[Notes for Installation Issues](./InstallationNotes.md)
