# How to build
项目依赖ClickHouse，有两种选择，一种是自动下载ClickHouse源码到当前目录，
另一种手动clone ClickHouse代码(https://github.com/Kyligence/ClickHouse.git)并通过cmake参数CH_SOURCE_DIR指定。

代码开发推荐使用外部ClickHouse源码，下面说明如果绑定外部ClickHouse项目进行编译
## 克隆ClickHouse
```shell
export CH_SOURCE_DIR=/tmp/ClickHouse
git clone https://github.com/Kyligence/ClickHouse.git ${CH_SOURCE_DIR}
cd ${CH_SOURCE_DIR}
# 更新submodule
git submodule update --init --recursive --depth 1
```

## 编译CH依赖
构建target build_ch，生成所有的静态链接库依赖。并刷新cmake build目录。
```shell
export GLUTEN_SOURCE=$(pwd)
cmake -G Ninja -S ${GLUTEN_SOURCE}/cpp-ch -B ${GLUTEN_SOURCE}/cpp-ch/build -DCH_SOURCE_DIR=/tmp/ClickHouse -DCMAKE_C_COMPILER=clang-15 -DCMAKE_CXX_COMPILER=clang++-15 -DCMAKE_BUILD_TYPE=Release
cmake --build ${GLUTEN_SOURCE}/cpp-ch/build --target build_ch
```
## 构建libch
构建target ch,这里需要在build ch之后刷新一下cmake的build目录
```shell
cmake -G Ninja -S ${GLUTEN_SOURCE}/cpp-ch -B ${GLUTEN_SOURCE}/cpp-ch/build -DCH_SOURCE_DIR=/tmp/ClickHouse -DCMAKE_C_COMPILER=clang-15 -DCMAKE_CXX_COMPILER=clang++-15 -DCMAKE_BUILD_TYPE=Release
cmake --build ${GLUTEN_SOURCE}/cpp-ch/build --target ch
```