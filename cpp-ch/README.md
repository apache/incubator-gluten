# How to build
项目依赖ClickHouse，有两种选择，一种是自动下载ClickHouse源码到当前目录，
另一种手动clone ClickHouse代码(https://github.com/Kyligence/ClickHouse.git)并通过cmake参数CH_SOURCE_DIR指定。

代码开发推荐使用外部ClickHouse源码，下面说明如果绑定外部ClickHouse项目进行编译
## 克隆ClickHouse
```shell
export CH_SOURCE_DIR=/tmp/ClickHouse #可以选择自己的目录
git clone https://github.com/Kyligence/ClickHouse.git ${CH_SOURCE_DIR}
cd ${CH_SOURCE_DIR}
# 更新submodule
git submodule update --init --recursive --depth 1
```

## 编译CH依赖
构建target build_ch，生成所有的静态链接库依赖。并刷新cmake build目录。
```shell
export GLUTEN_SOURCE=$(pwd)
cmake -G Ninja -S ${GLUTEN_SOURCE}/cpp-ch -B ${GLUTEN_SOURCE}/cpp-ch/build_ch -DCH_SOURCE_DIR=${CH_SOURCE_DIR} -DCMAKE_C_COMPILER=clang-15 -DCMAKE_CXX_COMPILER=clang++-15 -DCMAKE_BUILD_TYPE=Release
cmake --build ${GLUTEN_SOURCE}/cpp-ch/build_ch --target build_ch
```

动态链接库位于`cpp-ch/build/utils/extern-local-engine/libch.so`

# 模块拆分原理
local_engine目录与ClickHouse项目通过软链接的方式关联,cpp-ch下的cmake会在ClickHouse创建一个utils/extern-local-engine的软链接。
整体开发方式可以与之前保持一致，只是git提交需要通过gluten项目完成。
新增cmake option：
* ENABLE_EXTERN_LOCAL_ENGINE=ON   在导入ClickHouse时指定启用extern-local-engine

# 一些问题
启动时需要指定LD_PRELOAD={path of libch.so}，目的是让libch.so内的jemalloc最先被加载。

spark-submit --conf spark.executorEnv.LD_PRELOAD=/path/to/your/library

# 新的Jenkins CI
https://cicd-aws.kyligence.com/job/Gluten/job/gluten-ci/
公共只读账号：gluten/hN2xX3uQ4m
