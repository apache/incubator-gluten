# llvm-7.0: 
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

# cmake: 
Please make sure your cmake version is qualified based on the prerequisite.


# Arrow
``` shell
git clone https://github.com/oap-project/arrow.git
cd arrow && git checkout arrow-4.0.0-oap
mkdir -p arrow/cpp/release-build
cd arrow/cpp/release-build
cmake -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON -DARROW_PARQUET=ON -DARROW_CSV=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_DATASET=ON -DARROW_WITH_PROTOBUF=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_LZ4=ON -DARROW_FILESYSTEM=ON -DARROW_JSON=ON ..
make -j
make install

# build java
cd ../../java
# change property 'arrow.cpp.build.dir' to the relative path of cpp build dir in gandiva/pom.xml
mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=../cpp/release-build/release/ -DskipTests 
# if you are behine proxy, please also add proxy for socks
mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=../cpp/release-build/release/ -DskipTests -DsocksProxyHost=${proxyHost} -DsocksProxyPort=1080 
```

run test
``` shell
mvn test -pl adapter/parquet -P arrow-jni
mvn test -pl gandiva -P arrow-jni
```

After arrow installed in the specific directory, please make sure to set up -Dbuild_arrow=OFF -Darrow_root=/path/to/arrow when building Gazelle Plugin.
