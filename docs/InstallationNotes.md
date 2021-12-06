### Notes for Installation Issues
* Before the Installation, if you have installed other version of oap-native-sql, remove all installed lib and include from system path: libarrow* libgandiva* libspark-columnar-jni*

* libgandiva_jni.so was not found inside JAR

change property 'arrow.cpp.build.dir' to $ARROW_DIR/cpp/release-build/release/ in gandiva/pom.xml. If you do not want to change the contents of pom.xml, specify it like this:

```
mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=/root/git/t/arrow/cpp/release-build/release/ -DskipTests -Dcheckstyle.skip
```

* No rule to make target '../src/protobuf_ep', needed by `src/proto/Exprs.pb.cc'

remove the existing libprotobuf installation, then the script for find_package() will be able to download protobuf.

* can't find the libprotobuf.so.13 in the shared lib

copy the libprotobuf.so.13 from $OAP_DIR/oap-native-sql/cpp/src/resources to /usr/lib64/

* unable to load libhdfs: libgsasl.so.7: cannot open shared object file

libgsasl is missing, run `yum install libgsasl`

* CentOS 7.7 looks like didn't provide the glibc we required, so binaries packaged on F30 won't work.

```
20/04/21 17:46:17 WARN TaskSetManager: Lost task 0.1 in stage 1.0 (TID 2, 10.0.0.143, executor 6): java.lang.UnsatisfiedLinkError: /tmp/libgandiva_jni.sobe729912-3bbe-4bd0-bb96-4c7ce2e62336: /lib64/libm.so.6: version `GLIBC_2.29' not found (required by /tmp/libgandiva_jni.sobe729912-3bbe-4bd0-bb96-4c7ce2e62336)
```

* Missing symbols due to old GCC version.

```
[root@vsr243 release-build]# nm /usr/local/lib64/libparquet.so | grep ZN5boost16re_detail_10710012perl_matcherIN9__gnu_cxx17__normal_iteratorIPKcSsEESaINS_9sub_matchIS6_EEENS_12regex_traitsIcNS_16cpp_regex_traitsIcEEEEE14construct_initERKNS_11basic_regexIcSD_EENS_15regex_constants12_match_flagsE
_ZN5boost16re_detail_10710012perl_matcherIN9__gnu_cxx17__normal_iteratorIPKcSsEESaINS_9sub_matchIS6_EEENS_12regex_traitsIcNS_16cpp_regex_traitsIcEEEEE14construct_initERKNS_11basic_regexIcSD_EENS_15regex_constants12_match_flagsE
```

Need to compile all packags with newer GCC:

```
[root@vsr243 ~]# export CXX=/usr/local/bin/g++
[root@vsr243 ~]# export CC=/usr/local/bin/gcc
```

* Can not connect to hdfs @sr602

vsr606, vsr243 are both not able to connect to hdfs @sr602, need to skipTests to generate the jar

